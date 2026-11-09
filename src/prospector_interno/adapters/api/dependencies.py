"""Contenedor Singleton de Dependencias para el Prospector Interno.

Elimina el problema de instancias fragmentadas (*split-brain*) entre routers:
todos los controladores comparten el mismo PreFlightGatekeeper, almacenamiento
Bronze, rastreador de cola de descargas, linaje criptográfico y motor analítico.
En pruebas unitarias (`pytest`) aísla automáticamente las escrituras en memoria
para cumplir estrictamente la gobernanza de inmutabilidad de entorno.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import secrets
import sys
import tempfile
from typing import Any

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
    DuckDBGoldPartitionAdapter,
)
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.db_connection import (
    get_readwrite_connection,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.postgres_repositories import (
    PostgresApiKeyRepository,
    PostgresDownloadTrackingAdapter,
    PostgresLineageAndCatalogAdapter,
    PostgresSemanticDictionaryRepository,
    PostgresUrlInventoryRepository,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.analytics.views_service import (
    PredefinedViewsService,
)
from prospector_interno.application.catalog.asset_catalog_service import (
    AssetCatalogService,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.application.lineage.lineage_tracker import LineageTracker
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.domain.gatekeeper.derivation_filter import (
    AssetDerivationFilter,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.ports import (
    DownloadTrackingPort,
    LineageRepositoryPort,
)

PROJECT_ROOT: Path = Path(__file__).resolve().parents[4]
BRONZE_DIR: Path = PROJECT_ROOT / "data" / "bronze"
_IS_ISOLATED_TEST_BOOT: bool = (
    os.getenv("PROSPECTOR_TEST_USE_PG", "0") != "1"
    and bool(os.getenv("PYTEST_CURRENT_TEST") or "pytest" in sys.modules)
)
GOLD_DIR: Path = (
    Path(tempfile.mkdtemp(prefix="prospector_test_gold_"))
    if _IS_ISOLATED_TEST_BOOT
    else (PROJECT_ROOT / "data" / "gold")
)


class InMemoryApiKeyRepository:
    """Repositorio efímero de llaves API para aislamiento estricto en pytest."""

    def __init__(self) -> None:
        self._keys: dict[str, dict[str, Any]] = {}
        self._by_hash: dict[str, str] = {}
        self._logs: list[dict[str, Any]] = []
        self._seq = 0

    def create_key(self, label: str, allowed_datamarts: str = "*") -> dict[str, Any]:
        clean_label = label.strip() or "Conector Analítico Excel"
        raw_token = f"dtx_live_{secrets.token_hex(16)}"
        key_id = f"key_{secrets.token_hex(6)}"
        key_prefix = f"{raw_token[:13]}..."
        key_sha256 = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
        now_iso = datetime.now(timezone.utc).isoformat()
        entry = {
            "key_id": key_id,
            "label": clean_label,
            "key_prefix": key_prefix,
            "allowed_datamarts": allowed_datamarts,
            "is_active": True,
            "created_at": now_iso,
            "last_used_at": None,
            "usage_count": 0,
        }
        self._keys[key_id] = entry
        self._by_hash[key_sha256] = key_id
        out = dict(entry)
        out["api_key"] = raw_token
        out["raw_api_key"] = raw_token
        return out

    def list_keys(self) -> list[dict[str, Any]]:
        return [dict(v) for v in reversed(list(self._keys.values()))]

    def revoke_key(self, key_id: str) -> bool:
        if key_id not in self._keys:
            return False
        self._keys[key_id]["is_active"] = False
        return True

    def validate_and_log(
        self,
        raw_key: str,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
    ) -> dict[str, Any] | None:
        if not raw_key or not raw_key.strip():
            return None
        key_sha256 = hashlib.sha256(raw_key.strip().encode("utf-8")).hexdigest()
        key_id = self._by_hash.get(key_sha256)
        if not key_id:
            return None
        entry = self._keys.get(key_id)
        if not entry or not entry["is_active"]:
            return None
        allowed = str(entry["allowed_datamarts"] or "*").strip()
        if allowed != "*":
            allowed_set = {x.strip().lower() for x in allowed.split(",") if x.strip()}
            if datamart_name.lower() not in allowed_set:
                return None

        now_iso = datetime.now(timezone.utc).isoformat()
        entry["last_used_at"] = now_iso
        entry["usage_count"] = int(entry["usage_count"]) + 1
        self._seq += 1
        self._logs.append(
            {
                "log_id": self._seq,
                "key_id": key_id,
                "key_label": entry["label"],
                "datamart_name": datamart_name,
                "export_format": export_format,
                "rows_returned": rows_returned,
                "client_agent": client_agent or "Excel Power Query / Power BI",
                "accessed_at": now_iso,
            }
        )
        return dict(entry)

    def record_direct_export(
        self,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
    ) -> None:
        self._seq += 1
        self._logs.append(
            {
                "log_id": self._seq,
                "key_id": None,
                "key_label": "Consola Web de Autoservicio",
                "datamart_name": datamart_name,
                "export_format": export_format,
                "rows_returned": rows_returned,
                "client_agent": client_agent or "Navegador Analista",
                "accessed_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def list_access_logs(self, limit: int = 30) -> list[dict[str, Any]]:
        return list(reversed(self._logs))[:limit]


def is_isolated_test_mode() -> bool:
    """Detecta si estamos ejecutando pruebas automatizadas sin bandera explícita PG."""
    if os.getenv("PROSPECTOR_TEST_USE_PG", "0") == "1":
        return False
    return bool(os.getenv("PYTEST_CURRENT_TEST") or "pytest" in sys.modules)


def is_postgres_available() -> bool:
    """Comprueba conectividad real con la réplica PostgreSQL."""
    try:
        with get_readwrite_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone() is not None
    except Exception:
        return False


USE_POSTGRES_MUTATIONS: bool = (not is_isolated_test_mode()) and is_postgres_available()

bronze_storage = LocalBronzeStorage(base_path=BRONZE_DIR)
gatekeeper_state = InMemoryGatekeeperState()
gold_partition_checker = DuckDBGoldPartitionAdapter(base_gold_path=GOLD_DIR)
derivation_filter = AssetDerivationFilter(
    partition_checker=gold_partition_checker,
    base_gold_path=GOLD_DIR,
)

download_tracking: DownloadTrackingPort = (
    PostgresDownloadTrackingAdapter()
    if USE_POSTGRES_MUTATIONS
    else SqliteDownloadTrackingAdapter(":memory:")
)
download_state_filter = DownloadStateFilter(tracking_port=download_tracking)

gatekeeper = PreFlightGatekeeper(
    storage=bronze_storage,
    state_registry=gatekeeper_state,
    derivation_filter=derivation_filter,
    download_state_filter=download_state_filter,
    gold_partition_checker=gold_partition_checker,
)

lineage_adapter: LineageRepositoryPort = (
    PostgresLineageAndCatalogAdapter()
    if USE_POSTGRES_MUTATIONS
    else SqliteLineageAdapter(":memory:")
)
lineage_tracker = LineageTracker(lineage_adapter)
asset_catalog_service = AssetCatalogService(lineage_adapter)

url_inventory_repo = PostgresUrlInventoryRepository() if USE_POSTGRES_MUTATIONS else None
semantic_dict_repo = PostgresSemanticDictionaryRepository()
api_key_repo: PostgresApiKeyRepository | InMemoryApiKeyRepository = (
    PostgresApiKeyRepository() if USE_POSTGRES_MUTATIONS else InMemoryApiKeyRepository()
)

analytics_adapter = DuckDBAnalyticsAdapter(base_gold_path=GOLD_DIR)
views_service = PredefinedViewsService(base_gold_path=GOLD_DIR)
gold_materializer = GoldMaterializer(
    base_gold_path=GOLD_DIR,
    lineage_tracker=lineage_tracker,
)
