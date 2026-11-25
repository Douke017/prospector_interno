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
from prospector_interno.adapters.analytics.readonly_sql_engine import (
    ReadOnlyDuckDbSqlEngine,
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
from prospector_interno.adapters.storage.postgres_v2_repositories import (
    InMemoryAssetProfileRepository,
    InMemoryDeduplicationRepository,
    PostgresAssetProfileRepository,
    PostgresDeduplicationRepository,
    PostgresGatekeeperState,
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
from prospector_interno.application.catalog.raw_file_profiler import (
    RawFileProfilerService,
)
from prospector_interno.application.download_worker_pool import (
    AsyncDownloadWorkerPool,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.application.lineage.lineage_tracker import LineageTracker
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.application.pipelines.lossless_parquet_packager import (
    LosslessParquetPackager,
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
BRONZE_DIR: Path = Path(
    os.getenv("PROSPECTOR_BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
)
_IS_ISOLATED_TEST_BOOT: bool = (
    os.getenv("PROSPECTOR_TEST_USE_PG", "0") != "1"
    and bool(os.getenv("PYTEST_CURRENT_TEST") or "pytest" in sys.modules)
)
GOLD_DIR: Path = (
    Path(tempfile.mkdtemp(prefix="prospector_test_gold_"))
    if _IS_ISOLATED_TEST_BOOT
    else Path(os.getenv("PROSPECTOR_GOLD_DIR", str(PROJECT_ROOT / "data" / "gold")))
)


class InMemoryApiKeyRepository:
    """Repositorio efímero de llaves API para aislamiento estricto en pytest."""

    def __init__(self) -> None:
        self._keys: dict[str, dict[str, Any]] = {}
        self._by_hash: dict[str, str] = {}
        self._logs: list[dict[str, Any]] = []
        self._seq = 0

    def create_key(
        self,
        label: str,
        allowed_datamarts: str = "*",
        allowed_layers: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> dict[str, Any]:
        clean_label = label.strip() or "Cliente Programático DATAX"
        raw_token = f"dtx_live_{secrets.token_hex(16)}"
        key_id = f"key_{secrets.token_hex(6)}"
        key_prefix = f"{raw_token[:13]}..."
        key_sha256 = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
        now_iso = datetime.now(timezone.utc).isoformat()
        layers = [
            lyr.strip().upper()
            for lyr in (allowed_layers or ["RAW", "GOLD", "SQL"])
            if lyr.strip()
        ] or ["RAW", "GOLD", "SQL"]
        entry = {
            "key_id": key_id,
            "label": clean_label,
            "key_prefix": key_prefix,
            "allowed_datamarts": allowed_datamarts,
            "allowed_layers": layers,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "revoked_at": None,
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
        self._keys[key_id]["revoked_at"] = datetime.now(timezone.utc).isoformat()
        return True

    def authorize_and_log(
        self,
        *,
        raw_key: str,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
        required_layer: str = "GOLD",
    ) -> tuple[dict[str, Any] | None, int, str | None]:
        layer_u = required_layer.strip().upper()

        def _log_denied(key_id: str | None, key_label: str | None, reason: str) -> None:
            self._seq += 1
            self._logs.append(
                {
                    "log_id": self._seq,
                    "key_id": key_id,
                    "key_label": key_label or "Llave no autorizada",
                    "datamart_name": datamart_name,
                    "export_format": export_format,
                    "rows_returned": 0,
                    "client_agent": client_agent or "Cliente Externo",
                    "status": "DENIED",
                    "denial_reason": reason,
                    "layer": layer_u,
                    "accessed_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        if not raw_key or not raw_key.strip():
            _log_denied(None, None, "MISSING_API_KEY")
            return None, 401, "Llave API ausente."
        key_sha256 = hashlib.sha256(raw_key.strip().encode("utf-8")).hexdigest()
        key_id = self._by_hash.get(key_sha256)
        if not key_id:
            _log_denied(None, None, "INVALID_API_KEY")
            return None, 401, "Llave API inválida."
        entry = self._keys.get(key_id)
        if not entry or not entry["is_active"]:
            _log_denied(key_id, entry["label"] if entry else None, "REVOKED_API_KEY")
            return None, 401, "Llave API revocada."
        if entry.get("expires_at"):
            exp_dt = datetime.fromisoformat(str(entry["expires_at"]))
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            if exp_dt <= datetime.now(timezone.utc):
                _log_denied(key_id, entry["label"], "EXPIRED_API_KEY")
                return None, 401, "Llave API expirada."
        allowed_layers = [str(x).upper() for x in (entry.get("allowed_layers") or ["RAW", "GOLD", "SQL"])]
        if "*" not in allowed_layers and layer_u not in allowed_layers:
            _log_denied(key_id, entry["label"], f"LAYER_FORBIDDEN:{layer_u}")
            return None, 403, f"Llave API sin permiso sobre la capa '{layer_u}'."
        allowed = str(entry["allowed_datamarts"] or "*").strip()
        if allowed != "*" and datamart_name != "*":
            allowed_set = {x.strip().lower() for x in allowed.split(",") if x.strip()}
            if datamart_name.lower() not in allowed_set:
                _log_denied(key_id, entry["label"], f"DATAMART_FORBIDDEN:{datamart_name}")
                return None, 403, f"Llave API sin autorización para '{datamart_name}'."

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
                "client_agent": client_agent or "Cliente Programático DATAX",
                "status": "ALLOWED",
                "denial_reason": None,
                "layer": layer_u,
                "accessed_at": now_iso,
            }
        )
        return dict(entry), 200, None

    def validate_and_log(
        self,
        raw_key: str,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
    ) -> dict[str, Any] | None:
        entry, code, _ = self.authorize_and_log(
            raw_key=raw_key,
            datamart_name=datamart_name,
            export_format=export_format,
            rows_returned=rows_returned,
            client_agent=client_agent,
        )
        return entry if code == 200 else None

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
gatekeeper_state = (
    PostgresGatekeeperState() if USE_POSTGRES_MUTATIONS else InMemoryGatekeeperState()
)
dedup_repo: PostgresDeduplicationRepository | InMemoryDeduplicationRepository = (
    PostgresDeduplicationRepository()
    if USE_POSTGRES_MUTATIONS
    else InMemoryDeduplicationRepository()
)
asset_profile_repo: PostgresAssetProfileRepository | InMemoryAssetProfileRepository = (
    PostgresAssetProfileRepository()
    if USE_POSTGRES_MUTATIONS
    else InMemoryAssetProfileRepository()
)
gold_partition_checker = DuckDBGoldPartitionAdapter(base_gold_path=GOLD_DIR)
derivation_filter = AssetDerivationFilter(
    partition_checker=gold_partition_checker,
    base_gold_path=GOLD_DIR,
    bronze_storage=bronze_storage,
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
    dedup_repo=dedup_repo,
)

lineage_adapter: LineageRepositoryPort = (
    PostgresLineageAndCatalogAdapter()
    if USE_POSTGRES_MUTATIONS
    else SqliteLineageAdapter(":memory:")
)
lineage_tracker = LineageTracker(lineage_adapter)
asset_catalog_service = AssetCatalogService(lineage_adapter)
raw_file_profiler = RawFileProfilerService(profile_repo=asset_profile_repo)

url_inventory_repo = PostgresUrlInventoryRepository() if USE_POSTGRES_MUTATIONS else None
download_worker_pool = AsyncDownloadWorkerPool(
    tracking_port=download_tracking,
    bronze_storage=bronze_storage,
    lineage_adapter=lineage_adapter,
    dedup_repo=dedup_repo,
    url_inventory_repo=url_inventory_repo,
    on_asset_custodied=lambda aid, fpath, src, sha: raw_file_profiler.profile_file(
        asset_id=aid,
        file_path=fpath,
        source_id=src,
        sha256_hash=sha,
        persist=True,
    ),
)
semantic_dict_repo = PostgresSemanticDictionaryRepository()
api_key_repo: PostgresApiKeyRepository | InMemoryApiKeyRepository = (
    PostgresApiKeyRepository() if USE_POSTGRES_MUTATIONS else InMemoryApiKeyRepository()
)

analytics_adapter = DuckDBAnalyticsAdapter(base_gold_path=GOLD_DIR)
readonly_sql_engine = ReadOnlyDuckDbSqlEngine(base_gold_path=GOLD_DIR)
views_service = PredefinedViewsService(base_gold_path=GOLD_DIR)
gold_materializer = GoldMaterializer(
    base_gold_path=GOLD_DIR,
    lineage_tracker=lineage_tracker,
)
lossless_packager = LosslessParquetPackager(
    base_gold_path=GOLD_DIR,
    lineage_adapter=lineage_adapter,
)
