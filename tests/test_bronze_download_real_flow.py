"""Pruebas de integración para Descarga Cortés y Manifiesto Bronze.

Valida el flujo de adquisición hacia Capa Bronze para datasets de la
base de datos real (ej. brecha de control APS D_BO_000000447), asegurando:
1. Escritura streaming a archivo temporal (.tmp).
2. Cálculo criptográfico SHA-256 en vuelo sin desbordar memoria.
3. Renombrado atómico a ruta particionada final.
4. Registro inmutable en bronze_manifest.json e internal_assets (linaje).
5. Protección ante discordancia de checksum (ChecksumMismatchError).
6. Detección inmediata de Cache Hit en reintentos.
"""

import hashlib
import io
import json
from pathlib import Path
from typing import BinaryIO
import pytest

from prospector_interno.adapters.legacy.backup_seeder import PostgresLegacySeeder
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.bridge.sync_orchestrator import (
    SyncOrchestrator,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.exceptions import ChecksumMismatchError
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.models import (
    IngestionRequest,
    PreFlightVerdict,
    ReconciliationDiffReport,
    ReconciliationDiffReportItem,
    ResourceChangeCategory,
)
from prospector_interno.domain.ports import HttpDownloaderPort


SNAPSHOT_PATH = (
    Path(__file__).parent.parent / "data" / "snapshot" / "platform_prod_snapshot.db"
)
TOP_5_SOURCES = ["BCB", "ASFI", "INE", "BBV", "APS"]
TOP_5_FILES = [
    "D_BO_000000484",
    "D_BO_000000058",
    "D_BO_000000016",
    "D_BO_000000481",
    "D_BO_000000447",
]


class MockPoliteStreamDownloader(HttpDownloaderPort):
    """Downloader simulado que emite contenido binario controlado."""

    def __init__(self, content: bytes) -> None:
        self._content = content

    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        return io.BytesIO(self._content), 200, "application/pdf"


def test_bronze_atomic_download_and_manifest_registration(tmp_path: Path) -> None:
    """Verifica el flujo completo de almacenamiento en Bronze para el dataset de APS."""
    # 1. Sembrado de catálogo
    seeder = PostgresLegacySeeder(sqlite_snapshot_path=SNAPSHOT_PATH)
    catalog = SqliteLineageAdapter(db_path=tmp_path / "metadata.db")
    seeder.seed_catalog(
        catalog=catalog,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    # 2. Configurar almacenamiento Bronze
    bronze_dir = tmp_path / "bronze"
    bronze_storage = LocalBronzeStorage(base_path=bronze_dir)

    # 3. Datos simulados del boletín APS (brecha de control: 0 descargas previas)
    pdf_payload = (
        b"%PDF-1.4\n1 0 obj\n"
        b"<< /Title (Boletin Mensual Seguros APS) >>\nendobj\n"
    )
    expected_sha256 = hashlib.sha256(pdf_payload).hexdigest()

    aps_ds = catalog.get_dataset_by_legacy_code("D_BO_000000447")
    assert aps_ds is not None

    # 4. Almacenar stream con verificación SHA-256
    stream = io.BytesIO(pdf_payload)
    artifact = bronze_storage.store_stream(
        domain="BO_APS",
        dataset_code=aps_ds.dataset_code,
        period="2026-09",
        stream=stream,
        expected_checksum=expected_sha256,
        content_type="application/pdf",
        url="https://www.aps.gob.bo/images/webdocs/UNE/DS/2026/Septiembre.pdf",
    )

    # 5. Validaciones de artefacto
    assert artifact.sha256_hash == expected_sha256
    assert artifact.size_bytes == len(pdf_payload)
    assert Path(artifact.file_path).is_file()
    assert not str(artifact.file_path).endswith(".tmp")

    # 6. Validar bronze_manifest.json en disco
    manifest_file = bronze_dir / "bronze_manifest.json"
    assert manifest_file.is_file()
    manifest_data = json.loads(manifest_file.read_text(encoding="utf-8"))
    persisted_hashes = [a["sha256_hash"] for a in manifest_data["artifacts"]]
    assert expected_sha256 in persisted_hashes
    assert manifest_data["artifacts"][0]["domain"] == "BO_APS"

    # 7. Registrar linaje en SqliteLineageAdapter
    asset_record = InternalAssetRecord(
        asset_id=f"ast_bronze_aps_{expected_sha256[:12]}",
        source_id="BO_APS",
        dataset_id=aps_ds.dataset_code,
        period_start="2026-09-01",
        period_end="2026-09-30",
        storage_layer=StorageLayer.BRONZE,
        file_path=str(artifact.file_path),
        content_hash_sha256=expected_sha256,
        size_bytes=artifact.size_bytes,
    )
    catalog.record_asset(asset_record)

    persisted_asset = catalog.get_asset(asset_record.asset_id)
    assert persisted_asset is not None
    assert persisted_asset.content_hash_sha256 == expected_sha256

    # 8. Comprobar que Gatekeeper ahora reporta CACHE_HIT para este activo
    gatekeeper = PreFlightGatekeeper(
        storage=bronze_storage,
        state_registry=InMemoryGatekeeperState(),
    )
    req = IngestionRequest(
        request_id="req-aps-verify-001",
        domain="BO_APS",
        dataset_code=aps_ds.dataset_code,
        period="2026-09",
        target_url="https://www.aps.gob.bo/images/webdocs/UNE/DS/2026/Septiembre.pdf",
    )
    eval_res = gatekeeper.evaluate(req)
    assert eval_res.verdict == PreFlightVerdict.CACHE_HIT_BRONZE
    assert eval_res.lock_acquired is False


def test_bronze_download_checksum_mismatch_cleans_temporary_file(
    tmp_path: Path,
) -> None:
    """Verifica que discordancia de hash elimina el .tmp y levanta error."""
    bronze_storage = LocalBronzeStorage(base_path=tmp_path / "bronze")
    bad_payload = b"CONTENIDO_CORRUPTO_O_TRUNCADO"
    wrong_checksum = "a" * 64

    stream = io.BytesIO(bad_payload)
    with pytest.raises(ChecksumMismatchError):
        bronze_storage.store_stream(
            domain="BO_APS",
            dataset_code="DS_BO_APS_D_BO_000000447",
            period="2026-09",
            stream=stream,
            expected_checksum=wrong_checksum,
        )

    # Verificar que no quedaron archivos temporales en el directorio
    tmp_files = list((tmp_path / "bronze").glob("**/*.tmp"))
    assert len(tmp_files) == 0


def test_sync_orchestrator_executes_plan_with_bronze_storage(
    tmp_path: Path,
) -> None:
    """Verifica la ejecución coordinada de SyncPlan depositando en Bronze."""
    bronze_dir = tmp_path / "bronze"
    storage = LocalBronzeStorage(base_path=bronze_dir)
    state = InMemoryGatekeeperState()
    tracking = SqliteDownloadTrackingAdapter(db_path=tmp_path / "tracking.db")
    dl_filter = DownloadStateFilter(tracking_port=tracking)

    gatekeeper = PreFlightGatekeeper(
        storage=storage,
        state_registry=state,
        download_state_filter=dl_filter,
    )

    pdf_data = b"BOLETIN_APS_DATA_OCTUBRE_2026"
    downloader = MockPoliteStreamDownloader(content=pdf_data)

    orchestrator = SyncOrchestrator(
        gatekeeper=gatekeeper,
        storage=storage,
        downloader=downloader,
    )

    diff_report = ReconciliationDiffReport(
        source_id="aps",
        run_id="run_top5_001",
        total_external_resources=1,
        total_confirmed=0,
        total_new=1,
        total_modified=0,
        total_missing=0,
        items=[
            ReconciliationDiffReportItem(
                resource_key="APS_2026_09_PDF",
                source_id="aps",
                url="https://www.aps.gob.bo/images/webdocs/UNE/DS/2026/Septiembre.pdf",
                category=ResourceChangeCategory.NEW,
                file_extension=".pdf",
                period_label="2026-09",
                detail="Nuevo boletin mensual descubierto",
            )
        ],
    )

    plan = orchestrator.create_plan(diff_report)
    assert plan.total_tasks == 1

    executed_plan = orchestrator.execute_plan(plan, force_now=True)
    assert executed_plan.is_executed is True
    assert executed_plan.completed_tasks == 1
    assert executed_plan.failed_tasks == 0

    item = executed_plan.items[0]
    assert item.status == "COMPLETED"
    assert item.downloaded_artifact is not None
    assert Path(item.downloaded_artifact.file_path).is_file()
