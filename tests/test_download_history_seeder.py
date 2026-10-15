"""Pruebas de integración para sembrado de Historial de Descargas (HUI-03).

Valida la ingesta, normalización criptográfica y consulta de registros
históricos desde la base de datos de producción replicada hacia
DownloadTrackingPort (SqliteDownloadTrackingAdapter).
"""

from pathlib import Path
import pytest

from prospector_interno.adapters.legacy.backup_seeder import PostgresLegacySeeder
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
)


SNAPSHOT_PATH = (
    Path(__file__).parent.parent / "data" / "snapshot" / "platform_prod_snapshot.db"
)
TOP_5_SOURCES = ["BCB", "ASFI", "INE", "BBV", "APS"]
TOP_5_FILES = [
    "D_BO_000000484",  # BCB
    "D_BO_000000058",  # ASFI
    "D_BO_000000016",  # INE
    "D_BO_000000481",  # BBV
    "D_BO_000000447",  # APS (0 descargas en prod)
]


@pytest.fixture
def snapshot_seeder() -> PostgresLegacySeeder:
    """Instancia del seeder conectada al snapshot SQLite de producción."""
    assert SNAPSHOT_PATH.is_file(), (
        f"El archivo snapshot {SNAPSHOT_PATH} debe existir para ejecutar las pruebas"
    )
    return PostgresLegacySeeder(sqlite_snapshot_path=SNAPSHOT_PATH)


def test_extract_downloads_top5_normalized(
    snapshot_seeder: PostgresLegacySeeder,
) -> None:
    """Verifica la extracción y saneamiento del historial para los Top 5."""
    records = snapshot_seeder.extract_downloads(
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    # 105 registros de descarga legacy; BCB incluye sub-archivos PDF y XLSX (157 ítems)
    assert len(records) == 157

    # Verificar que el 100% de los registros tienen hash SHA-256 de 64 caracteres
    for rec in records:
        assert rec.sha256_checksum is not None
        assert len(rec.sha256_checksum) == 64
        assert rec.status == DownloadLifecycleStatus.COMPLETED
        assert rec.canonical_url is not None
        assert len(rec.download_id) == 64

    # Verificar separación de fuentes
    sources_in_records = {r.source_id for r in records}
    assert "BO_BCB" in sources_in_records
    assert "BO_ASFI" in sources_in_records
    assert "BO_INE" in sources_in_records
    assert "BO_BBV" in sources_in_records
    # APS debe estar ausente (0 descargas en prod)
    assert "BO_APS" not in sources_in_records


def test_extract_downloads_bcb_subitems(
    snapshot_seeder: PostgresLegacySeeder,
) -> None:
    """Verifica que descargas duales de BCB se extraen como ítems independientes."""
    bcb_records = snapshot_seeder.extract_downloads(
        source_codes=["BCB"],
        file_codes=["D_BO_000000484"],
    )

    # 52 descargas en legacy * 2 archivos (pdf y xlsx) = 104 ítems
    assert len(bcb_records) == 104

    pdf_records = [
        r for r in bcb_records
        if r.canonical_url.endswith(".pdf#subitem=0") or ".pdf" in r.canonical_url
    ]
    xlsx_records = [
        r for r in bcb_records
        if r.canonical_url.endswith(".xlsx#subitem=1") or ".xlsx" in r.canonical_url
    ]

    assert len(pdf_records) == 52
    assert len(xlsx_records) == 52


def test_seed_download_history_sqlite_persistence(
    snapshot_seeder: PostgresLegacySeeder,
    tmp_path: Path,
) -> None:
    """Verifica el sembrado en SqliteDownloadTrackingAdapter y persistencia en disco."""
    tracking_db = tmp_path / "download_tracking.db"
    tracker = SqliteDownloadTrackingAdapter(db_path=tracking_db)

    count = snapshot_seeder.seed_download_history(
        tracker=tracker,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )
    assert count == 157

    # Consultar por ID de descarga
    records = snapshot_seeder.extract_downloads(
        source_codes=["BBV"],
        file_codes=["D_BO_000000481"],
    )
    assert len(records) > 0
    sample = records[0]

    retrieved = tracker.get_download(sample.download_id)
    assert retrieved is not None
    assert retrieved.sha256_checksum == sample.sha256_checksum
    assert retrieved.source_id == "BO_BBV"
    assert retrieved.status == DownloadLifecycleStatus.COMPLETED

    # Consultar por URL
    by_url = tracker.find_by_url(sample.canonical_url)
    assert by_url is not None
    assert by_url.download_id == sample.download_id

    # Consultar por Checksum SHA-256
    by_hash = tracker.find_by_checksum(sample.sha256_checksum)
    assert by_hash is not None
    assert by_hash.download_id == sample.download_id

    # Consultar descargas por fuente
    asfi_downloads = tracker.list_downloads_by_source("BO_ASFI")
    assert len(asfi_downloads) == 13

    # Validar persistencia reabriendo el archivo SQLite
    tracker.close()
    tracker_reopened = SqliteDownloadTrackingAdapter(db_path=tracking_db)
    reopened_record = tracker_reopened.get_download(sample.download_id)
    assert reopened_record is not None
    assert reopened_record.sha256_checksum == sample.sha256_checksum
    tracker_reopened.close()
