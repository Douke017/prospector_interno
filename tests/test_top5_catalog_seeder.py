"""Pruebas de integración para sembrado de Catálogo Top 5 desde Snapshot real.

Valida la extracción, normalización y persistencia de las 5 fuentes y datasets
estratégicos seleccionados de la base de datos de producción replicada.
"""

from pathlib import Path
import pytest

from prospector_interno.adapters.legacy.backup_seeder import PostgresLegacySeeder
from prospector_interno.adapters.memory_state import InMemoryCatalogRepository
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.domain.exceptions import LegacySeederError
from prospector_interno.domain.models import Periodicity


SNAPSHOT_PATH = (
    Path(__file__).parent.parent / "data" / "snapshot" / "platform_prod_snapshot.db"
)
TOP_5_SOURCES = ["BCB", "ASFI", "INE", "BBV", "APS"]
TOP_5_FILES = [
    "D_BO_000000484",  # BCB - Tasas Interés Diarias
    "D_BO_000000058",  # ASFI - Evolución Fondos Inversión
    "D_BO_000000016",  # INE - Índice General IPC
    "D_BO_000000481",  # BBV - Boletín Diario Bursátil
    "D_BO_000000447",  # APS - Boletín Seguros Mensual
]


@pytest.fixture
def snapshot_seeder() -> PostgresLegacySeeder:
    """Instancia del seeder conectada al snapshot SQLite de producción."""
    assert SNAPSHOT_PATH.is_file(), (
        f"El archivo snapshot {SNAPSHOT_PATH} debe existir para ejecutar las pruebas"
    )
    return PostgresLegacySeeder(sqlite_snapshot_path=SNAPSHOT_PATH)


def test_extract_top5_sources_from_snapshot(
    snapshot_seeder: PostgresLegacySeeder,
) -> None:
    """Verifica la extracción y saneamiento de las 5 fuentes estratégicas."""
    sources = snapshot_seeder.extract_sources(source_codes=TOP_5_SOURCES)

    assert len(sources) == 5
    codes = {s.canonical_code for s in sources}
    assert codes == {
        "BO_BCB",
        "BO_ASFI",
        "BO_INE",
        "BO_BBV",
        "BO_APS",
    }

    # Verificar entidad BCB
    bcb = next(s for s in sources if s.canonical_code == "BO_BCB")
    assert bcb.source_id == 1
    assert bcb.official_name == "Banco Central de Bolivia"
    assert bcb.homepage == "https://www.bcb.gob.bo/"
    assert bcb.country_iso == "BOL"
    assert bcb.is_active is True

    # Verificar entidad APS (sin descargas en prod)
    aps = next(s for s in sources if s.canonical_code == "BO_APS")
    assert aps.source_id == 7
    assert aps.short_name == "APS"
    assert aps.homepage == "https://www.aps.gob.bo/"


def test_extract_top5_datasets_from_snapshot(
    snapshot_seeder: PostgresLegacySeeder,
) -> None:
    """Verifica la extracción y especificación técnica de los 5 datasets."""
    datasets = snapshot_seeder.extract_datasets(
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    assert len(datasets) == 5
    ds_by_file = {ds.legacy_file_code: ds for ds in datasets}

    # 1. BCB - Tasas Interés Diario (xlsx)
    bcb_ds = ds_by_file["D_BO_000000484"]
    assert bcb_ds.dataset_code == "DS_BO_BCB_D_BO_000000484"
    assert bcb_ds.source_id == 1
    assert bcb_ds.periodicity == Periodicity.DAILY
    assert bcb_ds.expected_format == "xlsx"

    # 2. ASFI - Fondos de Inversión (xlsx fallback)
    asfi_ds = ds_by_file["D_BO_000000058"]
    assert asfi_ds.dataset_code == "DS_BO_ASFI_D_BO_000000058"
    assert asfi_ds.source_id == 8
    assert asfi_ds.periodicity == Periodicity.DAILY
    assert asfi_ds.expected_format == "xlsx"

    # 3. INE - Índice IPC Mensual (xlsx fallback)
    ine_ds = ds_by_file["D_BO_000000016"]
    assert ine_ds.dataset_code == "DS_BO_INE_D_BO_000000016"
    assert ine_ds.source_id == 37
    assert ine_ds.periodicity == Periodicity.MONTHLY

    # 4. BBV - Boletín Diario Bursátil (xlsx fallback)
    bbv_ds = ds_by_file["D_BO_000000481"]
    assert bbv_ds.dataset_code == "DS_BO_BBV_D_BO_000000481"
    assert bbv_ds.source_id == 15
    assert bbv_ds.periodicity == Periodicity.DAILY

    # 5. APS - Boletín Seguros Mensual (pdf)
    aps_ds = ds_by_file["D_BO_000000447"]
    assert aps_ds.dataset_code == "DS_BO_APS_D_BO_000000447"
    assert aps_ds.source_id == 7
    assert aps_ds.periodicity == Periodicity.MONTHLY
    assert aps_ds.expected_format == "pdf"


def test_seed_catalog_memory_top5(
    snapshot_seeder: PostgresLegacySeeder,
) -> None:
    """Verifica el sembrado de los Top 5 en InMemoryCatalogRepository."""
    repo = InMemoryCatalogRepository()
    src_count, ds_count = snapshot_seeder.seed_catalog(
        catalog=repo,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    assert src_count == 5
    assert ds_count == 5

    # Consultar por código canónico de fuente
    bcb = repo.get_source_by_code("BO_BCB")
    assert bcb is not None
    assert bcb.official_name == "Banco Central de Bolivia"

    # Consultar datasets por id de fuente
    bcb_datasets = repo.list_datasets(source_id=1)
    assert len(bcb_datasets) == 1
    assert bcb_datasets[0].legacy_file_code == "D_BO_000000484"

    # Consultar dataset por legacy code
    ds_aps = repo.get_dataset_by_legacy_code("D_BO_000000447")
    assert ds_aps is not None
    assert ds_aps.expected_format == "pdf"


def test_seed_catalog_sqlite_persistence_top5(
    snapshot_seeder: PostgresLegacySeeder,
    tmp_path: Path,
) -> None:
    """Verifica la persistencia y lectura del Catálogo Top 5 en SQLite real."""
    db_file = tmp_path / "nexus_catalog.db"
    adapter = SqliteLineageAdapter(db_path=db_file)

    src_count, ds_count = snapshot_seeder.seed_catalog(
        catalog=adapter,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    assert src_count == 5
    assert ds_count == 5

    # Listar todas las fuentes en SQLite
    sources = adapter.list_sources()
    assert len(sources) == 5

    # Recuperar fuente por código canónico
    bbv = adapter.get_source_by_code("BO_BBV")
    assert bbv is not None
    assert bbv.short_name == "BBV"

    # Recuperar dataset por código canónico
    ds_bbv = adapter.get_dataset("DS_BO_BBV_D_BO_000000481")
    assert ds_bbv is not None
    assert ds_bbv.periodicity == Periodicity.DAILY

    # Recuperar dataset por legacy file code
    ds_ine = adapter.get_dataset_by_legacy_code("D_BO_000000016")
    assert ds_ine is not None
    assert ds_ine.source_id == 37

    # Instanciar un nuevo adaptador sobre el mismo archivo para validar persistencia
    adapter_reloaded = SqliteLineageAdapter(db_path=db_file)
    assert len(adapter_reloaded.list_sources()) == 5
    assert len(adapter_reloaded.list_datasets()) == 5


def test_snapshot_seeder_nonexistent_file_raises_error(tmp_path: Path) -> None:
    """Verifica que indicar un archivo inexistente levanta LegacySeederError."""
    fake_path = tmp_path / "nonexistent.db"
    seeder = PostgresLegacySeeder(sqlite_snapshot_path=fake_path)

    with pytest.raises(LegacySeederError, match="Snapshot SQLite no encontrado"):
        seeder.extract_sources()
