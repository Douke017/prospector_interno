"""Pruebas de integración para PostgresLegacySeeder y migración a Portal Nexus."""

import pytest

from prospector_interno.adapters.legacy.backup_seeder import PostgresLegacySeeder
from prospector_interno.adapters.memory_state import InMemoryCatalogRepository
from prospector_interno.domain.models import Periodicity

pytestmark = pytest.mark.legacy_pg


@pytest.fixture
def seeder() -> PostgresLegacySeeder:
    """Instancia del seeder conectada al PostgreSQL local restaurado."""
    return PostgresLegacySeeder(
        host="localhost",
        port=5434,
        dbname="platform_db",
        user="postgres",
        password="postgres",
    )


def test_extract_sources_cleans_debt_and_normalizes(
    seeder: PostgresLegacySeeder,
) -> None:
    """Verifica que las 74 fuentes del backup legacy se extraen y normalizan."""
    sources = seeder.extract_sources()

    # En el dump legacy los IDs van de 1 a 75, con el ID 72
    # omitido/eliminado en la base de datos (74 registros reales).
    assert len(sources) == 74

    # Validar entidad emblemática: BCB
    bcb = next((s for s in sources if s.short_name == "BCB"), None)
    assert bcb is not None
    assert bcb.canonical_code == "BO_BCB"
    assert bcb.official_name == "Banco Central de Bolivia"
    assert bcb.homepage == "https://www.bcb.gob.bo/"
    assert bcb.country_iso == "BOL"
    assert bcb.is_active is True

    # Validar saneamiento de deuda técnica: cadenas 'vacio' o 'no-data' deben ser None
    ada = next((s for s in sources if s.short_name == "ADA"), None)
    assert ada is not None
    assert ada.homepage is None  # En legacy decía 'vacio'

    fidalga = next((s for s in sources if s.short_name == "FIDALGA"), None)
    assert fidalga is not None
    assert fidalga.homepage is None  # En legacy decía 'no-data'


def test_extract_datasets_from_legacy_files(seeder: PostgresLegacySeeder) -> None:
    """Verifica que los archivos de configuración se extraen como datasets de Nexus."""
    datasets = seeder.extract_datasets()

    assert len(datasets) > 0

    # Validar que ningún dataset tiene código vacío
    for ds in datasets[:20]:
        assert ds.dataset_code.startswith("DS_BO_")
        assert ds.legacy_file_code is not None
        assert ds.legacy_file_code.startswith("D_BO_")
        assert isinstance(ds.periodicity, Periodicity)
        assert ds.is_active is True


def test_seed_catalog_populates_nexus_repository(
    seeder: PostgresLegacySeeder,
    catalog_repo: InMemoryCatalogRepository,
) -> None:
    """Verifica el flujo completo de sembrado hacia el catálogo canónico."""
    sources_count, datasets_count = seeder.seed_catalog(catalog_repo)

    assert sources_count == 74
    assert datasets_count > 0

    # Recuperar por código canónico en el catálogo
    bcb_source = catalog_repo.get_source_by_code("BO_BCB")
    assert bcb_source is not None
    assert bcb_source.official_name == "Banco Central de Bolivia"

    asfi_source = catalog_repo.get_source_by_code("BO_ASFI")
    assert asfi_source is not None
    assert asfi_source.country_iso == "BOL"
