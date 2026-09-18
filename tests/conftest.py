"""Configuración común y fixtures para las pruebas de Prospector Interno."""

from pathlib import Path
import pytest

from prospector_interno.adapters.memory_state import (
    InMemoryCatalogRepository,
    InMemoryGatekeeperState,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.models import IngestionRequest


@pytest.fixture
def temp_bronze_dir(tmp_path: Path) -> Path:
    """Provee un directorio temporal limpio para la Capa Bronze."""
    bronze_dir = tmp_path / "bronze_store"
    bronze_dir.mkdir()
    return bronze_dir


@pytest.fixture
def bronze_storage(temp_bronze_dir: Path) -> LocalBronzeStorage:
    """Instancia configurada de almacenamiento Bronze local."""
    return LocalBronzeStorage(base_path=temp_bronze_dir)


@pytest.fixture
def state_registry() -> InMemoryGatekeeperState:
    """Registro en memoria de locks y tareas en vuelo."""
    return InMemoryGatekeeperState()


@pytest.fixture
def catalog_repo() -> InMemoryCatalogRepository:
    """Catálogo canónico en memoria."""
    return InMemoryCatalogRepository()


@pytest.fixture
def gatekeeper(
    bronze_storage: LocalBronzeStorage,
    state_registry: InMemoryGatekeeperState,
) -> PreFlightGatekeeper:
    """Instancia del Pre-flight Gatekeeper lista para pruebas."""
    return PreFlightGatekeeper(
        storage=bronze_storage,
        state_registry=state_registry,
    )


@pytest.fixture
def sample_request() -> IngestionRequest:
    """Solicitud de ingesta canónica de prueba."""
    return IngestionRequest(
        request_id="req-test-001",
        dataset_code="DS_BO_BCB_INFLATION",
        domain="bcb.gob.bo",
        target_url="https://www.bcb.gob.bo/datos/inflacion.xlsx",
        period="2026-08",
        force_refresh=False,
    )
