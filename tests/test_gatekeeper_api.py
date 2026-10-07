"""Pruebas de integración HTTP para endpoints del Gatekeeper y Derivación (HUI-02)."""


from fastapi.testclient import TestClient
import pytest

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    set_derivation_filter,
    set_gatekeeper,
)
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.derivation_spec import DerivationStatus
from prospector_interno.domain.gatekeeper.derivation_filter import (
    AssetDerivationFilter,
)
from prospector_interno.domain.models import PreFlightVerdict
from prospector_interno.domain.ports import GoldPartitionPort

client = TestClient(app)


class MockPartitionPort(GoldPartitionPort):
    """Puerto de particiones para pruebas de integración de API."""

    def __init__(self, partitions: set[tuple[int, int]]) -> None:
        self._partitions = partitions

    def list_available_partitions(
        self, datamart: str = "mart_cartera"
    ) -> list[tuple[int, int]]:
        return sorted(list(self._partitions))

    def has_partition(
        self, datamart: str, gestion: int, mes: int
    ) -> bool:
        return (gestion, mes) in self._partitions


@pytest.fixture(autouse=True)
def setup_gatekeeper_env(tmp_path) -> None:
    """Configura el entorno de prueba con Gatekeeper y DerivationFilter aislados."""
    # 2024 tiene los 12 meses completos
    all_2024 = {(2024, m) for m in range(1, 13)}
    # 2025 tiene solo los meses 1 a 6
    s1_2025 = {(2025, m) for m in range(1, 7)}

    checker = MockPartitionPort(all_2024 | s1_2025)
    derivation_filter = AssetDerivationFilter(partition_checker=checker)
    storage = LocalBronzeStorage(base_path=tmp_path / "bronze")
    state = InMemoryGatekeeperState()

    gatekeeper = PreFlightGatekeeper(
        storage=storage,
        state_registry=state,
        derivation_filter=derivation_filter,
    )

    set_derivation_filter(derivation_filter)
    set_gatekeeper(gatekeeper)


def test_api_inspect_derivation_full_coverage() -> None:
    """Verifica endpoint POST /api/v1/gatekeeper/inspect con derivación completa."""
    payload = {
        "dataset_code": "mart_cartera",
        "period": "2024",
    }
    response = client.post("/api/v1/gatekeeper/inspect", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == DerivationStatus.DERIVABLE
    assert data["is_derivable"] is True
    assert data["coverage_ratio"] == 1.0
    assert len(data["available_periods"]) == 12
    assert len(data["missing_periods"]) == 0
    assert data["precompiled_sql"] is not None
    assert "gestion = 2024" in data["precompiled_sql"]


def test_api_inspect_derivation_missing_periods() -> None:
    """Verifica endpoint POST /api/v1/gatekeeper/inspect con periodos faltantes."""
    payload = {
        "dataset_code": "mart_cartera",
        "period": "2025",
    }
    response = client.post("/api/v1/gatekeeper/inspect", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == DerivationStatus.MISSING_PERIODS
    assert data["is_derivable"] is False
    assert data["coverage_ratio"] == 0.5
    assert len(data["missing_periods"]) == 6
    assert data["missing_periods"] == [
        "2025-07",
        "2025-08",
        "2025-09",
        "2025-10",
        "2025-11",
        "2025-12",
    ]


def test_api_evaluate_returns_reuse_derivation() -> None:
    """Verifica que POST /api/v1/gatekeeper/evaluate retorne REUSE_DERIVATION."""
    payload = {
        "dataset_code": "mart_cartera",
        "period": "2024",
        "target_url": "https://finrural.org.bo/data/2024_anual.pdf",
    }
    response = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["verdict"] == PreFlightVerdict.REUSE_DERIVATION
    assert data["lock_acquired"] is False
    assert data["derivation_query"] is not None
    assert "100% de cobertura disponible" in data["reason"]


def test_api_evaluate_incomplete_derivation_allows_download() -> None:
    """Verifica que si no es derivable, el Gatekeeper procede a descarga."""
    payload = {
        "dataset_code": "mart_cartera",
        "period": "2025",
        "target_url": "https://finrural.org.bo/data/2025_anual.pdf",
    }
    response = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert response.status_code == 200
    data = response.json()

    # Como no es derivable en Gold ni existe en Bronze, concede el lock de descarga
    assert data["verdict"] == PreFlightVerdict.PROCEED_DOWNLOAD
    assert data["lock_acquired"] is True
    assert len(data["missing_periods"]) == 6
