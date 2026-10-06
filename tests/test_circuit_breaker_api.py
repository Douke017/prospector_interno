"""Pruebas de integración para endpoints del Circuit Breaker (HUI-09)."""

from fastapi.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.domain.contracts.downloader_spec import CircuitState

client = TestClient(app)


def test_get_circuit_breaker_status_initial_closed() -> None:
    """Verifica consulta de estado inicial CLOSED para un dominio."""
    response = client.get("/api/v1/bridge/circuit-breaker/asfi.gob.bo")
    assert response.status_code == 200
    data = response.json()
    assert data["domain"] == "asfi.gob.bo"
    assert data["state"] == CircuitState.CLOSED
    assert data["failure_count"] == 0
    assert data["cooldown_seconds"] == 0.0


def test_reset_circuit_breaker_endpoint() -> None:
    """Verifica restablecimiento manual del disyuntor vía endpoint POST."""
    response = client.post("/api/v1/bridge/circuit-breaker/bcb.gob.bo/reset")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "RESET"
    assert data["domain"] == "bcb.gob.bo"
