"""Prueba de Integración End-to-End: Prospector Externo + Bridge + Interno.

Demuestra la interoperabilidad real entre ambos prospectores utilizando
los outputs generados por el Prospector Externo en output/finrural/ y output/bbv/.
"""

import socket
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app

client = TestClient(app)


def _is_external_service_running() -> bool:
    """Verifica si el Prospector Externo está activo en el puerto 8000."""
    try:
        with socket.create_connection(("127.0.0.1", 8000), timeout=0.3):
            return True
    except OSError:
        return False


def test_e2e_reconciliation_bridge_with_finrural() -> None:
    """Valida el ciclo completo de conciliación con FINRURAL."""
    map_path = Path("../prospector_externo/output/finrural/mapa_finrural.json")
    if not map_path.exists():
        map_path = Path("prospector_externo/output/finrural/mapa_finrural.json")

    payload = {
        "source_id": "finrural",
        "external_map_path": str(map_path),
    }

    # 1. Ejecutar Diff de Conciliación (HUI-18)
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["source_id"] == "finrural"
    assert data["total_external_resources"] >= 130
    assert data["total_new"] >= 0
    assert "Conciliación finalizada" in data["summary_message"]


def test_e2e_reconciliation_bridge_pure_rest_with_finrural() -> None:
    """Valida el ciclo de conciliación vía REST con ambos prospectores levantados."""
    if not _is_external_service_running():
        pytest.skip("Prospector Externo no está en ejecución en puerto 8000")

    payload = {
        "source_id": "finrural",
        "external_api_url": "http://0.0.0.0:8000",
    }
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["source_id"] == "finrural"
    assert data["total_external_resources"] >= 130
    assert "Conciliación finalizada" in data["summary_message"]


def test_e2e_sync_execution_pure_rest_with_finrural() -> None:
    """Valida la generación y ejecución de SyncPlan hacia Bronze vía REST puro."""
    if not _is_external_service_running():
        pytest.skip("Prospector Externo no está en ejecución en puerto 8000")

    payload = {
        "source_id": "finrural",
        "external_api_url": "http://0.0.0.0:8000",
        "max_downloads": 2,
        "execute_downloads": True,
    }
    response = client.post("/api/v1/bridge/sync", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["source_id"] == "finrural"
    assert data["total_tasks"] == 2
    assert data["is_executed"] is True
    assert "completado" in data["summary"].lower()


def test_e2e_sync_execution_with_bbv() -> None:
    """Valida generación y despacho del SyncPlan hacia Bronze con BBV."""
    map_path = Path("../prospector_externo/output/bbv/mapa_bbv.json")
    if not map_path.exists():
        map_path = Path("prospector_externo/output/bbv/mapa_bbv.json")

    payload = {
        "source_id": "bbv",
        "external_map_path": str(map_path),
        "max_downloads": 2,
    }

    # 2. Generar y Ejecutar SyncPlan (HUI-19)
    response = client.post("/api/v1/bridge/sync", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["source_id"] == "bbv"
    assert data["total_tasks"] == 2
    assert data["is_executed"] is True
    assert "completado" in data["summary"].lower()


def test_e2e_bridge_handles_missing_file_error() -> None:
    """Verifica manejo robusto de error 404 cuando el mapa no existe."""
    payload = {
        "source_id": "desconocido",
        "external_map_path": "ruta/inexistente/mapa.json",
    }
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 404
    assert "No se encontró el mapa" in response.json()["detail"]
