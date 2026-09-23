"""Prueba de Integración End-to-End: Prospector Externo + Bridge + Interno.

Demuestra la interoperabilidad real entre ambos prospectores utilizando
los outputs generados por el Prospector Externo en output/finrural/ y output/bbv/.
"""

from pathlib import Path
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app

client = TestClient(app)


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
    assert data["total_external_resources"] == 130
    assert data["total_new"] > 0
    assert "Conciliación finalizada" in data["summary_message"]


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
