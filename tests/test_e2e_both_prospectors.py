"""Prueba de Integración End-to-End: Prospector Externo + Bridge + Interno.

Demuestra la interoperabilidad real entre ambos prospectores utilizando
los outputs generados por el Prospector Externo en output/finrural/ y output/bbv/.
"""

import io
import socket
import sys
from pathlib import Path
from typing import BinaryIO
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.domain.ports import HttpDownloaderPort

client = TestClient(app)


class _DeterministicPdfDownloader(HttpDownloaderPort):
    """Descargador determinista para pruebas E2E aisladas sin mutar red ni disco productivo."""

    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        return io.BytesIO(b"%PDF-1.4\nCONTENIDO_PRUEBA_AISLADA_BBV"), 200, "application/pdf"


def _is_external_service_running() -> bool:
    """Verifica si el Prospector Externo está activo en el puerto 8000."""
    try:
        with socket.create_connection(("127.0.0.1", 8000), timeout=0.3):
            return True
    except OSError:
        return False


def test_e2e_reconciliation_bridge_with_finrural(finrural_map_path: Path) -> None:
    """Valida el ciclo completo de conciliación con FINRURAL."""
    payload = {
        "source_id": "finrural",
        "external_map_path": str(finrural_map_path),
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


def test_e2e_sync_execution_with_bbv(
    tmp_path: Path,
    bbv_map_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valida generación y despacho del SyncPlan hacia Bronze aislado con BBV."""
    isolated_bronze = LocalBronzeStorage(base_path=tmp_path / "isolated_bronze")

    br_mod = sys.modules["prospector_interno.adapters.api.routers.bridge_router"]
    monkeypatch.setattr(br_mod._diff_engine, "_storage", isolated_bronze)
    monkeypatch.setattr(br_mod._sync_orchestrator, "_storage", isolated_bronze)
    monkeypatch.setattr(br_mod._sync_orchestrator._gatekeeper, "_storage", isolated_bronze)
    monkeypatch.setattr(
        br_mod._sync_orchestrator, "_downloader", _DeterministicPdfDownloader()
    )

    payload = {
        "source_id": "bbv",
        "external_map_path": str(bbv_map_path),
        "max_downloads": 2,
    }

    # 2. Generar y Ejecutar SyncPlan (HUI-19)
    response = client.post("/api/v1/bridge/sync", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["source_id"] == "bbv"
    assert data["total_tasks"] == 2
    assert data["is_executed"] is True
    assert data["completed_tasks"] == 2
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


def test_e2e_reconciliation_bridge_with_bcb(bcb_projection_path: Path) -> None:
    """Valida el ciclo E2E de conciliación y simulación Sync con el BCB."""
    map_path = bcb_projection_path

    payload = {
        "source_id": "bcb",
        "external_map_path": str(map_path),
    }
    # 1. Reconcile
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["source_id"] == "bcb"
    assert data["total_external_resources"] == 527
    assert "Conciliación finalizada" in data["summary_message"]

    # 2. Sync plan simulation (dry-run)
    sync_payload = {
        "source_id": "bcb",
        "external_map_path": str(map_path),
        "max_downloads": 5,
        "execute_downloads": False,
    }
    sync_resp = client.post("/api/v1/bridge/sync", json=sync_payload)
    assert sync_resp.status_code == 200
    sync_data = sync_resp.json()
    assert sync_data["source_id"] == "bcb"
    assert sync_data["total_tasks"] == 5
    assert sync_data["is_executed"] is False
    assert "dry-run" in sync_data["summary"]

