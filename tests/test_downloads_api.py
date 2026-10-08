"""Pruebas de integración HTTP para los endpoints de monitoreo de descargas (HUI-03)."""

from pathlib import Path
from fastapi.testclient import TestClient
import pytest

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.downloads_router import (
    get_download_tracking,
    set_download_tracking,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    _derivation_filter,
    _state,
    _storage,
    set_download_state_filter,
    set_gatekeeper,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)


@pytest.fixture
def clean_client(tmp_path: Path) -> TestClient:
    """Fixture que configura adaptadores SQLite aislados en memoria para la API."""
    db_file = tmp_path / "api_test.db"
    adapter = SqliteDownloadTrackingAdapter(db_path=db_file)
    set_download_tracking(adapter)

    # Inyectar filtro en el gatekeeper del router
    filter_instance = DownloadStateFilter(tracking_port=adapter)
    set_download_state_filter(filter_instance)

    # Configurar gatekeeper aislado con el nuevo filtro
    gk = PreFlightGatekeeper(
        storage=_storage,
        state_registry=_state,
        derivation_filter=_derivation_filter,
        download_state_filter=filter_instance,
    )
    set_gatekeeper(gk)

    return TestClient(app)


def test_api_get_downloads_status(clean_client: TestClient) -> None:
    """Verifica GET /api/v1/downloads/status y filtrado por source_id."""
    adapter = get_download_tracking()
    adapter.record_download(
        DownloadRecord(
            download_id="dl_1",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/active1.xlsx",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="task_1",
        )
    )
    adapter.record_download(
        DownloadRecord(
            download_id="dl_2",
            source_id="BO_ASFI",
            canonical_url="https://asfi.gob.bo/active2.xlsx",
            status=DownloadLifecycleStatus.PENDING,
            task_id="task_2",
        )
    )

    # 1. Sin filtro
    resp = clean_client.get("/api/v1/downloads/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_active"] == 2
    assert len(data["downloads"]) == 2

    # 2. Con filtro por source_id
    resp_bcb = clean_client.get("/api/v1/downloads/status?source_id=BO_BCB")
    assert resp_bcb.status_code == 200
    data_bcb = resp_bcb.json()
    assert data_bcb["total_active"] == 1
    assert data_bcb["downloads"][0]["source_id"] == "BO_BCB"


def test_api_get_download_tracking_detail(clean_client: TestClient) -> None:
    """Verifica GET /api/v1/downloads/tracking/{id} y caso 404."""
    adapter = get_download_tracking()
    adapter.record_download(
        DownloadRecord(
            download_id="dl_detail_99",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/detail.xlsx",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="task_detail",
            subscribers=["req_1", "req_2"],
        )
    )

    # Éxito 200
    resp = clean_client.get("/api/v1/downloads/tracking/dl_detail_99")
    assert resp.status_code == 200
    data = resp.json()
    assert data["download_id"] == "dl_detail_99"
    assert data["task_id"] == "task_detail"
    assert data["subscribers_count"] == 2

    # No encontrado 404
    resp_404 = clean_client.get("/api/v1/downloads/tracking/inexistente")
    assert resp_404.status_code == 404


def test_api_gatekeeper_evaluate_returns_in_progress(
    clean_client: TestClient,
) -> None:
    """Criterio 1: POST /api/v1/gatekeeper/evaluate retorna status='IN_PROGRESS'."""
    adapter = get_download_tracking()
    url = "https://www.asfi.gob.bo/data/cartera_2024-06.xlsx"
    adapter.record_download(
        DownloadRecord(
            download_id="dl_asfi_2024_06",
            source_id="asfi",
            canonical_url=url,
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="worker_active_asfi_44",
        )
    )

    payload = {
        "dataset_id": "mart_cartera",
        "domain": "asfi.gob.bo",
        "target_url": url,
        "temporal_period": "2024-06",
        "request_id": "analista_http_req_99",
    }
    resp = clean_client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert resp.status_code == 200
    data = resp.json()

    assert data["verdict"] == "IN_PROGRESS"
    assert data["status"] == "IN_PROGRESS"
    assert data["task_id"] == "worker_active_asfi_44"
    assert data["in_flight_task_id"] == "worker_active_asfi_44"
    assert data["lock_acquired"] is False
    assert data["subscribers_count"] == 1
    assert "worker_active_asfi_44" in data["reason"]
