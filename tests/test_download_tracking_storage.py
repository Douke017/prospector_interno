"""Pruebas para el adaptador de persistencia SQLite de download_tracking (HUI-03)."""

from datetime import datetime, timezone
from pathlib import Path

from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)


def test_sqlite_download_tracking_crud(tmp_path: Path) -> None:
    """Verifica operaciones CRUD básicas sobre download_tracking."""
    db_file = tmp_path / "test_catalog.db"
    adapter = SqliteDownloadTrackingAdapter(db_path=db_file)

    rec = DownloadRecord(
        download_id="hash_url_123",
        source_id="BO_ASFI",
        canonical_url="https://asfi.gob.bo/data/cartera.xlsx",
        status=DownloadLifecycleStatus.DOWNLOADING,
        task_id="task_worker_1",
        started_at=datetime.now(timezone.utc),
    )
    adapter.record_download(rec)

    # 1. Recuperar por ID
    saved = adapter.get_download("hash_url_123")
    assert saved is not None
    assert saved.source_id == "BO_ASFI"
    assert saved.status == DownloadLifecycleStatus.DOWNLOADING
    assert saved.task_id == "task_worker_1"
    assert saved.bytes_downloaded == 0

    # 2. Recuperar por URL
    by_url = adapter.find_by_url("https://asfi.gob.bo/data/cartera.xlsx")
    assert by_url is not None
    assert by_url.download_id == "hash_url_123"

    # 3. Actualizar progreso y estado
    adapter.update_status(
        download_id="hash_url_123",
        status=DownloadLifecycleStatus.COMPLETED,
        bytes_downloaded=1048576,
        total_bytes=1048576,
        local_path="data/bronze/BO_ASFI/cartera.xlsx",
        sha256_checksum="b" * 64,
    )

    updated = adapter.get_download("hash_url_123")
    assert updated is not None
    assert updated.status == DownloadLifecycleStatus.COMPLETED
    assert updated.bytes_downloaded == 1048576
    assert updated.sha256_checksum == "b" * 64
    assert updated.completed_at is not None

    adapter.close()


def test_sqlite_download_tracking_concurrent_subscribers(tmp_path: Path) -> None:
    """Verifica suscripción múltiple de peticiones sobre la misma tarea."""
    adapter = SqliteDownloadTrackingAdapter(db_path=tmp_path / "subs.db")

    adapter.record_download(
        DownloadRecord(
            download_id="dl_concurrent",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/active.zip",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="worker_active",
        )
    )

    ok1 = adapter.subscribe_request("dl_concurrent", "client_req_01")
    ok2 = adapter.subscribe_request("dl_concurrent", "client_req_02")
    # Repetir misma petición no debe duplicar en lista
    ok3 = adapter.subscribe_request("dl_concurrent", "client_req_01")

    assert ok1 is True
    assert ok2 is True
    assert ok3 is True

    record = adapter.get_download("dl_concurrent")
    assert record is not None
    assert record.subscribers == ["client_req_01", "client_req_02"]

    # Suscribir a ID inexistente retorna False
    assert adapter.subscribe_request("no_existe", "req") is False
    adapter.close()


def test_sqlite_list_active_downloads(tmp_path: Path) -> None:
    """Verifica filtro de listado de descargas activas en curso o cola."""
    adapter = SqliteDownloadTrackingAdapter(db_path=tmp_path / "active.db")

    adapter.record_download(
        DownloadRecord(
            download_id="dl_act_1",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/1.xlsx",
            status=DownloadLifecycleStatus.DOWNLOADING,
        )
    )
    adapter.record_download(
        DownloadRecord(
            download_id="dl_act_2",
            source_id="BO_ASFI",
            canonical_url="https://asfi.gob.bo/2.xlsx",
            status=DownloadLifecycleStatus.PENDING,
        )
    )
    adapter.record_download(
        DownloadRecord(
            download_id="dl_act_3",
            source_id="BO_INE",
            canonical_url="https://ine.gob.bo/3.xlsx",
            status=DownloadLifecycleStatus.COMPLETED,
        )
    )

    active = adapter.list_active_downloads()
    active_ids = {a.download_id for a in active}
    assert active_ids == {"dl_act_1", "dl_act_2"}

    adapter.close()
