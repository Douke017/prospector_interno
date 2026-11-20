"""Pruebas del Bloque 3: Migración 002, Gatekeeper Seguro/Batch, Workers Reales y Deduplicación."""

from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time

from fastapi.testclient import TestClient
import httpx

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.downloads_router import (
    get_worker_pool,
    set_worker_pool,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    set_dedup_repo,
    set_gatekeeper,
)
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.postgres_v2_repositories import (
    InMemoryDeduplicationRepository,
    PostgresGatekeeperState,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import SqliteLineageAdapter
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.download_worker_pool import AsyncDownloadWorkerPool
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
)
from prospector_interno.domain.gatekeeper.download_state_filter import DownloadStateFilter


def _start_local_file_server(routes: dict[str, tuple[int, bytes, str]]) -> tuple[ThreadingHTTPServer, str]:
    """Servidor HTTP local determinístico en puerto efímero sin internet externo."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            entry = routes.get(self.path)
            if entry is None:
                self.send_response(404)
                self.end_headers()
                return
            status_code, body, ctype = entry
            self.send_response(status_code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", f'"{hashlib.md5(body).hexdigest()}"')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def test_task_3_1_migration_002_no_hardcoded_passwords_and_pg_locks() -> None:
    mig_path = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "002_raw_data_prospector_evolution.sql"
    )
    sql_text = mig_path.read_text(encoding="utf-8")
    assert "PASSWORD" not in sql_text.upper()
    assert "TIMESTAMPTZ" in sql_text
    assert "JSONB" in sql_text
    assert "BIGINT" in sql_text

    # Probar locks distribuidos TTL directamente sobre PostgreSQL `gatekeeper_locks`
    pg_state = PostgresGatekeeperState(worker_id="pytest-worker")
    key = f"test:lock:{time.time_ns()}"
    assert pg_state.try_acquire_lock(key, "task-a", ttl_seconds=60) is True
    assert pg_state.try_acquire_lock(key, "task-b", ttl_seconds=60) is False
    assert pg_state.get_in_flight_task(key) == "task-a"
    pg_state.release_lock(key, "task-a")
    assert pg_state.get_in_flight_task(key) is None


def test_task_3_2_dry_run_no_orphan_locks_and_batch_sla_under_150ms(tmp_path: Path) -> None:
    storage = LocalBronzeStorage(base_path=tmp_path / "bronze")
    state = InMemoryGatekeeperState()
    tracking = SqliteDownloadTrackingAdapter(":memory:")
    dedup = InMemoryDeduplicationRepository()
    gk = PreFlightGatekeeper(
        storage=storage,
        state_registry=state,
        download_state_filter=DownloadStateFilter(tracking),
        dedup_repo=dedup,
    )
    set_gatekeeper(gk)
    set_dedup_repo(dedup)

    # Precargar 1 artefacto en Bronze para verificar action_links y ahorro
    import io

    stored = storage.store_stream(
        domain="datos.bcb.gob.bo",
        dataset_code="DS_SERIE_0",
        period="2026-01",
        stream=io.BytesIO(b"col1,col2\n1,2\n"),
        content_type="text/csv",
        url="https://datos.bcb.gob.bo/serie_0.csv",
    )

    client = TestClient(app)

    # 1. DRY_RUN por defecto NO genera locks huérfanos
    payload = {
        "dataset_code": "DS_NUEVO_RAW",
        "domain": "datos.bcb.gob.bo",
        "target_url": "https://datos.bcb.gob.bo/nuevo_2026.parquet",
        "period": "2026-04",
    }
    r1 = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    r2 = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["verdict"] == "PROCEED_DOWNLOAD"
    assert r1.json()["lock_acquired"] is False
    assert r2.json()["verdict"] == "PROCEED_DOWNLOAD"  # Sin lock huérfano!
    assert r2.json()["lock_acquired"] is False

    # 2. ACQUIRE_LOCK bloquea segundo concurrente
    r3 = client.post(
        "/api/v1/gatekeeper/evaluate",
        json={**payload, "mode": "ACQUIRE_LOCK", "request_id": "worker_1", "lock_ttl_seconds": 30},
    )
    r4 = client.post(
        "/api/v1/gatekeeper/evaluate",
        json={**payload, "mode": "ACQUIRE_LOCK", "request_id": "worker_2"},
    )
    assert r3.json()["lock_acquired"] is True
    assert r4.json()["verdict"] == "IN_FLIGHT_LOCKED"
    assert r4.json()["in_flight_task_id"] == "worker_1"

    # 3. Evaluación por lotes de 100 ítems debe cumplir SLA < 150 ms
    batch_items = [
        {
            "dataset_code": f"DS_SERIE_{i}",
            "domain": "datos.bcb.gob.bo",
            "target_url": f"https://datos.bcb.gob.bo/serie_{i}.csv",
            "period": "2026-01",
        }
        for i in range(100)
    ]
    t0 = time.perf_counter()
    batch_resp = client.post(
        "/api/v1/gatekeeper/evaluate-batch",
        json={"items": batch_items, "mode": "DRY_RUN"},
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    assert batch_resp.status_code == 200
    bdata = batch_resp.json()
    assert bdata["total"] == 100
    assert bdata["saved_downloads"] == 1  # serie_0 ya en Bronze
    assert bdata["elapsed_ms"] < 150.0
    assert elapsed_ms < 250.0
    first_res = bdata["results"][0]
    assert first_res["verdict"] == "CACHE_HIT_BRONZE"
    assert first_res["action_links"]["raw_download_url"] == f"/api/v1/catalog/assets/{stored.artifact_id}/download"

    # 4. Métricas p50/p95/p99
    metrics = client.get("/api/v1/gatekeeper/metrics").json()
    assert metrics["total_evaluations"] >= 104
    assert metrics["p95_ms"] >= 0.0
    assert metrics["network_calls_saved"] >= 2


def test_task_3_3_and_3_4_real_worker_coalescing_callback_sse_and_sha256_dedup(
    tmp_path: Path,
) -> None:
    payload_bytes = b"entidad,periodo,valor\nBCB,2026-01,100.5\nASFI,2026-01,250.0\n"
    expected_sha = hashlib.sha256(payload_bytes).hexdigest()

    server, base_url = _start_local_file_server(
        {
            "/files/serie_a.csv": (200, payload_bytes, "text/csv"),
            "/mirror/serie_b_identica.csv": (200, payload_bytes, "text/csv"),
            "/files/error_500.csv": (500, b"Internal Error", "text/plain"),
        }
    )

    callback_calls: list[dict] = []

    def mock_external_cb(request: httpx.Request) -> httpx.Response:
        callback_calls.append(
            {
                "url": str(request.url),
                "body": json.loads(request.content.decode("utf-8")),
            }
        )
        return httpx.Response(200, json={"ok": True})

    bronze = LocalBronzeStorage(base_path=tmp_path / "bronze")
    tracking = SqliteDownloadTrackingAdapter(tmp_path / "dl.db")
    lineage = SqliteLineageAdapter(tmp_path / "lineage.db")
    dedup = InMemoryDeduplicationRepository()
    gk = PreFlightGatekeeper(
        storage=bronze,
        state_registry=InMemoryGatekeeperState(),
        download_state_filter=DownloadStateFilter(tracking),
        dedup_repo=dedup,
    )
    pool = AsyncDownloadWorkerPool(
        tracking_port=tracking,
        bronze_storage=bronze,
        lineage_adapter=lineage,
        dedup_repo=dedup,
        max_workers=2,
        external_api_url="http://externo.local:8000",
        external_callback_transport=httpx.MockTransport(mock_external_cb),
    )

    prev_pool = get_worker_pool()
    set_worker_pool(pool)
    set_gatekeeper(gk)
    set_dedup_repo(dedup)
    client = TestClient(app)

    try:
        url_a = f"{base_url}/files/serie_a.csv"
        url_b = f"{base_url}/mirror/serie_b_identica.csv"
        url_err = f"{base_url}/files/error_500.csv"

        # 1. Encolar URL A con dos peticiones concurrentes -> coalescing real sin duplicar descarga
        q1 = client.post(
            "/api/v1/downloads/queue",
            json={
                "source_id": "BCB",
                "canonical_url": url_a,
                "request_id": "cientista_1",
                "resource_key": "rk_ext_001",
                "execute_now": False,
            },
        )
        q2 = client.post(
            "/api/v1/downloads/queue",
            json={
                "source_id": "BCB",
                "canonical_url": url_a,
                "request_id": "cientista_2",
                "resource_key": "rk_ext_001",
                "execute_now": False,
            },
        )
        assert q1.status_code == 200
        assert q2.status_code == 200
        assert q1.json()["download_id"] == q2.json()["download_id"]
        assert q2.json()["coalesced"] is True
        assert q2.json()["subscribers_count"] == 2

        # 2. Worker ejecuta realmente la descarga de URL A
        res_a = pool.process_one("worker-test-1")
        assert res_a is not None
        assert res_a.status == DownloadLifecycleStatus.COMPLETED
        assert res_a.sha256_checksum == expected_sha
        assert res_a.bytes_downloaded == len(payload_bytes)
        assert res_a.local_path is not None and Path(res_a.local_path).is_file()

        # Callback bidireccional idempotente enviado al Prospector Externo
        assert any(
            c["body"]["actor"] == "PROSPECTOR_INTERNO"
            and c["body"]["processing_status"] == "PROCESADO_EXITOSAMENTE"
            and c["body"]["sha256"] == expected_sha
            for c in callback_calls
        )

        # 3. Tarea 3.4: Segunda URL distinta con el MISMO payload binario -> DEDUPLICATED + alias sin duplicar archivo
        bronze_files_before = [p for p in bronze.base_path.rglob("*.dat") if "_inflight_tmp" not in str(p)]
        q_b = client.post(
            "/api/v1/downloads/queue",
            json={
                "source_id": "BCB",
                "canonical_url": url_b,
                "request_id": "cientista_3",
                "resource_key": "rk_ext_002",
                "execute_now": False,
            },
        )
        assert q_b.status_code == 200
        res_b = pool.process_one("worker-test-1")
        assert res_b is not None
        assert res_b.status == DownloadLifecycleStatus.DEDUPLICATED
        assert res_b.sha256_checksum == expected_sha

        bronze_files_after = [p for p in bronze.base_path.rglob("*.dat") if "_inflight_tmp" not in str(p)]
        assert len(bronze_files_after) == len(bronze_files_before) == 1

        # Auditoría de deduplicación y alias verificables vía API
        dedup_resp = client.get("/api/v1/gatekeeper/deduplication-audit")
        assert dedup_resp.status_code == 200
        ddata = dedup_resp.json()
        assert ddata["summary"]["total_bytes_saved"] >= len(payload_bytes)
        assert ddata["summary"]["by_criterion"].get("SHA256_EXACT", 0) >= 1
        assert any(a["canonical_url"] == url_b and a["target_sha256"] == expected_sha for a in ddata["aliases"])

        # 4. Descarga fallida -> FAILED -> Retry -> Cancel
        q_err = client.post(
            "/api/v1/downloads/queue",
            json={"source_id": "BCB", "canonical_url": url_err, "execute_now": False},
        )
        dl_err_id = q_err.json()["download_id"]
        res_err = pool.process_one("worker-test-1")
        assert res_err is not None
        assert res_err.status == DownloadLifecycleStatus.FAILED
        assert "HTTP_500" in (res_err.last_error_code or "")

        retry_resp = client.post(f"/api/v1/downloads/{dl_err_id}/retry")
        assert retry_resp.status_code == 200
        assert retry_resp.json()["status"] == "QUEUED"
        assert retry_resp.json()["retry_count"] == 1

        cancel_resp = client.post(f"/api/v1/downloads/{dl_err_id}/cancel")
        assert cancel_resp.status_code == 200
        assert cancel_resp.json()["status"] == "CANCELLED"

        # 5. Stream SSE devuelve text/event-stream con snapshot de cola
        sse_resp = client.get("/api/v1/downloads/stream?snapshot_only=true")
        assert sse_resp.status_code == 200
        assert sse_resp.headers["content-type"].startswith("text/event-stream")
        assert "event: queue_snapshot" in sse_resp.text
    finally:
        set_worker_pool(prev_pool)
        server.shutdown()
        server.server_close()
