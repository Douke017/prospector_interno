"""Pool Asíncrono Real de Workers de Descarga (Objetivos 5 y 6).

Consume `download_tracking` mediante `FOR UPDATE SKIP LOCKED` (PostgreSQL) o
reclamo atómico (SQLite en pruebas), ejecuta transferencias HTTP reales con
streaming a disco (`.tmp` -> `rename`), aplica `DomainTokenBucket` y
`DomainCircuitBreaker`, verifica SHA-256 en vuelo con deduplicación post-descarga
(`asset_aliases` + `deduplication_audit`), ejecuta perfilado automático hookable,
emite eventos SSE en vivo y notifica de forma idempotente al Prospector Externo.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse
import uuid

import httpx

from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
    parse_retry_after,
)
from prospector_interno.application.scheduler import DomainTokenBucket
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.exceptions import CircuitOpenError
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    DownloadTrackingPort,
    LineageRepositoryPort,
)

logger = logging.getLogger("prospector_interno.worker_pool")

ACTIVE_QUEUE_STATUSES = {
    DownloadLifecycleStatus.QUEUED,
    DownloadLifecycleStatus.PENDING,
    DownloadLifecycleStatus.DOWNLOADING,
    DownloadLifecycleStatus.VERIFYING_SHA256,
}


class DownloadEventBroker:
    """Bus thread-safe de eventos en vivo para SSE (`GET /api/v1/downloads/stream`)."""

    def __init__(self, max_history: int = 200) -> None:
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []
        self._seq: int = 0
        self._max_history = max_history

    def publish(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._seq += 1
            ev = {
                "seq": self._seq,
                "event": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": payload,
            }
            self._events.append(ev)
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history :]
            return ev

    def since(self, last_seq: int) -> list[dict[str, Any]]:
        with self._lock:
            return [ev for ev in self._events if ev["seq"] > last_seq]

    @property
    def latest_seq(self) -> int:
        with self._lock:
            return self._seq


class AsyncDownloadWorkerPool:
    """Ejecutor real multi-worker para descargas físicas, SHA-256, dedup y callbacks."""

    def __init__(
        self,
        *,
        tracking_port: DownloadTrackingPort,
        bronze_storage: BronzeStoragePort,
        lineage_adapter: LineageRepositoryPort | None = None,
        dedup_repo: Any = None,
        url_inventory_repo: Any = None,
        rate_limiter: DomainTokenBucket | None = None,
        circuit_breaker: DomainCircuitBreaker | None = None,
        max_workers: int = 3,
        chunk_size: int = 64 * 1024,
        timeout_seconds: float = 30.0,
        http_transport: httpx.BaseTransport | None = None,
        external_api_url: str | None = None,
        external_callback_transport: httpx.BaseTransport | None = None,
        on_asset_custodied: Callable[[str, Path, str, str], None] | None = None,
    ) -> None:
        self.tracking = tracking_port
        self.bronze = bronze_storage
        self.lineage = lineage_adapter
        self.dedup_repo = dedup_repo
        self.url_inventory_repo = url_inventory_repo
        self.rate_limiter = rate_limiter or DomainTokenBucket(
            default_rate=2.0, default_capacity=5.0
        )
        self.circuit_breaker = circuit_breaker or DomainCircuitBreaker(
            default_cooldown_seconds=60.0
        )
        self.max_workers = max(1, max_workers)
        self.chunk_size = chunk_size
        self.timeout_seconds = timeout_seconds
        self.http_transport = http_transport
        self.external_api_url = (
            external_api_url
            if external_api_url is not None
            else os.environ.get("PROSPECTOR_EXTERNO_URL", "http://127.0.0.1:8000")
        )
        self.external_callback_transport = external_callback_transport
        self.on_asset_custodied = on_asset_custodied
        self.events = DownloadEventBroker()

        self._lock = threading.Lock()
        self._cancel_flags: dict[str, threading.Event] = {}
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._started = False

    def start(self) -> None:
        """Arranca el pool de hilos worker en segundo plano (idempotente)."""
        with self._lock:
            if self._started:
                return
            self._stop_event.clear()
            self._threads = []
            for idx in range(self.max_workers):
                wid = f"worker-{os.getpid()}-{idx + 1}"
                t = threading.Thread(
                    target=self._worker_loop,
                    args=(wid,),
                    name=f"datax-dl-{idx + 1}",
                    daemon=True,
                )
                t.start()
                self._threads.append(t)
            self._started = True

    def stop(self, timeout: float = 5.0) -> None:
        """Detiene limpiamente los hilos worker."""
        with self._lock:
            if not self._started:
                return
            self._stop_event.set()
            self._wake_event.set()
            threads = list(self._threads)
            self._started = False
        for t in threads:
            t.join(timeout=timeout)

    def wake(self) -> None:
        """Despierta a los workers cuando entra una nueva descarga."""
        self._wake_event.set()

    def enqueue(
        self,
        *,
        source_id: str,
        canonical_url: str,
        request_id: str | None = None,
        task_id: str | None = None,
        priority: int = 5,
        resource_key: str | None = None,
        dataset_code: str | None = None,
        period_label: str | None = None,
        etag: str | None = None,
        initial_status: DownloadLifecycleStatus = DownloadLifecycleStatus.QUEUED,
        auto_wake: bool = True,
    ) -> tuple[DownloadRecord, bool]:
        """Encola una descarga real aplicando coalescing concurrente sobre URLs activas.

        Devuelve `(record, coalesced)`. Si ya existe una tarea activa para esa URL,
        incrementa `subscribers_count` sin crear transferencia duplicada.
        """
        clean_url = canonical_url.strip()
        req_id = request_id or f"req_{uuid.uuid4().hex[:8]}"

        existing = self.tracking.find_by_url(clean_url)
        if existing is not None and existing.status in ACTIVE_QUEUE_STATUSES:
            self.tracking.subscribe_request(existing.download_id, req_id)
            refreshed = self.tracking.get_download(existing.download_id) or existing
            self.events.publish(
                "download_coalesced",
                {
                    "download_id": refreshed.download_id,
                    "source_id": refreshed.source_id,
                    "canonical_url": refreshed.canonical_url,
                    "status": refreshed.status.value,
                    "subscribers_count": len(refreshed.subscribers),
                },
            )
            return refreshed, True

        dl_id = f"dl_{uuid.uuid4().hex[:10]}"
        t_id = task_id or f"job_{source_id.lower()}_{uuid.uuid4().hex[:6]}"
        rec = DownloadRecord(
            download_id=dl_id,
            source_id=source_id.strip().upper(),
            canonical_url=clean_url,
            status=initial_status,
            task_id=t_id,
            subscribers=[req_id],
            started_at=datetime.now(timezone.utc),
            priority=priority,
            resource_key=resource_key,
            dataset_code=dataset_code or f"RAW_{source_id.strip().upper()}",
            period_label=period_label or datetime.now(timezone.utc).strftime("%Y-%m"),
            etag=etag,
        )
        self.tracking.record_download(rec)
        self.events.publish("download_queued", self._record_event_payload(rec))
        if auto_wake and initial_status in (
            DownloadLifecycleStatus.QUEUED,
            DownloadLifecycleStatus.PENDING,
        ):
            self.wake()
        return rec, False

    def retry_download(self, download_id: str) -> DownloadRecord | None:
        """Reencola una descarga fallida o cancelada preservando su trazabilidad."""
        rec = self.tracking.get_download(download_id)
        if rec is None:
            return None
        with self._lock:
            self._cancel_flags.pop(download_id, None)
        next_retries = rec.retry_count + 1
        updated_rec = rec.model_copy(
            update={
                "status": DownloadLifecycleStatus.QUEUED,
                "last_error_code": None,
                "retry_count": next_retries,
                "completed_at": None,
                "speed_bps": 0.0,
            }
        )
        self.tracking.record_download(updated_rec)
        self.events.publish("download_requeued", self._record_event_payload(updated_rec))
        self.wake()
        return updated_rec

    def cancel_download(self, download_id: str) -> DownloadRecord | None:
        """Solicita cancelación cooperativa de una descarga en cola o en ejecución."""
        rec = self.tracking.get_download(download_id)
        if rec is None:
            return None
        with self._lock:
            flag = self._cancel_flags.setdefault(download_id, threading.Event())
            flag.set()
        if rec.status in (
            DownloadLifecycleStatus.QUEUED,
            DownloadLifecycleStatus.PENDING,
            DownloadLifecycleStatus.DOWNLOADING,
            DownloadLifecycleStatus.VERIFYING_SHA256,
        ):
            self.tracking.update_status(
                download_id,
                DownloadLifecycleStatus.CANCELLED,
                error_code="CANCELLED_BY_OPERATOR",
            )
            refreshed = self.tracking.get_download(download_id) or rec
            self.events.publish("download_cancelled", self._record_event_payload(refreshed))
            return refreshed
        return rec

    def get_circuit_statuses(self, domains: list[str] | None = None) -> list[dict[str, Any]]:
        """Expone el estado del disyuntor de red por dominio."""
        target_domains = (
            domains
            if domains is not None
            else sorted(getattr(self.circuit_breaker, "_domains", {}).keys())
        )
        return [
            self.circuit_breaker.get_status(d).model_dump(mode="json")
            for d in target_domains
        ]

    def process_one(self, worker_id: str = "worker-sync") -> DownloadRecord | None:
        """Reclama y ejecuta exactamente un trabajo pendiente (útil también en tests)."""
        if not hasattr(self.tracking, "claim_next_pending"):
            return None
        claimed = self.tracking.claim_next_pending(worker_id)
        if claimed is None:
            return None
        return self._execute_job(claimed, worker_id)

    def _worker_loop(self, worker_id: str) -> None:
        while not self._stop_event.is_set():
            try:
                processed = self.process_one(worker_id)
                if processed is not None:
                    continue
            except Exception:
                logger.exception("Error inesperado en worker de descarga %s", worker_id)
            self._wake_event.wait(timeout=0.35)
            self._wake_event.clear()

    def _is_cancelled(self, download_id: str) -> bool:
        with self._lock:
            flag = self._cancel_flags.get(download_id)
        return bool(flag and flag.is_set())

    def _execute_job(self, job: DownloadRecord, worker_id: str) -> DownloadRecord:
        dl_id = job.download_id
        url = job.canonical_url
        domain = (urlparse(url).netloc or job.source_id.lower()).lower()
        dataset_code = job.dataset_code or f"RAW_{job.source_id.upper()}"
        period = job.period_label or datetime.now(timezone.utc).strftime("%Y-%m")
        ext = Path(urlparse(url).path).suffix.lstrip(".").lower() or "bin"

        self.events.publish("download_started", self._record_event_payload(job))
        self._notify_external_prospector(
            job, processing_status="EN_PROCESO", sha256=None, error=None
        )

        # 1. Verificar disyuntor del dominio
        try:
            self.circuit_breaker.verify_can_execute(domain)
        except CircuitOpenError as exc:
            self.tracking.update_status(
                dl_id,
                DownloadLifecycleStatus.FAILED,
                error_code=f"CIRCUIT_OPEN:{domain}",
            )
            failed = self.tracking.get_download(dl_id) or job
            self.events.publish("download_failed", self._record_event_payload(failed))
            self._notify_external_prospector(
                failed, processing_status="ERROR", sha256=None, error=str(exc)
            )
            return failed

        # 2. Control de tasa por dominio
        delay = self.rate_limiter.acquire_delay(domain)
        if delay > 0.0:
            time.sleep(min(delay, 1.5))

        tmp_dir = self.bronze.base_path / "_inflight_tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / f"{dl_id}.{ext}.tmp"

        hasher = hashlib.sha256()
        bytes_read = 0
        total_bytes = 0
        content_type: str | None = None
        response_etag: str | None = job.etag
        t_start = time.perf_counter()
        last_emit = t_start

        try:
            client_kwargs: dict[str, Any] = {
                "timeout": self.timeout_seconds,
                "follow_redirects": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (compatible; DATAX-Prospector-Interno/2.0)",
                },
            }
            if self.http_transport is not None:
                client_kwargs["transport"] = self.http_transport

            with httpx.Client(**client_kwargs) as client:
                with client.stream("GET", url) as resp:
                    status_code = resp.status_code
                    if status_code in (429, 503):
                        retry_after = parse_retry_after(resp.headers.get("Retry-After"))
                        self.circuit_breaker.record_failure(
                            domain=domain,
                            status_code=status_code,
                            retry_after_seconds=retry_after,
                        )
                        raise RuntimeError(f"HTTP_{status_code}")
                    if status_code >= 400:
                        raise RuntimeError(f"HTTP_{status_code}")

                    content_type = resp.headers.get("content-type")
                    response_etag = resp.headers.get("etag") or response_etag
                    raw_len = resp.headers.get("content-length")
                    if raw_len and raw_len.isdigit():
                        total_bytes = int(raw_len)

                    with open(tmp_path, "wb") as out_f:
                        for chunk in resp.iter_bytes(chunk_size=self.chunk_size):
                            if self._is_cancelled(dl_id):
                                raise asyncio.CancelledError("CANCELLED_BY_OPERATOR")
                            if not chunk:
                                continue
                            out_f.write(chunk)
                            hasher.update(chunk)
                            bytes_read += len(chunk)
                            now_t = time.perf_counter()
                            if (now_t - last_emit) >= 0.25 or bytes_read == total_bytes:
                                elapsed = max(0.001, now_t - t_start)
                                speed = round(bytes_read / elapsed, 2)
                                self._update_tracking_safe(
                                    dl_id,
                                    DownloadLifecycleStatus.DOWNLOADING,
                                    bytes_downloaded=bytes_read,
                                    total_bytes=max(total_bytes, bytes_read),
                                    speed_bps=speed,
                                )
                                self.events.publish(
                                    "download_progress",
                                    {
                                        "download_id": dl_id,
                                        "source_id": job.source_id,
                                        "canonical_url": url,
                                        "status": "DOWNLOADING",
                                        "bytes_downloaded": bytes_read,
                                        "total_bytes": max(total_bytes, bytes_read),
                                        "speed_bps": speed,
                                        "progress_pct": round(
                                            (bytes_read / max(total_bytes, bytes_read, 1)) * 100.0,
                                            1,
                                        ),
                                    },
                                )
                                last_emit = now_t

            self.circuit_breaker.record_success(domain)
            final_size = max(total_bytes, bytes_read)
            elapsed_total = max(0.001, time.perf_counter() - t_start)
            final_speed = round(bytes_read / elapsed_total, 2)

            # 3. Transición a VERIFYING_SHA256
            sha256_hex = hasher.hexdigest().lower()
            self._update_tracking_safe(
                dl_id,
                DownloadLifecycleStatus.VERIFYING_SHA256,
                bytes_downloaded=bytes_read,
                total_bytes=final_size,
                sha256_checksum=sha256_hex,
                speed_bps=final_speed,
            )
            self.events.publish(
                "download_verifying",
                {
                    "download_id": dl_id,
                    "source_id": job.source_id,
                    "canonical_url": url,
                    "status": "VERIFYING_SHA256",
                    "sha256_checksum": sha256_hex,
                },
            )

            # 4. Deduplicación post-descarga por SHA-256 binario exacto (Objetivo 5)
            existing_artifact = self.bronze.find_by_checksum(sha256_hex)
            if existing_artifact is not None:
                tmp_path.unlink(missing_ok=True)
                if self.dedup_repo is not None:
                    self.dedup_repo.register_alias(
                        canonical_url=url,
                        source_id=job.source_id,
                        target_asset_id=existing_artifact.artifact_id,
                        target_sha256=sha256_hex,
                        etag=response_etag,
                    )
                    self.dedup_repo.record_audit(
                        request_id=job.subscribers[0] if job.subscribers else dl_id,
                        source_id=job.source_id,
                        canonical_url=url,
                        criterion="SHA256_EXACT",
                        reused_asset_id=existing_artifact.artifact_id,
                        reused_sha256=sha256_hex,
                        bytes_saved=final_size,
                        detail=(
                            f"Colisión binaria SHA-256 exacta con activo "
                            f"'{existing_artifact.artifact_id}'; alias creado sin duplicar archivo."
                        ),
                    )
                self._update_tracking_safe(
                    dl_id,
                    DownloadLifecycleStatus.DEDUPLICATED,
                    bytes_downloaded=bytes_read,
                    total_bytes=final_size,
                    local_path=str(existing_artifact.file_path),
                    sha256_checksum=sha256_hex,
                    speed_bps=final_speed,
                )
                self._sync_url_inventory(
                    job.source_id, url, sha256_hex, str(existing_artifact.file_path)
                )
                finished = self.tracking.get_download(dl_id) or job
                self.events.publish("download_deduplicated", self._record_event_payload(finished))
                self._notify_external_prospector(
                    finished,
                    processing_status="PROCESADO_EXITOSAMENTE",
                    sha256=sha256_hex,
                    error=None,
                )
                return finished

            # 5. Almacenar binario intacto en custodia Bronze + registrar en internal_assets
            with open(tmp_path, "rb") as stream_f:
                artifact = self.bronze.store_stream(
                    domain=domain,
                    dataset_code=dataset_code,
                    period=period,
                    stream=stream_f,
                    expected_checksum=sha256_hex,
                    content_type=content_type,
                    url=url,
                    resource_key=job.resource_key,
                )
            tmp_path.unlink(missing_ok=True)

            if self.lineage is not None:
                now_utc = datetime.now(timezone.utc)
                self.lineage.record_asset(
                    InternalAssetRecord(
                        asset_id=artifact.artifact_id,
                        source_id=job.source_id,
                        dataset_id=dataset_code,
                        period_start=period,
                        period_end=period,
                        storage_layer=StorageLayer.BRONZE,
                        file_path=str(artifact.file_path),
                        content_hash_sha256=artifact.sha256_hash,
                        row_count=None,
                        size_bytes=artifact.size_bytes,
                        created_at=now_utc,
                        last_updated_at=now_utc,
                    )
                )

            if self.dedup_repo is not None:
                self.dedup_repo.register_alias(
                    canonical_url=url,
                    source_id=job.source_id,
                    target_asset_id=artifact.artifact_id,
                    target_sha256=artifact.sha256_hash,
                    etag=response_etag,
                )

            if self.on_asset_custodied is not None:
                try:
                    self.on_asset_custodied(
                        artifact.artifact_id,
                        artifact.file_path,
                        job.source_id,
                        artifact.sha256_hash,
                    )
                except Exception:
                    logger.warning(
                        "Perfilado automático falló para activo %s",
                        artifact.artifact_id,
                        exc_info=True,
                    )

            self._update_tracking_safe(
                dl_id,
                DownloadLifecycleStatus.COMPLETED,
                bytes_downloaded=bytes_read,
                total_bytes=final_size,
                local_path=str(artifact.file_path),
                sha256_checksum=artifact.sha256_hash,
                speed_bps=final_speed,
            )
            self._sync_url_inventory(
                job.source_id, url, artifact.sha256_hash, str(artifact.file_path)
            )
            finished = self.tracking.get_download(dl_id) or job
            self.events.publish("download_completed", self._record_event_payload(finished))
            self._notify_external_prospector(
                finished,
                processing_status="PROCESADO_EXITOSAMENTE",
                sha256=artifact.sha256_hash,
                error=None,
            )
            return finished

        except asyncio.CancelledError:
            tmp_path.unlink(missing_ok=True)
            self.tracking.update_status(
                dl_id,
                DownloadLifecycleStatus.CANCELLED,
                bytes_downloaded=bytes_read,
                total_bytes=total_bytes,
                error_code="CANCELLED_BY_OPERATOR",
            )
            cancelled = self.tracking.get_download(dl_id) or job
            self.events.publish("download_cancelled", self._record_event_payload(cancelled))
            return cancelled
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            err_code = str(exc)[:180] or type(exc).__name__
            self._update_tracking_safe(
                dl_id,
                DownloadLifecycleStatus.FAILED,
                bytes_downloaded=bytes_read,
                total_bytes=total_bytes,
                error_code=err_code,
            )
            failed = self.tracking.get_download(dl_id) or job
            self.events.publish("download_failed", self._record_event_payload(failed))
            self._notify_external_prospector(
                failed,
                processing_status="ERROR",
                sha256=None,
                error=err_code,
            )
            return failed

    def _update_tracking_safe(
        self,
        download_id: str,
        status: DownloadLifecycleStatus,
        *,
        bytes_downloaded: int | None = None,
        total_bytes: int | None = None,
        local_path: str | None = None,
        sha256_checksum: str | None = None,
        error_code: str | None = None,
        speed_bps: float | None = None,
    ) -> None:
        try:
            self.tracking.update_status(
                download_id,
                status,
                bytes_downloaded=bytes_downloaded,
                total_bytes=total_bytes,
                local_path=local_path,
                sha256_checksum=sha256_checksum,
                error_code=error_code,
                speed_bps=speed_bps,
            )
        except TypeError:
            self.tracking.update_status(
                download_id,
                status,
                bytes_downloaded=bytes_downloaded,
                total_bytes=total_bytes,
                local_path=local_path,
                sha256_checksum=sha256_checksum,
                error_code=error_code,
            )

    def _sync_url_inventory(
        self, source_id: str, canonical_url: str, sha256_hex: str, local_path: str
    ) -> None:
        if self.url_inventory_repo is None:
            return
        try:
            if hasattr(self.url_inventory_repo, "upsert_downloaded_url"):
                self.url_inventory_repo.upsert_downloaded_url(
                    source_id=source_id,
                    canonical_url=canonical_url,
                    sha256_hash=sha256_hex,
                    storage_path=local_path,
                )
        except Exception:
            return

    def _notify_external_prospector(
        self,
        job: DownloadRecord,
        *,
        processing_status: str,
        sha256: str | None,
        error: str | None,
    ) -> None:
        """Envía callback idempotente hacia Prospector Externo (contrato bidireccional)."""
        if not job.resource_key or not self.external_api_url:
            return
        base = self.external_api_url.rstrip("/")
        endpoint = f"{base}/catalog/resources/{job.resource_key}/status"
        payload = {
            "processing_status": processing_status,
            "processing_error": error,
            "actor": "PROSPECTOR_INTERNO",
            "download_id": job.download_id,
            "sha256": sha256,
            "idempotency_key": f"pi_{job.download_id}_{processing_status}",
        }
        client_kwargs: dict[str, Any] = {"timeout": 4.0}
        if self.external_callback_transport is not None:
            client_kwargs["transport"] = self.external_callback_transport
        try:
            with httpx.Client(**client_kwargs) as client:
                client.patch(endpoint, json=payload)
        except Exception:
            logger.debug("Callback hacia Prospector Externo no disponible para %s", job.resource_key)

    @staticmethod
    def _record_event_payload(rec: DownloadRecord) -> dict[str, Any]:
        total = max(rec.total_bytes, rec.bytes_downloaded, 0)
        pct = round((rec.bytes_downloaded / total) * 100.0, 1) if total > 0 else 0.0
        return {
            "download_id": rec.download_id,
            "source_id": rec.source_id,
            "canonical_url": rec.canonical_url,
            "status": rec.status.value,
            "bytes_downloaded": rec.bytes_downloaded,
            "total_bytes": rec.total_bytes,
            "progress_pct": pct,
            "speed_bps": rec.speed_bps,
            "retry_count": rec.retry_count,
            "max_retries": rec.max_retries,
            "last_error_code": rec.last_error_code,
            "sha256_checksum": rec.sha256_checksum,
            "local_path": rec.local_path,
            "task_id": rec.task_id,
            "worker_id": rec.worker_id,
            "subscribers_count": len(rec.subscribers),
            "resource_key": rec.resource_key,
        }
