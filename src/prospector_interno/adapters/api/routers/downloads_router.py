"""Router de Cola Asíncrona Real de Descargas, Workers y Stream SSE (Objetivo 6)."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.schemas import (
    DownloadStatusItemResponse,
    DownloadsStatusListResponse,
)
from prospector_interno.application.download_worker_pool import (
    ACTIVE_QUEUE_STATUSES,
    AsyncDownloadWorkerPool,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.ports import DownloadTrackingPort

router = APIRouter(prefix="/downloads", tags=["5. Monitoreo de Ingesta & Descargas"])

_tracking_adapter: DownloadTrackingPort = deps.download_tracking
_worker_pool: AsyncDownloadWorkerPool = deps.download_worker_pool


class EnqueueDownloadRequest(BaseModel):
    """Solicitud para encolar una descarga física real en el pool de workers."""

    source_id: str = Field(..., examples=["BCB", "ASFI", "BBV"])
    canonical_url: str = Field(..., examples=["https://www.bcb.gob.bo/boletin_2026_09.xlsx"])
    status: str = Field(default="QUEUED", description="Estado inicial (QUEUED por defecto)")
    task_id: str | None = Field(default=None)
    request_id: str | None = Field(default=None)
    priority: int = Field(default=5, ge=1, le=10)
    resource_key: str | None = Field(default=None)
    dataset_code: str | None = Field(default=None)
    period_label: str | None = Field(default=None)
    etag: str | None = Field(default=None)
    execute_now: bool = Field(
        default=True,
        description="Si True, despierta al pool de workers para iniciar la descarga real.",
    )


def set_download_tracking(adapter: DownloadTrackingPort) -> None:
    """Permite inyectar un adaptador de tracking aislado para pruebas."""
    global _tracking_adapter
    _tracking_adapter = adapter
    _worker_pool.tracking = adapter


def get_download_tracking() -> DownloadTrackingPort:
    """Retorna el adaptador activo."""
    return _tracking_adapter


def set_worker_pool(pool: AsyncDownloadWorkerPool) -> None:
    """Permite inyectar un pool de workers aislado para pruebas."""
    global _worker_pool, _tracking_adapter
    _worker_pool = pool
    _tracking_adapter = pool.tracking


def get_worker_pool() -> AsyncDownloadWorkerPool:
    """Retorna el pool de workers activo."""
    return _worker_pool


def _to_item_response(
    record: DownloadRecord, *, coalesced: bool = False
) -> DownloadStatusItemResponse:
    started_str = record.started_at.isoformat() if record.started_at else None
    completed_str = record.completed_at.isoformat() if record.completed_at else None
    total = max(record.total_bytes, record.bytes_downloaded, 0)
    if record.status in (
        DownloadLifecycleStatus.COMPLETED,
        DownloadLifecycleStatus.DEDUPLICATED,
    ):
        pct = 100.0
    elif total > 0:
        pct = round((record.bytes_downloaded / total) * 100.0, 1)
    else:
        pct = 0.0

    return DownloadStatusItemResponse(
        download_id=record.download_id,
        source_id=record.source_id,
        canonical_url=record.canonical_url,
        status=record.status.value,
        bytes_downloaded=record.bytes_downloaded,
        total_bytes=record.total_bytes,
        progress_pct=pct,
        speed_bps=record.speed_bps,
        retry_count=record.retry_count,
        max_retries=record.max_retries,
        last_error_code=record.last_error_code,
        sha256_checksum=record.sha256_checksum,
        local_path=record.local_path,
        task_id=record.task_id,
        worker_id=record.worker_id,
        resource_key=record.resource_key,
        priority=record.priority,
        subscribers_count=len(record.subscribers),
        coalesced=coalesced,
        started_at=started_str,
        completed_at=completed_str,
    )


@router.get(
    "/status",
    response_model=DownloadsStatusListResponse,
    summary="Monitoreo en tiempo real del estado de descargas y cola",
)
def get_downloads_status(
    source_id: str | None = Query(default=None),
    include_recent: bool = Query(default=False),
) -> DownloadsStatusListResponse:
    """Consulta las transferencias físicas en cola/ejecución y su historial reciente."""
    if include_recent and hasattr(_tracking_adapter, "list_recent_downloads"):
        records = _tracking_adapter.list_recent_downloads(
            limit=50,
            source_id=source_id,
        )
        active_cnt = sum(1 for r in records if r.status in ACTIVE_QUEUE_STATUSES)
        items = [_to_item_response(d) for d in records]
        return DownloadsStatusListResponse(
            total_active=active_cnt,
            downloads=items,
        )

    active = _tracking_adapter.list_active_downloads()
    if source_id:
        active = [d for d in active if d.source_id.upper() == source_id.upper()]

    items = [_to_item_response(d) for d in active]
    return DownloadsStatusListResponse(
        total_active=len(items),
        downloads=items,
    )


@router.post(
    "/queue",
    response_model=DownloadStatusItemResponse,
    summary="Encolar descarga física real con coalescing concurrente y ejecución por workers",
)
def enqueue_download(request: EnqueueDownloadRequest) -> DownloadStatusItemResponse:
    """Encola una descarga real en `download_tracking` y despierta al pool de workers."""
    raw_status = request.status.strip().upper()
    if raw_status == "DOWNLOADING":
        init_status = DownloadLifecycleStatus.DOWNLOADING
    elif raw_status == "PENDING":
        init_status = DownloadLifecycleStatus.PENDING
    else:
        init_status = DownloadLifecycleStatus.QUEUED

    rec, coalesced = _worker_pool.enqueue(
        source_id=request.source_id,
        canonical_url=request.canonical_url,
        request_id=request.request_id,
        task_id=request.task_id,
        priority=request.priority,
        resource_key=request.resource_key,
        dataset_code=request.dataset_code,
        period_label=request.period_label,
        etag=request.etag,
        initial_status=init_status,
        auto_wake=request.execute_now,
    )
    return _to_item_response(rec, coalesced=coalesced)


@router.post(
    "/{download_id}/retry",
    response_model=DownloadStatusItemResponse,
    summary="Reencolar una descarga fallida o cancelada",
)
def retry_download(download_id: str) -> DownloadStatusItemResponse:
    """Reencola la transferencia en estado QUEUED incrementando retry_count."""
    rec = _worker_pool.retry_download(download_id)
    if rec is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Descarga '{download_id}' no encontrada.",
        )
    return _to_item_response(rec)


@router.post(
    "/{download_id}/cancel",
    response_model=DownloadStatusItemResponse,
    summary="Cancelar limpiamente una descarga en cola o en ejecución",
)
def cancel_download(download_id: str) -> DownloadStatusItemResponse:
    """Detiene la transferencia cooperativa y la marca como CANCELLED."""
    rec = _worker_pool.cancel_download(download_id)
    if rec is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Descarga '{download_id}' no encontrada.",
        )
    return _to_item_response(rec)


@router.get(
    "/circuit-breakers",
    summary="Estado en vivo del disyuntor de red por dominio (CLOSED/OPEN/HALF_OPEN)",
)
def list_circuit_breakers() -> dict[str, Any]:
    """Expone el estado del disyuntor de red para observabilidad en consola."""
    return {"domains": _worker_pool.get_circuit_statuses()}


@router.get(
    "/stream",
    summary="Stream SSE en vivo de progreso, verificación SHA-256 y cierre de descargas",
)
async def stream_downloads(
    snapshot_only: bool = Query(
        default=False,
        description="Si es True, emite el snapshot actual y cierra el stream (útil para smoke checks).",
    ),
) -> StreamingResponse:
    """Emite eventos Server-Sent Events (`text/event-stream`) de la cola de descargas."""

    async def event_generator() -> AsyncIterator[str]:
        active = (
            _tracking_adapter.list_recent_downloads(limit=40)
            if hasattr(_tracking_adapter, "list_recent_downloads")
            else _tracking_adapter.list_active_downloads()
        )
        snapshot_payload = {
            "event": "queue_snapshot",
            "total_active": sum(1 for r in active if r.status in ACTIVE_QUEUE_STATUSES),
            "downloads": [_to_item_response(r).model_dump(mode="json") for r in active],
        }
        yield f"event: queue_snapshot\ndata: {json.dumps(snapshot_payload, ensure_ascii=False)}\n\n"
        if snapshot_only:
            return

        last_seq = _worker_pool.events.latest_seq
        ticks = 0
        while True:
            new_events = _worker_pool.events.since(last_seq)
            if new_events:
                ticks = 0
                for ev in new_events:
                    last_seq = ev["seq"]
                    ev_name = ev.get("event", "message")
                    yield f"event: {ev_name}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"
            else:
                ticks += 1
                if ticks >= 25:
                    ticks = 0
                    yield ": keep-alive\n\n"
                await asyncio.sleep(0.2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/tracking/{download_id}",
    response_model=DownloadStatusItemResponse,
    summary="Consultar detalle y suscriptores de una descarga por ID",
)
def get_download_tracking_detail(download_id: str) -> DownloadStatusItemResponse:
    """Retorna estado de una descarga específica y cuántas peticiones esperan."""
    record = _tracking_adapter.get_download(download_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Descarga con ID '{download_id}' no encontrada en download_tracking."
            ),
        )
    return _to_item_response(record)
