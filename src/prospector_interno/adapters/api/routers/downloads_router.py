"""Router de Monitoreo del Ciclo de Ingesta y Estado de Descargas."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.schemas import (
    DownloadStatusItemResponse,
    DownloadsStatusListResponse,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.ports import DownloadTrackingPort

router = APIRouter(prefix="/downloads", tags=["5. Monitoreo de Ingesta & Descargas"])

_tracking_adapter: DownloadTrackingPort = deps.download_tracking


class EnqueueDownloadRequest(BaseModel):
    """Solicitud para registrar un archivo fuente en cola de descarga."""

    source_id: str = Field(..., examples=["BCB", "ASFI", "BBV"])
    canonical_url: str = Field(..., examples=["https://www.bcb.gob.bo/boletin_2026_09.xlsx"])
    status: str = Field(default="PENDING", description="PENDING o DOWNLOADING")
    task_id: str | None = Field(default=None)


def set_download_tracking(adapter: DownloadTrackingPort) -> None:
    """Permite inyectar un adaptador de tracking aislado para pruebas."""
    global _tracking_adapter
    _tracking_adapter = adapter


def get_download_tracking() -> DownloadTrackingPort:
    """Retorna el adaptador activo."""
    return _tracking_adapter


def _to_item_response(record: DownloadRecord) -> DownloadStatusItemResponse:
    started_str = record.started_at.isoformat() if record.started_at else None
    completed_str = record.completed_at.isoformat() if record.completed_at else None

    return DownloadStatusItemResponse(
        download_id=record.download_id,
        source_id=record.source_id,
        canonical_url=record.canonical_url,
        status=record.status.value,
        bytes_downloaded=record.bytes_downloaded,
        total_bytes=record.total_bytes,
        task_id=record.task_id,
        subscribers_count=len(record.subscribers),
        started_at=started_str,
        completed_at=completed_str,
    )


@router.get(
    "/status",
    response_model=DownloadsStatusListResponse,
    summary="Monitoreo en tiempo real del estado de descargas y cola",
)
def get_downloads_status(
    source_id: str | None = Query(
        default=None,
        description="Filtro opcional por código de fuente (ej. 'BO_BCB')",
    ),
    include_recent: bool = Query(
        default=False,
        description="Si es True, incluye también las transferencias completadas recientemente.",
    ),
) -> DownloadsStatusListResponse:
    """Consulta las transferencias físicas en cola/ejecución y su historial reciente."""
    if include_recent and hasattr(_tracking_adapter, "list_recent_downloads"):
        records = _tracking_adapter.list_recent_downloads(
            limit=40,
            source_id=source_id,
        )
        active_cnt = sum(
            1
            for r in records
            if r.status in (DownloadLifecycleStatus.PENDING, DownloadLifecycleStatus.DOWNLOADING)
        )
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
    summary="Registrar archivo fuente en cola de descarga controlada",
)
def enqueue_download(request: EnqueueDownloadRequest) -> DownloadStatusItemResponse:
    """Registra una URL en cola o en curso para trazabilidad y control de concurrencia."""
    raw_status = request.status.strip().upper()
    if raw_status not in {"PENDING", "DOWNLOADING"}:
        raw_status = "PENDING"
    lifecycle_status = DownloadLifecycleStatus(raw_status)
    dl_id = f"dl_{uuid.uuid4().hex[:10]}"
    t_id = request.task_id or f"cola_{request.source_id.lower()}_{uuid.uuid4().hex[:6]}"
    rec = DownloadRecord(
        download_id=dl_id,
        source_id=request.source_id.strip().upper(),
        canonical_url=request.canonical_url.strip(),
        status=lifecycle_status,
        task_id=t_id,
        started_at=datetime.now(timezone.utc),
    )
    _tracking_adapter.record_download(rec)
    return _to_item_response(rec)


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
