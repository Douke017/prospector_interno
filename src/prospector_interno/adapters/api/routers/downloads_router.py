"""Router de Monitoreo del Ciclo de Ingesta y Estado de Descargas (HUI-03).

Implementa los endpoints de la Sección 13.1 del documento de arquitectura:
- GET /downloads/status: Retorna estado de ingesta y progreso de colas.
- GET /downloads/tracking/{download_id}: Detalle de un recurso y suscriptores.
"""

from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, status

from prospector_interno.adapters.api.schemas import (
    DownloadStatusItemResponse,
    DownloadsStatusListResponse,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadRecord,
)
from prospector_interno.domain.ports import DownloadTrackingPort

router = APIRouter(prefix="/downloads", tags=["5. Monitoreo de Ingesta & Descargas"])

_tracking_adapter: DownloadTrackingPort = SqliteDownloadTrackingAdapter(
    db_path=Path("data/catalog/datax_catalog.db")
)


def set_download_tracking(adapter: DownloadTrackingPort) -> None:
    """Permite inyectar un adaptador de tracking aislado para pruebas."""
    global _tracking_adapter
    _tracking_adapter = adapter


def get_download_tracking() -> DownloadTrackingPort:
    """Retorna el adaptador activo para inspección en pruebas."""
    return _tracking_adapter


def _to_item_response(record: DownloadRecord) -> DownloadStatusItemResponse:
    """Convierte un DownloadRecord de dominio a DTO de presentación."""
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
    summary="Monitoreo en tiempo real del estado de descargas activas (HUI-03)",
    description=(
        "Retorna el estado de todas las descargas activas ('DOWNLOADING') o en cola "
        "('PENDING'), con soporte para filtro por entidad emisora (source_id)."
    ),
)
def get_downloads_status(
    source_id: str | None = Query(
        default=None,
        description="Filtro opcional por código de fuente (ej. 'BO_BCB')",
    ),
) -> DownloadsStatusListResponse:
    """Consulta las transferencias físicas en vuelo y su progreso."""
    active = _tracking_adapter.list_active_downloads()
    if source_id:
        active = [d for d in active if d.source_id == source_id]

    items = [_to_item_response(d) for d in active]
    return DownloadsStatusListResponse(
        total_active=len(items),
        downloads=items,
    )


@router.get(
    "/tracking/{download_id}",
    response_model=DownloadStatusItemResponse,
    summary="Consultar detalle y suscriptores de una descarga por ID (HUI-03)",
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
