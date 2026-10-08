"""Contratos inmutables para el Monitoreo del Ciclo de Ingesta (HUI-03).

Define los estados del ciclo de vida de descarga, el modelo de registro
para la tabla `download_tracking` (Sección 14.1) y los contratos de
evaluación para el Filtro 3 del Pre-Flight Gatekeeper.
"""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class DownloadLifecycleStatus(str, Enum):
    """Estados canónicos de descarga física según Sección 14.1 de la arquitectura."""

    PENDING = "PENDING"
    DOWNLOADING = "DOWNLOADING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    THROTTLED = "THROTTLED"


class DownloadRecord(BaseModel):
    """Registro inmutable de descarga física (tabla download_tracking)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    download_id: str = Field(
        ...,
        description="Identificador único (hash SHA-256 de la URL canónica o UUID)",
    )
    source_id: str = Field(..., description="Identificador de la fuente emisora")
    canonical_url: str = Field(..., description="URL canónica del recurso")
    local_path: str | None = Field(
        default=None,
        description="Ruta local en Capa Bronze donde se almacena el archivo",
    )
    status: DownloadLifecycleStatus = Field(
        ...,
        description="Estado actual del ciclo de transferencia",
    )
    sha256_checksum: str | None = Field(
        default=None,
        description="Firma criptográfica calculada en disco/vuelo",
    )
    bytes_downloaded: int = Field(
        default=0,
        ge=0,
        description="Bytes transferidos hasta el momento",
    )
    total_bytes: int = Field(
        default=0,
        ge=0,
        description="Tamaño total esperado según Content-Length",
    )
    consecutive_failures: int = Field(
        default=0,
        ge=0,
        description="Número de fallos consecutivos registrados",
    )
    last_error_code: str | None = Field(
        default=None,
        description="Código o mensaje del último fallo (ej: HTTP_429, TIMEOUT)",
    )
    task_id: str | None = Field(
        default=None,
        description="Identificador de la tarea en ejecución encargada de la descarga",
    )
    subscribers: list[str] = Field(
        default_factory=list,
        description="Lista de IDs de peticiones concurrentes suscritas a esta tarea",
    )
    started_at: datetime | None = Field(
        default=None,
        description="Marca temporal de inicio de transferencia",
    )
    completed_at: datetime | None = Field(
        default=None,
        description="Marca temporal de finalización de transferencia",
    )


class DownloadStateEvaluationRequest(BaseModel):
    """Petición enviada al Filtro 3 del Gatekeeper para consultar el estado."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    canonical_url: str = Field(..., description="URL del recurso a consultar")
    request_id: str = Field(
        ...,
        description="Identificador único de la petición del cliente o analista",
    )
    source_id: str | None = Field(
        default=None,
        description="Identificador opcional de la institución emisor",
    )


class DownloadStateEvaluationResult(BaseModel):
    """Veredicto emitido por el Filtro 3 del Gatekeeper."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    is_in_progress: bool = Field(
        ...,
        description="True si la descarga ya está activa o en cola",
    )
    status: DownloadLifecycleStatus | None = Field(
        default=None,
        description="Estado actual registrado en el catálogo si existe",
    )
    task_id: str | None = Field(
        default=None,
        description="Identificador de la tarea activa a la que se suscribió",
    )
    download_id: str | None = Field(
        default=None,
        description="Identificador de la descarga en download_tracking",
    )
    subscribers_count: int = Field(
        default=0,
        ge=0,
        description="Cantidad total de peticiones concurrentes suscritas",
    )
    verdict_status: str = Field(
        ...,
        description="Código de veredicto (ej: 'IN_PROGRESS', 'READY_TO_DOWNLOAD')",
    )
    explanation: str = Field(
        ...,
        description="Explicación legible para diagnóstico y auditoría",
    )
