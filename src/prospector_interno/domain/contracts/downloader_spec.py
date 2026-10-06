"""Contratos y especificaciones para el Downloader Resiliente y Disyuntor.

Cubre HUI-08 (Streaming Reanudable) y HUI-09 (Domain Circuit Breaker).
"""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class CircuitState(str, Enum):
    """Estados del Disyuntor Adaptativo (Circuit Breaker)."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class DomainCircuitStatus(BaseModel):
    """Metadatos del estado del disyuntor para un dominio específico."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    domain: str = Field(..., description="Nombre del host o dominio supervisado")
    state: CircuitState = Field(
        default=CircuitState.CLOSED,
        description="Estado operacional del circuito",
    )
    failure_count: int = Field(
        default=0,
        description="Cantidad de fallos 429/503 consecutivos registrados",
    )
    cooldown_seconds: float = Field(
        default=0.0,
        description="Segundos restantes de enfriamiento",
    )
    last_status_code: int | None = Field(
        default=None,
        description="Último código HTTP de error (429 o 503)",
    )
    retry_after_until: float | None = Field(
        default=None,
        description="Timestamp UNIX hasta el cual el circuito permanece OPEN",
    )


class ResumableDownloadResult(BaseModel):
    """Resultado estructurado de una descarga streaming reanudable."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str = Field(..., description="URL de origen del recurso descargado")
    domain: str = Field(..., description="Dominio emisor")
    file_path: str = Field(..., description="Ruta final del archivo descargado")
    bytes_downloaded: int = Field(
        ...,
        description="Total de bytes transferidos en esta sesión",
    )
    total_file_size: int = Field(
        ...,
        description="Tamaño total del archivo en disco",
    )
    was_resumed: bool = Field(
        default=False,
        description="Indica si la descarga fue reanudada desde un archivo parcial",
    )
    sha256_hash: str = Field(
        ...,
        description="Hash criptográfico SHA-256 verificado en vuelo",
    )
    content_type: str | None = Field(
        default=None,
        description="Tipo MIME devuelto por el servidor",
    )
    elapsed_seconds: float = Field(
        ...,
        description="Tiempo transcurrido en la descarga",
    )
