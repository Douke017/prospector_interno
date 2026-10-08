"""Contratos y especificaciones para el Downloader Resiliente y Disyuntor.

Cubre HUI-08 (Streaming Reanudable) y HUI-09 (Domain Circuit Breaker).
"""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


REALISTIC_BROWSER_HEADERS: dict[str, str] = {

    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


class BrowserFingerprintProfile(BaseModel):
    """Perfil formal de huella HTTP de navegador real y pool de red (HUI-07)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    profile_name: str = Field(
        default="chrome_windows_128",
        description="Nombre identificador del perfil de huella HTTP",
    )
    user_agent: str = Field(
        default=REALISTIC_BROWSER_HEADERS["User-Agent"],
        description="Encabezado User-Agent del navegador emulado",
    )
    headers: dict[str, str] = Field(
        default_factory=lambda: dict(REALISTIC_BROWSER_HEADERS),
        description="Diccionario de cabeceras canónicas completas",
    )
    http2_enabled: bool = Field(
        default=True,
        description="Bandera que indica si HTTP/2 multiplexado está activo",
    )
    keep_alive_enabled: bool = Field(
        default=True,
        description="Bandera que indica si conexiones persistentes están activas",
    )
    max_connections: int = Field(
        default=20,
        description="Límite máximo de conexiones simultáneas en el pool",
    )
    max_keepalive_connections: int = Field(
        default=10,
        description="Límite máximo de conexiones en estado Keep-Alive",
    )
    keepalive_expiry_seconds: float = Field(
        default=30.0,
        description="Tiempo de vida en segundos para conexiones inactivas",
    )


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


class BatchExecutionVerdict(str, Enum):
    """Veredicto de admisibilidad para ejecución de lotes de descarga (HUI-10)."""

    PERMITTED_IMMEDIATE = "PERMITTED_IMMEDIATE"
    PAUSED_WAITING_OFFPEAK = "PAUSED_WAITING_OFFPEAK"
    FORCED_OVERRIDE = "FORCED_OVERRIDE"


class OffPeakEvaluationRequest(BaseModel):
    """Solicitud de evaluación horaria para ejecución de lotes masivos."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    batch_size: int = Field(
        ...,
        ge=1,
        description="Cantidad total de archivos en el lote de sincronización",
    )
    force_now: bool = Field(
        default=False,
        description="Bandera de anulación manual (--force-now) para descargas urgentes",
    )
    current_time: datetime | None = Field(
        default=None,
        description="Timestamp de referencia (opcional, para testing determinista)",
    )
    source_id: str | None = Field(
        default=None,
        description="Identificador de la fuente financiera asociada",
    )


class OffPeakEvaluationResult(BaseModel):
    """Resultado formal de la evaluación de ventana horaria valle (HUI-10)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    verdict: BatchExecutionVerdict = Field(
        ...,
        description="Veredicto final sobre la ejecución del lote",
    )
    is_permitted: bool = Field(
        ...,
        description="Indica si la descarga física puede proceder de inmediato",
    )
    reason: str = Field(
        ...,
        description="Explicación detallada de la regla horaria aplicada",
    )
    current_hour_bolivia: int = Field(
        ...,
        ge=0,
        le=23,
        description="Hora actual en Bolivia (0 a 23)",
    )
    next_window_opens_at: datetime | None = Field(
        default=None,
        description="Timestamp exacto de apertura de la próxima ventana valle (23:00)",
    )
    seconds_until_next_window: float = Field(
        default=0.0,
        ge=0.0,
        description="Tiempo de espera en segundos hasta la próxima ventana valle",
    )
    threshold_batch_size: int = Field(
        default=100,
        description="Umbral de archivos a partir del cual rige la restricción valle",
    )
