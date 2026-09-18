"""Modelos canónicos del dominio para Prospector Interno (Target: Portal Nexus).

Todos los modelos de datos son inmutables para garantizar consistencia y
prevenir efectos secundarios en procesamiento concurrente.
"""

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Periodicity(str, Enum):
    """Periodicidad oficial de publicación del dato."""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    ANNUAL = "ANNUAL"
    ON_DEMAND = "ON_DEMAND"


class PreFlightVerdict(str, Enum):
    """Veredicto emitido por el Pre-flight Gatekeeper antes de descargar."""

    PROCEED_DOWNLOAD = "PROCEED_DOWNLOAD"
    CACHE_HIT_BRONZE = "CACHE_HIT_BRONZE"
    CACHE_HIT_GOLD = "CACHE_HIT_GOLD"
    IN_FLIGHT_LOCKED = "IN_FLIGHT_LOCKED"
    REUSE_DERIVATION = "REUSE_DERIVATION"


class SourceDefinition(BaseModel):
    """Definición canónica de una institución emisora de información."""

    model_config = ConfigDict(frozen=True)

    source_id: int
    canonical_code: str = Field(
        ...,
        description="Código único legible en mayúsculas (ej. 'BO_BCB', 'BO_ASFI')",
    )
    official_name: str
    short_name: str
    homepage: str | None = None
    country_iso: str = Field(default="BOL", max_length=3)
    is_active: bool = True
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    notes: str | None = None


class DatasetSpecification(BaseModel):
    """Especificación técnica de un conjunto de datos o reporte a adquirir."""

    model_config = ConfigDict(frozen=True)

    dataset_code: str = Field(
        ...,
        description="Código formal único de la serie (ej. 'DS_BO_BCB_INFLATION')",
    )
    source_id: int
    legacy_file_code: str | None = Field(
        default=None,
        description="Código D_BO_... del sistema antiguo para compatibilidad",
    )
    name: str
    periodicity: Periodicity = Periodicity.MONTHLY
    url_template: str
    expected_format: str = Field(default="xlsx")
    is_active: bool = True
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class IngestionRequest(BaseModel):
    """Solicitud formal de ingesta o adquisición enviada al Gatekeeper."""

    model_config = ConfigDict(frozen=True)

    request_id: str
    dataset_code: str
    domain: str
    target_url: str
    period: str = Field(
        ...,
        description="Identificador del periodo (ej. '2026-08', '2026-Q2')",
    )
    force_refresh: bool = False
    requested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def idempotency_key(self) -> str:
        """Clave única para detectar duplicados concurrentes e históricos."""
        return f"{self.domain}:{self.dataset_code}:{self.period}"


class BronzeArtifact(BaseModel):
    """Metadatos inmutables de un archivo persistido en Capa Bronze."""

    model_config = ConfigDict(frozen=True)

    artifact_id: str
    dataset_code: str
    domain: str
    file_path: Path
    sha256_hash: str
    size_bytes: int
    downloaded_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    http_status: int = 200
    content_type: str | None = None


class PreFlightEvaluation(BaseModel):
    """Resultado de la evaluación del Gatekeeper antes de cualquier I/O."""

    model_config = ConfigDict(frozen=True)

    verdict: PreFlightVerdict
    idempotency_key: str
    reason: str
    cached_artifact: BronzeArtifact | None = None
    in_flight_task_id: str | None = None
    lock_acquired: bool = False
