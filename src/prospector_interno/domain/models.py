"""Modelos canónicos del dominio para Prospector Interno (Target: Portal Nexus).

Todos los modelos de datos son inmutables para garantizar consistencia y
prevenir efectos secundarios en procesamiento concurrente.
"""

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


GatekeeperMode = Literal["DRY_RUN", "ACQUIRE_LOCK"]


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
    IN_PROGRESS = "IN_PROGRESS"


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
    checksum_sha256: str | None = None
    etag: str | None = None
    force_refresh: bool = False
    mode: GatekeeperMode = "ACQUIRE_LOCK"
    lock_ttl_seconds: int = Field(default=300, ge=1, le=3600)
    requested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def idempotency_key(self) -> str:
        """Clave única para detectar duplicados concurrentes e históricos."""
        if self.period and self.period != "LATEST":
            return f"{self.domain}:{self.dataset_code}:{self.period}:{self.target_url}"
        return f"{self.domain}:{self.dataset_code}:{self.target_url}"


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
    derivation_query: str | None = None
    missing_periods: list[str] = Field(default_factory=list)
    subscribers_count: int = 0
    action_links: dict[str, str | None] = Field(default_factory=dict)
    latency_ms: float = 0.0
    dedup_criterion: str | None = None


class ResourceChangeCategory(str, Enum):

    """Categoría de discrepancia entre mapa externo y almacenamiento local."""

    CONFIRMED = "CONFIRMED"
    NEW = "NEW"
    MODIFIED = "MODIFIED"
    URL_CHANGED = "URL_CHANGED"


class DiscoveredResourceItem(BaseModel):
    """Recurso descubierto proyectado desde el JSON del Prospector Externo."""

    model_config = ConfigDict(frozen=True)

    resource_key: str
    url: str
    source_id: str
    title: str
    file_extension: str
    content_type: str | None = None
    content_length_bytes: int | None = None
    last_modified_header: str | None = None
    etag: str | None = None
    period_label: str | None = None
    content_hash: str | None = None
    change_status: str = "UNCHANGED"


class ReconciliationDiffReportItem(BaseModel):
    """Resultado del diff para un recurso específico."""

    model_config = ConfigDict(frozen=True)

    resource_key: str
    url: str
    source_id: str
    category: ResourceChangeCategory
    file_extension: str
    period_label: str | None = None
    size_bytes: int | None = None
    detail: str


class ReconciliationDiffReport(BaseModel):
    """Reporte formal consolidado de diferencias (HUI-18)."""

    model_config = ConfigDict(frozen=True)

    source_id: str
    run_id: str
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    total_external_resources: int
    total_confirmed: int
    total_new: int
    total_modified: int
    items: list[ReconciliationDiffReportItem]


class SyncPriority(str, Enum):
    """Nivel de prioridad de adquisición en el SyncPlan."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class SyncPlanItem(BaseModel):
    """Tarea individual de sincronización priorizada dentro del SyncPlan."""

    model_config = ConfigDict(frozen=True)

    item_id: str
    resource_key: str
    url: str
    source_id: str
    domain: str
    period: str
    priority: SyncPriority
    expected_hash: str | None = None
    status: str = "PENDING"
    downloaded_artifact: BronzeArtifact | None = None
    error_message: str | None = None


class SyncPlan(BaseModel):
    """Plan de sincronización priorizado para adquisición masiva (HUI-19)."""

    model_config = ConfigDict(frozen=True)

    plan_id: str
    source_id: str
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    total_tasks: int
    items: list[SyncPlanItem]
    completed_tasks: int = 0
    failed_tasks: int = 0
    is_executed: bool = False
