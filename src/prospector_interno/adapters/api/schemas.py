from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from prospector_interno.domain.models import (
    GatekeeperMode,
    PreFlightVerdict,
    ReconciliationDiffReportItem,
)


class SourceResponse(BaseModel):
    """Información oficial de una institución emisora en Portal Nexus."""

    model_config = ConfigDict(frozen=True)

    source_id: int = Field(..., description="Identificador único numérico")
    canonical_code: str = Field(..., examples=["BO_BCB"], description="Código único")
    official_name: str = Field(..., examples=["Banco Central de Bolivia"])
    short_name: str = Field(..., examples=["BCB"])
    homepage: str | None = Field(default=None, examples=["https://www.bcb.gob.bo/"])
    country_iso: str = Field(default="BOL", examples=["BOL"])
    is_active: bool = True
    total_active_datasets: int = Field(
        default=0,
        description="Número de datasets activos disponibles",
    )


class DatasetResponse(BaseModel):
    """Especificación y estado de disponibilidad de un conjunto de datos."""

    model_config = ConfigDict(frozen=True)

    canonical_dataset_code: str = Field(
        ...,
        examples=["DS_BO_BCB_INFLATION"],
        description="Código formal único de la serie",
    )
    source_code: str = Field(..., examples=["BCB"])
    source_name: str = Field(..., examples=["Banco Central de Bolivia"])
    dataset_name: str = Field(..., examples=["Inflación Mensual General"])
    publication_frequency: str = Field(..., examples=["Mensual"])
    target_url: str | None = Field(default=None)
    format_type: str | None = Field(default="xlsx")
    last_download_date: str | None = Field(default=None, examples=["2026-06-02"])
    last_known_hash: str | None = Field(default=None)
    total_historical_downloads: int = Field(default=0)


class CatalogStatsResponse(BaseModel):
    """Métricas consolidadas del catálogo activo de Portal Nexus."""

    model_config = ConfigDict(frozen=True)

    total_active_sources: int = Field(..., examples=[31])
    total_canonical_datasets: int = Field(..., examples=[585])
    total_historical_downloads: int = Field(..., examples=[28852])
    view_status: str = Field(default="MATERIALIZED_IN_POSTGRESQL")
    query_engine: str = Field(default="DuckDB + PostgreSQL MV")


class IngestionEvaluateRequest(BaseModel):
    """Solicitud enviada por Portal Nexus para verificar disponibilidad."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    dataset_code: str = Field(
        ...,
        alias="dataset_id",
        examples=["DS_BO_BCB_INFLATION"],
        description="Código o identificador canónico del dataset",
    )
    domain: str | None = Field(
        default=None,
        examples=["bcb.gob.bo"],
        description="Dominio del portal emisor",
    )
    target_url: str | None = Field(
        default=None,
        examples=["https://www.bcb.gob.bo/datos/inflacion.xlsx"],
        description="URL directa hacia el recurso",
    )
    period: str = Field(
        ...,
        alias="temporal_period",
        examples=["2026-08"],
        description="Período temporal del recurso (ej: 2026-05)",
    )
    source_id: str | None = Field(
        default=None,
        examples=["ine"],
        description="Identificador opcional de la institución emisor",
    )
    checksum_sha256: str | None = Field(
        default=None,
        description="Huella SHA-256 opcional para validación directa antiduplicidad",
    )
    etag: str | None = Field(
        default=None,
        description="Cabecera ETag opcional para validación pre-descarga",
    )
    force_refresh: bool = Field(default=False)
    mode: GatekeeperMode = Field(
        default="DRY_RUN",
        description="DRY_RUN (defecto seguro sin lock) o ACQUIRE_LOCK (reserva TTL)",
    )
    dry_run: bool | None = Field(
        default=None,
        description="Alias booleano opcional: true=DRY_RUN, false=ACQUIRE_LOCK",
    )
    lock_ttl_seconds: int = Field(default=300, ge=1, le=3600)
    request_id: str | None = Field(default=None)

    def resolved_mode(self) -> GatekeeperMode:
        if self.dry_run is True:
            return "DRY_RUN"
        if self.dry_run is False:
            return "ACQUIRE_LOCK"
        return self.mode


class IngestionEvaluateResponse(BaseModel):
    """Respuesta del Pre-flight Gatekeeper con el veredicto de adquisición."""

    model_config = ConfigDict(frozen=True)

    verdict: PreFlightVerdict
    idempotency_key: str
    reason: str
    lock_acquired: bool
    mode: GatekeeperMode = "DRY_RUN"
    cached_artifact_id: str | None = None
    in_flight_task_id: str | None = None
    task_id: str | None = None
    status: str | None = None
    subscribers_count: int = 0
    derivation_query: str | None = None
    missing_periods: list[str] = Field(default_factory=list)
    action_links: dict[str, str | None] = Field(default_factory=dict)
    latency_ms: float = 0.0
    dedup_criterion: str | None = None


class BatchGatekeeperEvaluateRequest(BaseModel):
    """Evaluación masiva de hasta 500 solicitudes en el Pre-flight Gatekeeper."""

    model_config = ConfigDict(frozen=True)

    items: list[IngestionEvaluateRequest] = Field(..., min_length=1, max_length=500)
    mode: GatekeeperMode = Field(default="DRY_RUN")
    lock_ttl_seconds: int = Field(default=300, ge=1, le=3600)


class BatchGatekeeperEvaluateResponse(BaseModel):
    """Resultado consolidado de la evaluación por lotes."""

    model_config = ConfigDict(frozen=True)

    total: int
    mode: GatekeeperMode
    elapsed_ms: float
    summary: dict[str, int]
    saved_downloads: int
    results: list[IngestionEvaluateResponse]


class DownloadStatusItemResponse(BaseModel):
    """Estado resumido de una descarga física para monitoreo en tiempo real."""

    model_config = ConfigDict(frozen=True)

    download_id: str
    source_id: str
    canonical_url: str
    status: str
    bytes_downloaded: int = 0
    total_bytes: int = 0
    progress_pct: float = 0.0
    speed_bps: float = 0.0
    retry_count: int = 0
    max_retries: int = 3
    last_error_code: str | None = None
    sha256_checksum: str | None = None
    local_path: str | None = None
    task_id: str | None = None
    worker_id: str | None = None
    resource_key: str | None = None
    priority: int = 5
    subscribers_count: int = 0
    coalesced: bool = False
    started_at: str | None = None
    completed_at: str | None = None


class DownloadsStatusListResponse(BaseModel):
    """Listado consolidado de descargas activas (Sección 13.1)."""

    model_config = ConfigDict(frozen=True)

    total_active: int
    downloads: list[DownloadStatusItemResponse]


class ReconciliationDiffRequest(BaseModel):

    """Petición de cruce entre catálogo interno y mapa del Prospector Externo."""

    model_config = ConfigDict(frozen=True)

    source_code: str = Field(..., examples=["BCB"])
    map_metadata_json_path: str = Field(
        ...,
        examples=["prospector_externo/output/BCB/map_metadata.json"],
        description="Ruta al archivo JSON de metadata del Prospector Externo",
    )
    search_pattern: str | None = Field(
        default=None,
        examples=["%inflacion%"],
        description="Filtro opcional para buscar URLs específicas",
    )


class ReconciliationDiffItem(BaseModel):
    """Resultado del diff para una URL individual analizada con DuckDB."""

    model_config = ConfigDict(frozen=True)

    url: str
    content_length: int | None = None
    last_modified: str | None = None
    file_extension: str | None = None
    status: str = Field(
        ...,
        examples=["VERIFIED_IN_MAP"],
        description="Estado: VERIFIED_IN_MAP, UNKNOWN_IN_EXTERNAL, URL_CHANGED",
    )


class ReconciliationDiffResponse(BaseModel):
    """Respuesta del motor DuckDB tras comparar el inventario y los JSONs."""

    model_config = ConfigDict(frozen=True)

    source_code: str
    matches_count: int
    duration_ms: float
    items: list[ReconciliationDiffItem]


class BridgeReconcileRequest(BaseModel):
    """Petición formal para ejecutar el diff de conciliación (HUI-18)."""

    model_config = ConfigDict(frozen=True)

    source_id: str = Field(..., examples=["bbv", "finrural"])
    external_api_url: str | None = Field(
        default=None,
        examples=["http://localhost:8000"],
        description=(
            "URL base de la Catalog API Facade del Prospector Externo. "
            "Si se omite, se utiliza el valor configurado en PROSPECTOR_EXTERNO_URL."
        ),
    )
    external_map_path: str | None = Field(
        default=None,
        examples=["prospector_externo/output/finrural/mapa_finrural.json"],
        description=(
            "Ruta opcional al archivo local para pruebas offline o "
            "mapas CSV canónicos. Si no se especifica, la conciliación "
            "se realiza vía HTTP REST."
        ),
    )


class BridgeReconcileResponse(BaseModel):
    """Resultado formal del diff categorizado por el Bridge (HUI-18)."""

    model_config = ConfigDict(frozen=True)

    source_id: str
    run_id: str
    total_external_resources: int
    total_confirmed: int
    total_new: int
    total_modified: int
    summary_message: str
    items: list[ReconciliationDiffReportItem] = Field(default_factory=list)


class BridgeSyncRequest(BaseModel):
    """Petición para generar y ejecutar el SyncPlan hacia Bronze (HUI-19)."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    source_id: str = Field(..., examples=["bbv", "finrural"])
    external_api_url: str | None = Field(
        default=None,
        examples=["http://localhost:8000"],
        description="URL base de la Catalog API Facade del Prospector Externo.",
    )
    external_map_path: str | None = Field(
        default=None,
        examples=["prospector_externo/output/finrural/mapa_finrural.json"],
        description="Ruta opcional para pruebas offline o mapas CSV canónicos.",
    )
    max_downloads: int = Field(
        default=10,
        ge=1,
        le=1000,
        alias="batch_size",
        description="Límite máximo de descargas a procesar en esta tanda",
    )
    execute_downloads: bool = Field(
        default=True,
        description=(
            "Si es True, ejecuta la descarga hacia Bronze. "
            "Si es False, sólo genera y previsualiza el SyncPlan (dry-run)."
        ),
    )


class BridgeSyncResponse(BaseModel):
    """Resultado de la ejecución del SyncPlan hacia Capa Bronze (HUI-19)."""

    model_config = ConfigDict(frozen=True)

    plan_id: str
    source_id: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    is_executed: bool
    summary: str
