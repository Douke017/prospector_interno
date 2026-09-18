from pydantic import BaseModel, ConfigDict, Field

from prospector_interno.domain.models import PreFlightVerdict


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

    model_config = ConfigDict(frozen=True)

    dataset_code: str = Field(..., examples=["DS_BO_BCB_INFLATION"])
    domain: str = Field(..., examples=["bcb.gob.bo"])
    target_url: str = Field(
        ...,
        examples=["https://www.bcb.gob.bo/datos/inflacion.xlsx"],
    )
    period: str = Field(..., examples=["2026-08"])
    force_refresh: bool = Field(default=False)
    request_id: str | None = Field(default=None)


class IngestionEvaluateResponse(BaseModel):
    """Respuesta del Pre-flight Gatekeeper con el veredicto de adquisición."""

    model_config = ConfigDict(frozen=True)

    verdict: PreFlightVerdict
    idempotency_key: str
    reason: str
    lock_acquired: bool
    cached_artifact_id: str | None = None
    in_flight_task_id: str | None = None


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
