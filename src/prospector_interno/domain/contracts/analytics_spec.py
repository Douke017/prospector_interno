"""Especificación de contrato para la API de Consultas Analíticas (FastAPI)."""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class AnalyticsQueryRequest(BaseModel):
    """Petición de consulta analítica de autoservicio sobre la Capa Gold."""

    model_config = ConfigDict(frozen=True)

    datamart: str = Field(
        default="mart_cartera",
        description="Nombre del datamart en Capa Gold a consultar",
    )
    institutions: list[str] | None = Field(
        default=None,
        examples=[["CRECER", "PRO_MUJER"]],
        description="Filtro opcional de instituciones",
    )
    gestiones: list[int] | None = Field(
        default=None,
        examples=[[2025, 2026]],
        description="Años a filtrar en particiones de Gold",
    )
    meses: list[int] | None = Field(
        default=None,
        examples=[[1, 2, 3]],
        description="Meses a filtrar en particiones de Gold (1 a 12)",
    )
    metrics: list[str] | None = Field(
        default=None,
        examples=[["institucion_id", "cartera_bruta", "ratio_mora"]],
        description="Columnas o métricas a proyectar en el resultado",
    )


class AnalyticsQueryResponse(BaseModel):
    """Respuesta estructurada para consultas analíticas sobre Capa Gold."""

    model_config = ConfigDict(frozen=True)

    datamart: str
    total_records: int
    execution_time_ms: float
    columns: list[str]
    records: list[dict[str, Any]]
