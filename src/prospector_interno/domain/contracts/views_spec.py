"""Contratos y especificaciones para la Biblioteca de Vistas Analíticas (HUI-22)."""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class PredefinedViewMetadata(BaseModel):
    """Metadatos descriptivos de una vista analítica predefinida."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    view_id: str = Field(
        ...,
        description="Identificador único legible de la vista (kebab-case)",
        examples=["evolucion-mora-temporal", "ranking-solvencia-cobertura"],
    )
    title: str = Field(
        ...,
        description="Título de negocio formal de la vista",
        examples=["Evolución Temporal de Cartera y Mora"],
    )
    description: str = Field(
        ...,
        description="Propósito analítico y regla de agregación",
    )
    category: str = Field(
        ...,
        description="Categoría temática de la consulta (RIESGO, SOLVENCIA, BENCHMARK)",
        examples=["RIESGO", "SOLVENCIA"],
    )
    supported_filters: list[str] = Field(
        default_factory=list,
        description="Parámetros de filtrado admitidos por la vista",
        examples=[["gestion", "mes", "institutions", "limit"]],
    )
    output_columns: list[str] = Field(
        default_factory=list,
        description="Lista de columnas proyectadas en el resultado",
    )


class PredefinedViewQueryRequest(BaseModel):
    """Parámetros de invocación parametrizada para una vista precalculada."""

    model_config = ConfigDict(frozen=True)

    gestion: int | None = Field(
        default=None,
        description="Año de corte para filtrar la vista",
        examples=[2026],
    )
    mes: int | None = Field(
        default=None,
        description="Mes de corte para filtrar la vista (1 a 12)",
        examples=[1],
    )
    institutions: list[str] | None = Field(
        default=None,
        description="Lista opcional de instituciones a incluir en el análisis",
        examples=[["CRECER", "PRO_MUJER"]],
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Número máximo de registros a retornar",
    )


class PredefinedViewResult(BaseModel):
    """Resultado estructurado de la ejecución de una vista precalculada."""

    model_config = ConfigDict(frozen=True)

    view_id: str
    title: str
    execution_time_ms: float
    total_records: int
    columns: list[str]
    records: list[dict[str, Any]]
