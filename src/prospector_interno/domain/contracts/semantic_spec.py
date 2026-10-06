"""Contratos y especificaciones para el Diccionario Semántico (HUI-16)."""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class MetricDataType(str, Enum):
    """Tipo de dato de la métrica financiera."""

    DECIMAL = "DECIMAL"
    PERCENTAGE = "PERCENTAGE"
    INTEGER = "INTEGER"
    STRING = "STRING"


class FinancialMetricDefinition(BaseModel):
    """Definición formal e inmutable de una métrica financiera."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    metric_code: str = Field(
        ...,
        description="Código canónico único de la métrica en minúsculas snake_case",
        examples=["ratio_mora", "cartera_bruta"],
    )
    canonical_name: str = Field(
        ...,
        description="Nombre formal y legible de la métrica",
        examples=["Ratio de Mora", "Cartera Bruta Total"],
    )
    category: str = Field(
        ...,
        description="Categoría analítica (CARTERA, BALANCE, SOLVENCIA, RENTABILIDAD)",
        examples=["CARTERA", "SOLVENCIA"],
    )
    data_type: MetricDataType = Field(
        default=MetricDataType.DECIMAL,
        description="Tipo de dato canónico del indicador",
    )
    unit: str = Field(
        ...,
        description="Unidad de medida (BOB, Porcentaje, Ratio)",
        examples=["Bolivianos (BOB)", "Porcentaje (%)"],
    )
    formula_latex: str = Field(
        ...,
        description="Fórmula matemática canónica formalizada en LaTeX",
        examples=[r"\frac{\text{cartera\_en\_mora}}{\text{cartera\_bruta}} \times 100"],
    )
    description: str = Field(
        ...,
        description="Definición conceptual y regla de negocio según la arquitectura",
    )
    required_fields: list[str] = Field(
        default_factory=list,
        description="Campos fuente necesarios para computar la métrica",
        examples=[["cartera_en_mora", "cartera_bruta"]],
    )


class SemanticDictionaryResponse(BaseModel):
    """Respuesta estructurada para consultas del catálogo de métricas."""

    model_config = ConfigDict(frozen=True)

    total_metrics: int
    categories: list[str]
    metrics: list[FinancialMetricDefinition]
