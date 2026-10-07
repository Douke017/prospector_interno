"""Especificación de contratos para Reutilización y Derivación (HUI-02).

Define los contratos inmutables para evaluar relaciones temporales
(mensual -> trimestral, mensual -> anual) sobre tablas en Capa Gold.
"""


from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class DerivationGranularity(str, Enum):
    """Granularidad temporal solicitada para derivación analítica."""

    ANNUAL = "ANNUAL"
    SEMESTER = "SEMESTER"
    QUARTER = "QUARTER"
    MONTHLY = "MONTHLY"
    CUSTOM_RANGE = "CUSTOM_RANGE"


class DerivationStatus(str, Enum):
    """Estado de resolución de la evaluación de derivación."""

    DERIVABLE = "DERIVABLE"
    MISSING_PERIODS = "MISSING_PERIODS"
    NOT_DERIVABLE = "NOT_DERIVABLE"


class PeriodBreakdown(BaseModel):
    """Desglose formal de un periodo macro en sus componentes atómicos mensuales."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_period: str = Field(..., description="Cadena del periodo original")
    granularity: DerivationGranularity = Field(
        ..., description="Granularidad temporal clasificada"
    )
    gestion: int = Field(..., ge=1990, le=2100, description="Año calendario")
    required_months: list[int] = Field(
        ..., description="Lista ordenada de meses requeridos (1 a 12)"
    )
    required_period_keys: list[str] = Field(
        ...,
        description="Lista de claves canónicas ISO YYYY-MM (ej: '2024-01')",
    )


class DerivationEvaluationRequest(BaseModel):
    """Solicitud de evaluación de derivabilidad para el Pre-flight Gatekeeper."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset_code: str = Field(
        default="mart_cartera",
        description="Identificador del datamart o tabla a evaluar",
    )
    period: str = Field(
        ...,
        description="Periodo solicitado (ej: '2024', '2024-Q1', '2024-S2', '2024-05')",
    )
    institution_id: str | None = Field(
        default=None,
        description="Código opcional de institución para acotar la consulta",
    )


class DerivationEvaluationResult(BaseModel):
    """Resultado formal del análisis de derivación analítica (HUI-02)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    status: DerivationStatus = Field(
        ...,
        description="Estado de la derivación: DERIVABLE o MISSING_PERIODS",
    )
    is_derivable: bool = Field(
        ...,
        description="True si el 100% de los insumos requeridos existen en Gold",
    )
    target_period: str = Field(
        ...,
        description="Periodo objetivo solicitado",
    )
    granularity: DerivationGranularity = Field(
        ...,
        description="Granularidad identificada",
    )
    required_periods: list[str] = Field(
        ...,
        description="Lista de todos los periodos mensuales necesarios",
    )
    available_periods: list[str] = Field(
        ...,
        description="Lista de periodos ya consolidados y vigentes en Gold",
    )
    missing_periods: list[str] = Field(
        ...,
        description="Lista exacta de periodos faltantes que impiden la derivación",
    )
    coverage_ratio: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Ratio de cobertura alcanzado (1.0 = 100% completo)",
    )
    precompiled_sql: str | None = Field(
        default=None,
        description="Sentencia SQL precompilada sobre Gold si es DERIVABLE",
    )
    explanation: str = Field(
        ...,
        description="Detalle técnico legible para el analista o motor de ingesta",
    )
