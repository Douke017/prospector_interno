"""Contratos para el Aislamiento de Cargas Bulkhead y Presupuesto de RAM (HUI-23)."""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class BulkheadPoolType(str, Enum):
    """Identificador del pool de aislamiento de ejecución."""

    GATEKEEPER = "GATEKEEPER"
    ANALYTICS = "ANALYTICS"


class BulkheadConfig(BaseModel):
    """Configuración de dimensionamiento y límites de memoria del Bulkhead."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gatekeeper_max_workers: int = Field(
        default=8,
        ge=1,
        le=64,
        description="Hilos concurrentes para validaciones rápidas (<50ms)",
    )
    analytics_max_workers: int = Field(
        default=4,
        ge=1,
        le=32,
        description=(
            "Hilos concurrentes para consultas analíticas pesadas sobre Capa Gold"
        ),
    )
    max_memory_duckdb: str = Field(
        default="4GB",
        description="Presupuesto máximo de memoria asignado al motor DuckDB",
    )


class BulkheadStats(BaseModel):
    """Métricas en tiempo real del estado de los pools de aislamiento."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gatekeeper_active_workers: int = Field(
        ...,
        ge=0,
        description="Hilos de Gatekeeper ejecutando tareas activas",
    )
    gatekeeper_max_workers: int = Field(
        ...,
        ge=1,
        description="Capacidad máxima de hilos del pool de Gatekeeper",
    )
    analytics_active_workers: int = Field(
        ...,
        ge=0,
        description="Hilos de analítica ejecutando consultas activas",
    )
    analytics_max_workers: int = Field(
        ...,
        ge=1,
        description="Capacidad máxima de hilos del pool de analítica",
    )
    max_memory_duckdb: str = Field(
        ...,
        description="Presupuesto de memoria RAM configurado para DuckDB",
    )
