"""Especificación de contrato para la Capa Silver (Datos Tabulares Limpios)."""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class StatementType(str, Enum):
    """Tipo de estado financiero o cuadro reportado."""

    SITUACION_PATRIMONIAL = "SITUACION_PATRIMONIAL"
    GANANCIAS_PERDIDAS = "GANANCIAS_PERDIDAS"
    CARTERA_DETALLADA = "CARTERA_DETALLADA"
    INDICADORES = "INDICADORES"


class AccountCategory(str, Enum):
    """Categoría contable o conceptual mayor."""

    ACTIVO = "ACTIVO"
    PASIVO = "PASIVO"
    PATRIMONIO = "PATRIMONIO"
    RESULTADO = "RESULTADO"
    CARTERA = "CARTERA"
    OTRO = "OTRO"


class SilverFinancialRecord(BaseModel):
    """Registro atómico tabular normalizado en la Capa Silver."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str = Field(
        ..., description="Identificador de la fuente (ej: 'finrural', 'bbv')"
    )
    institution_id: str = Field(
        ...,
        description="Código normalizado de la institución (ej: 'CRECER')",
    )
    institution_name: str = Field(
        ..., description="Nombre formal de la institución financiera"
    )
    period: str = Field(
        ...,
        description="Periodo temporal en formato ISO YYYY-MM (ej: '2026-01')",
    )
    gestion: int = Field(
        ..., ge=1990, le=2100, description="Año calendario (ej: 2026)"
    )
    mes: int = Field(..., ge=1, le=12, description="Mes del periodo (1 a 12)")
    statement_type: StatementType = Field(
        ..., description="Tipo de estado financiero extraído"
    )
    account_category: AccountCategory = Field(
        ..., description="Categoría contable principal"
    )
    account_name: str = Field(
        ..., description="Nombre canónico de la cuenta contable"
    )
    amount_bob: Decimal = Field(
        ..., description="Monto monetario expresado en bolivianos (BOB)"
    )
    source_file_sha256: str = Field(
        ..., description="Hash SHA-256 del archivo físico en Capa Bronze"
    )
    extracted_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de extracción y tipado hacia Silver",
    )
