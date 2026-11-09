"""Especificación de contratos para la Capa Gold (4 Repositorios Consolidados)."""

from datetime import datetime, timezone
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field


class GoldMartCarteraRecord(BaseModel):
    """Registro dimensional del Repositorio de Cartera y Mora (FINRURAL / IFDs)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gestion: int = Field(..., ge=1990, le=2100, description="Año calendario de partición")
    mes: int = Field(..., ge=1, le=12, description="Mes del periodo de partición (1 a 12)")
    institucion_id: str = Field(..., description="Código normalizado de la institución")
    institucion_nombre: str = Field(..., description="Nombre formal de la institución financiera")

    cartera_bruta: Decimal = Field(..., description="Cartera total bruta en bolivianos")
    cartera_vigente: Decimal = Field(..., description="Cartera vigente en bolivianos")
    cartera_vencida: Decimal = Field(..., description="Cartera con cuotas vencidas")
    cartera_en_ejecucion: Decimal = Field(..., description="Cartera en cobro judicial o ejecución")
    cartera_en_mora: Decimal = Field(..., description="Cartera en mora total (vencida + ejecución)")
    prevision_cartera: Decimal = Field(..., description="Previsión para incobrables")
    total_activo: Decimal = Field(..., description="Activo total consolidado en bolivianos")
    total_pasivo: Decimal = Field(..., description="Pasivo total consolidado en bolivianos")
    total_patrimonio: Decimal = Field(..., description="Patrimonio neto total en bolivianos")

    ratio_mora: Decimal = Field(..., description="Ratio de mora (cartera_en_mora / cartera_bruta)")
    ratio_cobertura: Decimal = Field(..., description="Ratio de cobertura (prevision_cartera / cartera_en_mora)")

    source_file_sha256: str = Field(..., description="Hash SHA-256 del archivo de origen en Capa Bronze")
    transformed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de consolidación dimensional en Gold",
    )


class GoldMartTasasBcbRecord(BaseModel):
    """Registro dimensional del Repositorio Monetario y Cambiario (BCB)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gestion: int = Field(..., ge=1990, le=2100)
    mes: int = Field(..., ge=1, le=12)
    institucion_id: str = Field(default="BCB")
    institucion_nombre: str = Field(default="Banco Central de Bolivia")

    base_monetaria_bs: Decimal = Field(..., description="Base monetaria en bolivianos")
    emision_monetaria_bs: Decimal = Field(..., description="Emisión monetaria en bolivianos")
    reservas_bancarias_bs: Decimal = Field(..., description="Reservas bancarias en bolivianos")
    depositos_sistema_bs: Decimal = Field(..., description="Depósitos MN en el sistema financiero en Bs.")
    cartera_sistema_bs: Decimal = Field(..., description="Crédito MN al sector privado en Bs.")
    reservas_internacionales_usd: Decimal = Field(..., description="Reservas Internacionales Netas en USD")
    inflacion_mensual_pct: Decimal = Field(..., description="Variación porcentual mensual del IPC (proporción)")
    bolivianizacion_depositos_pct: Decimal = Field(..., description="Proporción de bolivianización de depósitos")
    ufv_cierre: Decimal = Field(..., description="Cotización de cierre mensual Bs/UFV")

    source_file_sha256: str = Field(...)
    transformed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class GoldMartEstadisticasBbvRecord(BaseModel):
    """Registro dimensional del Repositorio Bursátil y Patrimonial (BBV)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gestion: int = Field(..., ge=1990, le=2100)
    mes: int = Field(..., ge=1, le=12)
    institucion_id: str = Field(default="BBV")
    institucion_nombre: str = Field(default="Bolsa Boliviana de Valores S.A.")

    disponible_bs: Decimal = Field(..., description="Disponibilidades en bolivianos")
    inversiones_bursatiles_bs: Decimal = Field(..., description="Inversiones bursátiles temporales en Bs.")
    activo_corriente_bs: Decimal = Field(..., description="Total activo corriente en bolivianos")
    inversiones_permanentes_bs: Decimal = Field(..., description="Inversiones permanentes en bolivianos")
    total_activo: Decimal = Field(..., description="Total activo en bolivianos")
    pasivo_corriente_bs: Decimal = Field(..., description="Total pasivo corriente en bolivianos")
    total_pasivo: Decimal = Field(..., description="Total pasivo en bolivianos")
    total_patrimonio: Decimal = Field(..., description="Patrimonio neto en bolivianos")
    ratio_liquidez: Decimal = Field(..., description="Liquidez corriente (activo_corriente / pasivo_corriente)")
    ratio_endeudamiento: Decimal = Field(..., description="Endeudamiento patrimonial (total_pasivo / total_patrimonio)")

    source_file_sha256: str = Field(...)
    transformed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class GoldMartIndicadoresAsfiRecord(BaseModel):
    """Registro dimensional del Repositorio de Indicadores Prudenciales (ASFI)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gestion: int = Field(..., ge=1990, le=2100)
    mes: int = Field(..., ge=1, le=12)
    institucion_id: str = Field(...)
    institucion_nombre: str = Field(...)

    total_activo: Decimal = Field(...)
    total_pasivo: Decimal = Field(...)
    total_patrimonio: Decimal = Field(...)
    cartera_bruta: Decimal = Field(...)
    prevision_cartera: Decimal = Field(...)
    coeficiente_solvencia: Decimal = Field(..., description="total_patrimonio / total_activo")
    peso_cartera_activo: Decimal = Field(..., description="cartera_bruta / total_activo")
    ratio_mora: Decimal = Field(..., description="cartera_en_mora / cartera_bruta")
    ratio_cobertura: Decimal = Field(..., description="prevision_cartera / cartera_en_mora")
    apalancamiento_veces: Decimal = Field(..., description="total_pasivo / total_patrimonio")

    source_file_sha256: str = Field(...)
    transformed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
