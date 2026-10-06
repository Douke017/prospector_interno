"""Especificación de contrato para la Capa Gold (Datamarts en Parquet)."""

from datetime import datetime, timezone
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field


class GoldMartCarteraRecord(BaseModel):
    """Registro dimensional del Datamart de Cartera en Capa Gold."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Claves dimensionales y de partición
    gestion: int = Field(
        ..., ge=1990, le=2100, description="Año calendario de partición"
    )
    mes: int = Field(
        ..., ge=1, le=12, description="Mes del periodo de partición (1 a 12)"
    )
    institucion_id: str = Field(
        ..., description="Código normalizado de la institución (ej: 'CRECER')"
    )
    institucion_nombre: str = Field(
        ..., description="Nombre formal de la institución financiera"
    )

    # Métricas de cartera y balance
    cartera_bruta: Decimal = Field(
        ..., description="Cartera total bruta en bolivianos"
    )
    cartera_vigente: Decimal = Field(
        ..., description="Cartera vigente en bolivianos"
    )
    cartera_vencida: Decimal = Field(
        ..., description="Cartera con cuotas vencidas"
    )
    cartera_en_ejecucion: Decimal = Field(
        ..., description="Cartera en cobro judicial o ejecución"
    )
    cartera_en_mora: Decimal = Field(
        ..., description="Cartera en mora total (vencida + ejecución)"
    )
    prevision_cartera: Decimal = Field(
        ..., description="Previsión específica y genérica para incobrables"
    )
    total_activo: Decimal = Field(
        ..., description="Activo total consolidado en bolivianos"
    )
    total_pasivo: Decimal = Field(
        ..., description="Pasivo total consolidado en bolivianos"
    )
    total_patrimonio: Decimal = Field(
        ..., description="Patrimonio neto total en bolivianos"
    )

    # Indicadores financieros calculados
    ratio_mora: Decimal = Field(
        ..., description="Ratio de mora (cartera_en_mora / cartera_bruta)"
    )
    ratio_cobertura: Decimal = Field(
        ..., description="Ratio de cobertura (prevision_cartera / cartera_en_mora)"
    )

    # Metadatos de auditoría y linaje
    source_file_sha256: str = Field(
        ..., description="Hash SHA-256 del archivo de origen en Capa Bronze"
    )
    transformed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de consolidación dimensional en Gold",
    )
