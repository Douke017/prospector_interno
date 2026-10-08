"""Contrato Canónico de Búsqueda Federada Unificada (HUI-20, Sección 10.1)."""

from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict, Field


class UnifiedSearchDocument(BaseModel):
    """Documento canónico proyectado para indexación en el motor federado."""

    model_config = ConfigDict(extra="forbid")

    search_id: str = Field(
        ...,
        description="Identificador canónico: source_id:dataset_id:period:sha256[:16]",
        examples=["finrural:reporte_mensual:2025-05:a1b2c3d4e5f67890"],
    )
    origin: str = Field(
        default="INTERNAL_GOLD",
        description="Origen del activo (INTERNAL_GOLD, INTERNAL_BRONZE, etc.)",
    )
    source_id: str = Field(
        ...,
        description="Identificador de la fuente original",
    )
    title: str = Field(
        ...,
        description="Título legible o descriptor del documento",
    )
    document_type: str = Field(
        ...,
        description="Formato del archivo (PDF, PARQUET, XLSX, etc.)",
    )
    period_end: str = Field(
        ...,
        description="Fecha de corte o fin de período (YYYY-MM-DD o YYYY-MM)",
    )
    sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description="Hash SHA-256 íntegro del activo",
    )
    download_url: str | None = Field(
        default=None,
        description="URL pública de origen o descarga si aplica",
    )
    local_path: str | None = Field(
        default=None,
        description="Ruta física local en el sistema de archivos",
    )
    gold_table: str | None = Field(
        default=None,
        description="Nombre de tabla en capa Gold si aplica",
    )
    entities_mentioned: list[str] = Field(
        default_factory=list,
        description="Entidades financieras mencionadas en el activo",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Etiquetas de clasificación temática",
    )
    indexed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de indexación o generación del documento",
    )


class UnifiedSearchIndexResponse(BaseModel):
    """Contenedor de respuesta para exportación de índice de búsqueda federada."""

    model_config = ConfigDict(extra="forbid")

    total_documents: int = Field(
        ...,
        ge=0,
        description="Cantidad total de documentos indexables",
    )
    origin: str = Field(
        default="INTERNAL_PROSPECTOR",
        description="Emisor de la exportación del índice",
    )
    since_timestamp: datetime | None = Field(
        default=None,
        description="Filtro temporal incremental aplicado a la exportación",
    )
    documents: list[UnifiedSearchDocument] = Field(
        default_factory=list,
        description="Lista de documentos proyectados",
    )
