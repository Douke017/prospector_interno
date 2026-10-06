"""Especificación de contratos para Trazabilidad y Grafo de Linaje (HUI-17).

Define los contratos inmutables requeridos para auditar la cadena de custodia
desde el Prospector Externo hasta los Datamarts en Capa Gold.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class StorageLayer(str, Enum):
    """Capa de persistencia según el patrón Medallion."""

    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class InternalAssetRecord(BaseModel):
    """Registro canónico de un activo de datos catalogado (Sección 14.1)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    asset_id: str = Field(
        ...,
        description="Identificador único del activo (ej: 'gold_mart_cartera_2025_05')",
    )
    source_id: str = Field(
        ...,
        description="Identificador de la institución (ej: 'finrural', 'asfi')",
    )
    dataset_id: str = Field(
        ...,
        description="Código del dataset (ej: 'mart_cartera', 'balance')",
    )

    period_start: str = Field(
        ...,
        description="Fecha de inicio del periodo en formato ISO YYYY-MM-DD",
    )
    period_end: str = Field(
        ...,
        description="Fecha de fin del periodo en formato ISO YYYY-MM-DD",
    )
    storage_layer: StorageLayer = Field(
        ...,
        description="Capa Medallion donde reside el activo",
    )
    file_path: str = Field(
        ...,
        description="Ruta relativa o absoluta del archivo o partición física",
    )
    content_hash_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description="Hash criptográfico SHA-256 del contenido",
    )
    row_count: int | None = Field(
        default=None,
        ge=0,
        description="Cantidad total de registros tabulares contenidos",
    )
    size_bytes: int = Field(
        ...,
        ge=0,
        description="Tamaño físico del archivo en bytes",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de registro del activo",
    )
    last_updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de última modificación",
    )


class DataLineageRecord(BaseModel):
    """Vínculo de linaje y cadena de custodia (Sección 14.1 data_lineage)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    lineage_id: str = Field(
        ...,
        description="Identificador único y determinista del nodo de linaje",
    )
    gold_asset_id: str = Field(
        ...,
        description="Identificador del activo en Capa Gold derivado",
    )
    bronze_source_file: str = Field(
        ...,
        description="Nombre o ruta del archivo de entrada físico en Capa Bronze",
    )
    bronze_file_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description="Hash SHA-256 verificado del archivo de entrada en Bronze",
    )
    external_run_id: str = Field(
        ...,
        description="Identificador de corrida del Prospector Externo",
    )
    pipeline_version: str = Field(
        ...,
        description="Versión del pipeline de transformación (ej: 'v1.0.0')",
    )
    transformed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp de transformación y consolidación",
    )


class ProvenanceStep(BaseModel):
    """Paso cronológico verificado dentro de la cadena de custodia."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    step_order: int = Field(..., ge=1, description="Secuencia cronológica del paso")
    step_name: str = Field(..., description="Nombre del proceso auditado")
    component: str = Field(..., description="Subsistema responsable del procesamiento")
    timestamp: datetime = Field(..., description="Momento de ejecución del paso")
    input_digest: str | None = Field(
        default=None,
        description="Hash de los insumos consumidos",
    )
    output_digest: str = Field(
        ...,
        description="Hash o firma del producto generado",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Metadatos contextuales (headers, URLs, parámetros)",
    )


class AssetLineageGraphResponse(BaseModel):
    """Grafo de linaje completo para auditoría de cumplimiento (HUI-17)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    asset: InternalAssetRecord = Field(
        ...,
        description="Metadatos del activo consultado",
    )
    lineage_records: list[DataLineageRecord] = Field(
        ...,
        description="Lista de enlaces directos a insumos de Capa Bronze",
    )
    upstream_bronze_files: list[str] = Field(
        ...,
        description="Listado de archivos físicos de origen",
    )
    upstream_sha256_digests: list[str] = Field(
        ...,
        description="Listado de hashes SHA-256 de los insumos de origen",
    )
    external_run_ids: list[str] = Field(
        ...,
        description="Listado de corridas externas asociadas",
    )
    provenance_chain: list[ProvenanceStep] = Field(
        ...,
        description="Cadena de custodia ordenada de extremo a extremo",
    )


class LineageIntegrityCheckResult(BaseModel):
    """Resultado de verificación criptográfica de autenticidad en disco."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    asset_id: str
    is_valid: bool = Field(
        ...,
        description="True si los hashes físicos coinciden con el registro inmutable",
    )
    recorded_sha256: str
    current_disk_sha256: str | None = None
    all_upstream_intact: bool = Field(
        ...,
        description=(
            "True si todos los archivos de origen en Bronze siguen "
            "existiendo e intactos"
        ),
    )

    upstream_details: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Estado individual de cada insumo en Bronze",
    )
    verification_message: str
    verified_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
