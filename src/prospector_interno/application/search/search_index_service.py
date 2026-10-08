"""Servicio de Aplicación para el Contrato Canónico de Búsqueda Federada (HUI-20)."""

from datetime import datetime, timezone
from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.contracts.search_spec import (
    UnifiedSearchDocument,
    UnifiedSearchIndexResponse,
)
from prospector_interno.domain.ports import LineageRepositoryPort

DEFAULT_FINRURAL_ENTITIES: list[str] = [
    "CRECER",
    "PRO MUJER",
    "IDEPRO",
    "FUBODE",
    "DIACONIA",
    "FONDECO",
    "CIDRE",
]


class SearchIndexService:
    """Proyecta el inventario técnico de activos bajo el contrato canónico.

    Implementa el contrato canónico de búsqueda unificada (HUI-20, Sección 10.1).
    """

    def __init__(self, repository: LineageRepositoryPort) -> None:
        self._repository = repository

    def build_search_id(
        self,
        source_id: str,
        dataset_id: str,
        period_end: str,
        sha256: str,
    ) -> str:
        """Calcula el identificador canónico estable.

        Formato canónico: source_id:dataset_id:period:sha256[:16].
        """
        short_hash = sha256[:16]
        return f"{source_id}:{dataset_id}:{period_end}:{short_hash}"

    def project_document(
        self,
        asset: InternalAssetRecord,
    ) -> UnifiedSearchDocument:
        """Proyecta un activo de datos interno al contrato canónico unificado."""
        search_id = self.build_search_id(
            source_id=asset.source_id,
            dataset_id=asset.dataset_id,
            period_end=asset.period_end,
            sha256=asset.content_hash_sha256,
        )

        # Inferencia de tipo de documento
        if asset.storage_layer == StorageLayer.GOLD:
            doc_type = "PARQUET"
            gold_table = asset.dataset_id
        elif asset.file_path.lower().endswith(".pdf"):
            doc_type = "PDF"
            gold_table = None
        elif (
            asset.file_path.lower().endswith(".xlsx")
            or asset.file_path.lower().endswith(".xls")
        ):
            doc_type = "XLSX"
            gold_table = None
        else:
            doc_type = "DATASET"
            gold_table = None

        # Título descriptivo limpio
        clean_name = asset.dataset_id.replace("_", " ").title()
        title = f"{asset.source_id.upper()} - {clean_name} ({asset.period_end})"

        # Entidades conocidas por fuente
        entities: list[str] = []
        if asset.source_id.lower() == "finrural":
            entities = list(DEFAULT_FINRURAL_ENTITIES)

        tags = [
            asset.source_id.lower(),
            asset.dataset_id.lower(),
            asset.storage_layer.value.lower(),
        ]
        if asset.storage_layer == StorageLayer.GOLD:
            tags.extend(["analytics", "gold_mart"])

        return UnifiedSearchDocument(
            search_id=search_id,
            origin=f"INTERNAL_{asset.storage_layer.value}",
            source_id=asset.source_id,
            title=title,
            document_type=doc_type,
            period_end=asset.period_end,
            sha256=asset.content_hash_sha256,
            download_url=None,
            local_path=asset.file_path,
            gold_table=gold_table,
            entities_mentioned=entities,
            tags=tags,
            indexed_at=asset.last_updated_at,
        )

    def export_index(
        self,
        since_timestamp: datetime | None = None,
        source_id: str | None = None,
        storage_layer: str | None = None,
        limit: int = 500,
    ) -> UnifiedSearchIndexResponse:
        """Exporta el índice de búsqueda de activos internos con filtros opcionales."""
        assets = self._repository.list_assets(
            storage_layer=storage_layer,
            source_id=source_id,
        )

        if since_timestamp is not None:
            # Normalizar timezone para comparación segura
            def _normalize(dt: datetime) -> datetime:
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

            target_ts = _normalize(since_timestamp)
            assets = [
                a
                for a in assets
                if _normalize(a.last_updated_at) >= target_ts
                or _normalize(a.created_at) >= target_ts
            ]

        # Aplicar límite superior
        selected_assets = assets[:limit]
        documents = [self.project_document(a) for a in selected_assets]

        return UnifiedSearchIndexResponse(
            total_documents=len(documents),
            origin="INTERNAL_PROSPECTOR",
            since_timestamp=since_timestamp,
            documents=documents,
        )
