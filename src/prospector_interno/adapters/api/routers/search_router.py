"""Router de Búsqueda Federada Unificada (HUI-20, Sección 10.1)."""

from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Query

from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.search.search_index_service import (
    SearchIndexService,
)
from prospector_interno.domain.contracts.search_spec import (
    UnifiedSearchIndexResponse,
)

router = APIRouter(prefix="/search", tags=["8. Búsqueda Federada"])

_default_lineage_adapter = SqliteLineageAdapter(
    Path("data/catalog/datax_catalog.db")
)
_search_service = SearchIndexService(_default_lineage_adapter)


def get_search_index_service() -> SearchIndexService:
    """Obtiene la instancia activa del servicio de índice de búsqueda."""
    return _search_service


def set_search_index_service(service: SearchIndexService) -> None:
    """Sobrescribe la instancia activa de SearchIndexService (útil para tests)."""
    global _search_service
    _search_service = service


@router.get(
    "/export-index",
    response_model=UnifiedSearchIndexResponse,
    summary="Exporta índice unificado para motor de búsqueda federada",
    description=(
        "Expone los activos internos proyectados bajo el contrato canónico "
        "UnifiedSearchDocument con identificadores estables "
        "`source_id:dataset_id:period:sha256[:16]`."
    ),
)
def export_search_index(
    since_timestamp: datetime | None = Query(
        None,
        description="Filtro incremental: activos indexados a partir de esta fecha/hora",
    ),
    source_id: str | None = Query(
        None,
        description="Filtrar por identificador de fuente emisora (ej: finrural)",
    ),
    storage_layer: str | None = Query(
        None,
        description="Filtrar por capa de persistencia (BRONZE, SILVER, GOLD)",
    ),
    limit: int = Query(
        500,
        ge=1,
        le=5000,
        description="Número máximo de documentos a exportar",
    ),
) -> UnifiedSearchIndexResponse:
    """Retorna los documentos proyectados para el índice federado."""
    service = get_search_index_service()
    return service.export_index(
        since_timestamp=since_timestamp,
        source_id=source_id,
        storage_layer=storage_layer,
        limit=limit,
    )
