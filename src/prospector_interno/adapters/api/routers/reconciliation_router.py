"""Router de Conciliación y Diff con Prospector Externo usando DuckDB."""

from fastapi import APIRouter

from prospector_interno.adapters.api.reconciliation_service import (
    DuckDBReconciliationService,
)
from prospector_interno.adapters.api.schemas import (
    ReconciliationDiffRequest,
    ReconciliationDiffResponse,
)

router = APIRouter(prefix="/reconciliation", tags=["3. Conciliación & DuckDB Diff"])

_reconciliation_service = DuckDBReconciliationService()


@router.post(
    "/diff",
    response_model=ReconciliationDiffResponse,
    summary="Interrogar y cruzar los 3 JSONs del Prospector Externo con DuckDB",
    description=(
        "Ejecuta consultas vectorizadas con DuckDB directamente sobre "
        "map_metadata.json para verificar validez y metadata de URLs."
    ),
)
def diff_with_external_map(
    request: ReconciliationDiffRequest,
) -> ReconciliationDiffResponse:
    """Ejecuta la comparación rápida entre inventario interno y mapa externo."""
    return _reconciliation_service.diff_with_external_map(
        source_code=request.source_code,
        json_path=request.map_metadata_json_path,
        search_pattern=request.search_pattern,
    )
