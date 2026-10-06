"""Router para el Diccionario Semántico de Métricas Financieras (HUI-16)."""

from fastapi import APIRouter, HTTPException, Query

from prospector_interno.application.catalog.semantic_service import (
    SemanticDictionaryService,
)
from prospector_interno.domain.contracts.semantic_spec import (
    FinancialMetricDefinition,
    SemanticDictionaryResponse,
)

router = APIRouter(prefix="/dictionary", tags=["7. Diccionario Semántico"])

_semantic_service = SemanticDictionaryService()


@router.get(
    "/metrics",
    response_model=SemanticDictionaryResponse,
    summary="Listar catálogo de métricas financieras formalizadas",
    description=(
        "Retorna las definiciones conceptuales, fórmulas en LaTeX y unidades "
        "de medida de las métricas oficiales, opcionalmente filtradas por categoría."
    ),
)
def list_metrics(
    category: str | None = Query(
        default=None,
        description=(
            "Filtro opcional por categoría "
            "(CARTERA, BALANCE, SOLVENCIA, RENTABILIDAD)"
        ),
    ),
) -> SemanticDictionaryResponse:
    """Consulta el catálogo de métricas financieras."""
    return _semantic_service.list_metrics(category=category)


@router.get(
    "/metrics/{metric_code}",
    response_model=FinancialMetricDefinition,
    summary="Consultar detalle de una métrica financiera específica",
    description=(
        "Devuelve la definición formal, fórmula matemática y campos requeridos."
    ),
)
def get_metric(metric_code: str) -> FinancialMetricDefinition:
    """Busca una métrica por su código canónico único."""
    metric = _semantic_service.get_metric(metric_code)
    if not metric:
        raise HTTPException(
            status_code=404,
            detail=(
                f"La métrica financiera '{metric_code}' no existe en el diccionario."
            ),
        )
    return metric
