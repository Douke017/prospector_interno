"""Router para el Diccionario Técnico de Datos y Relaciones entre Repositorios."""

from __future__ import annotations

from typing import Any
from fastapi import APIRouter, HTTPException, Query

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DATAMART_CATALOG,
)
from prospector_interno.adapters.api import dependencies as deps
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
)
def list_metrics(
    category: str | None = Query(
        default=None,
        description="Filtro opcional por categoría (CARTERA, BALANCE, SOLVENCIA, RENTABILIDAD)",
    ),
) -> SemanticDictionaryResponse:
    """Consulta el catálogo conceptual de métricas financieras."""
    return _semantic_service.list_metrics(category=category)


@router.get(
    "/metrics/{metric_code}",
    response_model=FinancialMetricDefinition,
    summary="Consultar detalle de una métrica financiera específica",
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


@router.get(
    "/datamarts",
    summary="Consultar diccionario técnico de variables por repositorio consolidado",
)
def list_datamart_dictionary(
    datamart: str | None = Query(
        default=None,
        description="Filtrar opcionalmente por código de repositorio (ej. 'mart_cartera')",
    ),
) -> dict[str, Any]:
    """Retorna el inventario técnico de columnas, tipos, unidades y fórmulas."""
    try:
        cols = deps.semantic_dict_repo.list_datamart_columns(datamart_name=datamart)
    except Exception:
        cols = []

    return {
        "total_columns": len(cols),
        "datamart_filter": datamart,
        "columns": cols,
    }


@router.get(
    "/datamarts/{datamart_name}",
    summary="Consultar diccionario técnico y metadatos de un repositorio específico",
)
def get_datamart_dictionary_detail(datamart_name: str) -> dict[str, Any]:
    """Retorna definición del repositorio, sus columnas y sus relaciones formales."""
    clean_dm = datamart_name.strip().lower()
    if clean_dm not in DATAMART_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"El repositorio consolidado '{datamart_name}' no está registrado.",
        )

    cfg = DATAMART_CATALOG[clean_dm]
    try:
        cols = deps.semantic_dict_repo.list_datamart_columns(datamart_name=clean_dm)
        rels = deps.semantic_dict_repo.list_relationships(datamart_name=clean_dm)
    except Exception:
        cols = []
        rels = []

    return {
        "datamart": clean_dm,
        "title": cfg["title"],
        "source_code": cfg["source_code"],
        "description": cfg["description"],
        "total_columns": len(cols),
        "columns": cols,
        "relationships": rels,
    }


@router.get(
    "/relationships",
    summary="Listar relaciones de integración entre los repositorios consolidados",
)
def list_datamart_relationships(
    datamart: str | None = Query(
        default=None,
        description="Filtro opcional por repositorio involucrado",
    ),
) -> list[dict[str, Any]]:
    """Retorna cómo se relacionan las tablas consolidadas para cruces analíticos."""
    try:
        return deps.semantic_dict_repo.list_relationships(datamart_name=datamart)
    except Exception:
        return []
