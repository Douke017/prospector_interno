"""Router para la API de Autoservicio y Consultas Analíticas (HUI-21)."""

from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.analytics.views_service import (
    PredefinedViewsService,
    ViewNotFoundError,
)
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.application.pipelines.silver_pipeline import (
    SilverTransformPipeline,
)
from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewMetadata,
    PredefinedViewQueryRequest,
    PredefinedViewResult,
)

router = APIRouter(prefix="/analytics", tags=["6. Autoservicio Analítico"])

_analytics_adapter = DuckDBAnalyticsAdapter(base_gold_path=Path("data/gold"))
_bronze_storage = LocalBronzeStorage(base_path=Path("data/bronze"))
_silver_pipeline = SilverTransformPipeline()
_gold_materializer = GoldMaterializer(base_gold_path=Path("data/gold"))
_views_service = PredefinedViewsService(base_gold_path=Path("data/gold"))


@router.post(
    "/query",
    response_model=AnalyticsQueryResponse,
    summary="Consultar Datamart en Capa Gold con DuckDB (HUI-14 / HUI-21)",
    description=(
        "Ejecuta una consulta SQL vectorizada sobre las particiones Parquet "
        "de la Capa Gold y devuelve el dataset filtrado en milisegundos."
    ),
)
def query_gold_mart(request: AnalyticsQueryRequest) -> AnalyticsQueryResponse:
    """Ejecuta consulta analítica sobre la Capa Gold."""
    return _analytics_adapter.execute_query(request)


@router.get(
    "/dimensions",
    summary="Listar dimensiones disponibles en Capa Gold",
    description="Retorna las instituciones, años y meses consolidados en el datamart.",
)
def get_gold_dimensions(
    datamart: str = "mart_cartera",
) -> dict[str, list[Any]]:
    """Consulta las dimensiones disponibles en el datamart."""
    return _analytics_adapter.get_dimensions(datamart)


@router.get(
    "/views",
    response_model=list[PredefinedViewMetadata],
    summary="Listar catálogo de vistas analíticas predefinidas (HUI-22)",
    description=(
        "Retorna la lista de consultas frecuentes de negocio optimizadas "
        "para visualización directa en Portal Nexus."
    ),
)
def list_predefined_views() -> list[PredefinedViewMetadata]:
    """Retorna las vistas analíticas disponibles."""
    return _views_service.list_views()


@router.get(
    "/views/{view_id}",
    response_model=PredefinedViewMetadata,
    summary="Consultar metadatos de una vista analítica específica",
    description=(
        "Devuelve el título, descripción, filtros admitidos y columnas de la vista."
    ),
)
def get_predefined_view(view_id: str) -> PredefinedViewMetadata:
    """Consulta los metadatos de una vista precalculada."""
    view_meta = _views_service.get_view(view_id)
    if not view_meta:
        raise HTTPException(
            status_code=404,
            detail=f"La vista analítica '{view_id}' no existe en el catálogo.",
        )
    return view_meta


@router.post(
    "/views/{view_id}/execute",
    response_model=PredefinedViewResult,
    summary="Ejecutar vista analítica precalculada (HUI-22)",
    description=(
        "Ejecuta la consulta analítica optimizada con DuckDB aplicando filtros "
        "opcionales de año, mes o instituciones."
    ),
)
def execute_predefined_view(
    view_id: str,
    request: PredefinedViewQueryRequest | None = None,
) -> PredefinedViewResult:
    """Ejecuta la vista analítica solicitada."""
    try:
        return _views_service.execute_view(view_id, request=request)
    except ViewNotFoundError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err


@router.post(
    "/materialize",
    summary="Materializar Datamart Gold desde Capa Bronze (Pipeline ETL)",
    description=(
        "Lee los documentos brutos en Bronze, ejecuta la normalización Silver "
        "y genera las particiones Parquet en Capa Gold."
    ),
)
def materialize_gold_from_bronze() -> dict[str, Any]:
    """Ejecuta el pipeline Bronze -> Silver -> Gold sobre los archivos locales."""
    artifacts = _bronze_storage.get_artifacts()

    silver_records = _silver_pipeline.process_artifacts(artifacts)
    gold_records = _gold_materializer.transform_to_mart_cartera(silver_records)
    mart_path = _gold_materializer.materialize_mart_cartera(gold_records)

    return {
        "status": "COMPLETED",
        "total_bronze_artifacts": len(artifacts),
        "total_silver_records": len(silver_records),
        "total_gold_records": len(gold_records),
        "mart_path": str(mart_path),
    }
