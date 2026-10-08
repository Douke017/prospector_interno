"""Router del Pre-flight Gatekeeper para cache hits, locks y derivación."""


from pathlib import Path
from urllib.parse import urlparse
import uuid
from fastapi import APIRouter

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBGoldPartitionAdapter,
)
from prospector_interno.adapters.api.schemas import (
    IngestionEvaluateRequest,
    IngestionEvaluateResponse,
)
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.resilience.bulkhead_manager import (
    get_bulkhead_manager,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.bulkhead_spec import BulkheadStats
from prospector_interno.domain.contracts.derivation_spec import (
    DerivationEvaluationRequest,
    DerivationEvaluationResult,
)
from prospector_interno.domain.gatekeeper.derivation_filter import (
    AssetDerivationFilter,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.models import IngestionRequest

router = APIRouter(prefix="/gatekeeper", tags=["2. Pre-flight Gatekeeper"])

# Instancias compartidas para el runtime de la API
_storage = LocalBronzeStorage(base_path=Path("data/bronze"))
_state = InMemoryGatekeeperState()
_gold_checker = DuckDBGoldPartitionAdapter(base_gold_path=Path("data/gold"))
_derivation_filter = AssetDerivationFilter(
    partition_checker=_gold_checker,
    base_gold_path=Path("data/gold"),
)
_download_tracking = SqliteDownloadTrackingAdapter(
    db_path=Path("data/catalog/datax_catalog.db")
)
_download_state_filter = DownloadStateFilter(tracking_port=_download_tracking)
_gatekeeper = PreFlightGatekeeper(
    storage=_storage,
    state_registry=_state,
    derivation_filter=_derivation_filter,
    download_state_filter=_download_state_filter,
)


def set_gatekeeper(gatekeeper: PreFlightGatekeeper) -> None:
    """Permite inyectar un Gatekeeper aislado para pruebas."""
    global _gatekeeper
    _gatekeeper = gatekeeper


def set_derivation_filter(filter_instance: AssetDerivationFilter) -> None:
    """Permite inyectar un filtro de derivación aislado para pruebas."""
    global _derivation_filter
    _derivation_filter = filter_instance


def set_download_state_filter(filter_instance: DownloadStateFilter) -> None:
    """Permite inyectar un filtro de ciclo de vida de descarga para pruebas."""
    global _download_state_filter
    _download_state_filter = filter_instance


@router.post(
    "/evaluate",
    response_model=IngestionEvaluateResponse,
    summary="Evaluar solicitud antes de descarga (HUI-01/HUI-02/HUI-04)",
    description=(

        "Verifica si el artefacto es derivable de Capa Gold (REUSE_DERIVATION), "
        "si ya existe en Bronze (CACHE_HIT_BRONZE), o si está bloqueado en vuelo."
    ),
)
def evaluate_ingestion(
    request: IngestionEvaluateRequest,
) -> IngestionEvaluateResponse:
    """Ejecuta las reglas de pre-vuelo para prevenir tráfico de red redundante."""
    req_id = request.request_id or f"req-{uuid.uuid4().hex[:8]}"

    domain = request.domain
    if not domain and request.target_url:
        domain = urlparse(request.target_url).netloc
    if not domain and request.source_id:
        domain = f"www.{request.source_id}.gob.bo"
    if not domain:
        domain = "portal.gob.bo"

    target_url = (
        request.target_url
        or f"https://{domain}/data/{request.dataset_code}_{request.period}.xlsx"
    )

    domain_request = IngestionRequest(
        request_id=req_id,
        dataset_code=request.dataset_code,
        domain=domain,
        target_url=target_url,
        period=request.period,
        force_refresh=request.force_refresh,
    )

    bulkhead = get_bulkhead_manager()
    evaluation = bulkhead.execute_gatekeeper(_gatekeeper.evaluate, domain_request)

    cached_id = (
        evaluation.cached_artifact.artifact_id
        if evaluation.cached_artifact
        else None
    )

    return IngestionEvaluateResponse(
        verdict=evaluation.verdict,
        idempotency_key=evaluation.idempotency_key,
        reason=evaluation.reason,
        lock_acquired=evaluation.lock_acquired,
        cached_artifact_id=cached_id,
        in_flight_task_id=evaluation.in_flight_task_id,
        task_id=evaluation.in_flight_task_id,
        status=evaluation.verdict.value,
        subscribers_count=evaluation.subscribers_count,
        derivation_query=evaluation.derivation_query,
        missing_periods=evaluation.missing_periods,
    )


@router.post(
    "/inspect",
    response_model=DerivationEvaluationResult,
    summary="Inspeccionar derivabilidad de periodos macro sobre Capa Gold (HUI-02)",
    description=(
        "Evalúa si un periodo macro (anual, trimestral, semestral) puede "
        "construirse agregando particiones mensuales en Gold sin requerir descargas. "
        "Si faltan periodos, retorna la lista exacta de insumos pendientes."
    ),
)
def inspect_derivation(
    request: DerivationEvaluationRequest,
) -> DerivationEvaluationResult:
    """Inspecciona la cobertura de particiones aislada en pool Gatekeeper (HUI-23)."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_gatekeeper(_derivation_filter.evaluate, request)


@router.get(
    "/bulkhead/stats",
    response_model=BulkheadStats,
    summary="Métricas de aislamiento de hilos y memoria (HUI-23)",
    description="Retorna el estado de los pools de workers y presupuesto de RAM.",
)
def get_bulkhead_metrics() -> BulkheadStats:
    """Retorna métricas del pool Bulkhead en tiempo real."""
    return get_bulkhead_manager().get_stats()
