"""Router del Control Previo de Solicitudes (Gatekeeper)."""

from __future__ import annotations

from urllib.parse import urlparse
import uuid
from fastapi import APIRouter

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.schemas import (
    IngestionEvaluateRequest,
    IngestionEvaluateResponse,
)
from prospector_interno.adapters.resilience.bulkhead_manager import (
    get_bulkhead_manager,
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

# Referencias compartidas con soporte de inyección para pruebas
_storage = deps.bronze_storage
_state = deps.gatekeeper_state
_gold_checker = deps.gold_partition_checker
_derivation_filter = deps.derivation_filter
_download_tracking = deps.download_tracking
_download_state_filter = deps.download_state_filter
_gatekeeper = deps.gatekeeper


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
    summary="Evaluar solicitud de procesamiento antes de descarga",
    description=(
        "Verifica si la información solicitada ya existe en reportes consolidados, "
        "si puede derivarse desde insumos existentes, si ya está custodiada en Bronze "
        "o si se encuentra en cola/descarga activa."
    ),
)
def evaluate_ingestion(
    request: IngestionEvaluateRequest,
) -> IngestionEvaluateResponse:
    """Ejecuta las reglas de control previo para prevenir procesamiento redundante."""
    req_id = request.request_id or f"req-{uuid.uuid4().hex[:8]}"

    domain = request.domain
    if not domain and request.target_url:
        domain = urlparse(request.target_url).netloc
    if not domain and request.source_id:
        src_clean = request.source_id.strip().lower()
        if src_clean == "finrural":
            domain = "www.finrural.org.bo"
        elif src_clean == "bbv":
            domain = "www.bbv.com.bo"
        else:
            domain = f"www.{src_clean}.gob.bo"
    if not domain:
        domain = "portal.gob.bo"

    target_url = request.target_url
    if not target_url and deps.url_inventory_repo is not None:
        try:
            target_url = deps.url_inventory_repo.find_canonical_url(
                dataset_code=request.dataset_code,
                source_id=request.source_id,
                period=request.period,
                domain=domain,
            )
        except Exception:
            target_url = None

    if not target_url:
        target_url = f"https://{domain}/data/{request.dataset_code}_{request.period}.xlsx"

    domain_request = IngestionRequest(
        request_id=req_id,
        dataset_code=request.dataset_code,
        domain=domain,
        target_url=target_url,
        period=request.period,
        checksum_sha256=request.checksum_sha256,
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
    summary="Evaluar reutilización de insumos consolidados sobre Capa Gold",
    description=(
        "Evalúa si un periodo agregado (anual, semestral o trimestral) puede "
        "construirse reutilizando particiones mensuales ya consolidadas en Gold."
    ),
)
def inspect_derivation(
    request: DerivationEvaluationRequest,
) -> DerivationEvaluationResult:
    """Inspecciona la cobertura de particiones aislada en pool Gatekeeper."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_gatekeeper(_derivation_filter.evaluate, request)


@router.get(
    "/bulkhead/stats",
    response_model=BulkheadStats,
    summary="Métricas de aislamiento de hilos y memoria",
    description="Retorna el estado de los pools de ejecución y presupuesto de memoria.",
)
def get_bulkhead_metrics() -> BulkheadStats:
    """Retorna métricas operativas en tiempo real."""
    return get_bulkhead_manager().get_stats()
