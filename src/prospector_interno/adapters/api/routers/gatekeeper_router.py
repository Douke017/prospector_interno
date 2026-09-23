"""Router del Pre-flight Gatekeeper para verificación de cache hits y locks."""

import uuid
from pathlib import Path
from urllib.parse import urlparse
from fastapi import APIRouter

from prospector_interno.adapters.api.schemas import (
    IngestionEvaluateRequest,
    IngestionEvaluateResponse,
)
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.models import IngestionRequest

router = APIRouter(prefix="/gatekeeper", tags=["2. Pre-flight Gatekeeper"])

# Instancias compartidas para el runtime de la API
_storage = LocalBronzeStorage(base_path=Path("data/bronze"))
_state = InMemoryGatekeeperState()
_gatekeeper = PreFlightGatekeeper(storage=_storage, state_registry=_state)


@router.post(
    "/evaluate",
    response_model=IngestionEvaluateResponse,
    summary="Evaluar solicitud de ingesta antes de realizar descarga",
    description=(
        "Verifica si el artefacto ya existe en Capa Bronze/Gold (Cache Hit) "
        "o si otra corrutina ya lo está adquiriendo (In-flight Lock)."
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

    evaluation = _gatekeeper.evaluate(domain_request)

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
    )
