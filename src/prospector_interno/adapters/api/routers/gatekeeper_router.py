"""Router del Control Previo de Solicitudes (Gatekeeper — Objetivos 1, 5 y 7)."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse
import uuid

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.routers.downloads_router import get_worker_pool
from prospector_interno.adapters.api.schemas import (
    BatchGatekeeperEvaluateRequest,
    BatchGatekeeperEvaluateResponse,
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
from prospector_interno.domain.models import (
    GatekeeperMode,
    IngestionRequest,
    PreFlightVerdict,
)

router = APIRouter(prefix="/gatekeeper", tags=["2. Pre-flight Gatekeeper"])

# Referencias compartidas con soporte de inyección para pruebas
_storage = deps.bronze_storage
_state = deps.gatekeeper_state
_gold_checker = deps.gold_partition_checker
_derivation_filter = deps.derivation_filter
_download_tracking = deps.download_tracking
_download_state_filter = deps.download_state_filter
_gatekeeper = deps.gatekeeper
_dedup_repo = deps.dedup_repo


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


def set_dedup_repo(repo: Any) -> None:
    """Permite inyectar un repositorio de deduplicación aislado para pruebas."""
    global _dedup_repo
    _dedup_repo = repo


def _resolve_domain_and_url(
    request: IngestionEvaluateRequest,
    *,
    allow_inventory_lookup: bool = True,
) -> tuple[str, str]:
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
    if not target_url and allow_inventory_lookup and deps.url_inventory_repo is not None:
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
    return domain, target_url


def _build_domain_request(
    request: IngestionEvaluateRequest,
    *,
    override_mode: GatekeeperMode | None = None,
    override_ttl: int | None = None,
    allow_inventory_lookup: bool = True,
) -> IngestionRequest:
    req_id = request.request_id or f"req-{uuid.uuid4().hex[:8]}"
    domain, target_url = _resolve_domain_and_url(
        request, allow_inventory_lookup=allow_inventory_lookup
    )
    eff_mode = override_mode if override_mode is not None else request.resolved_mode()
    eff_ttl = override_ttl if override_ttl is not None else request.lock_ttl_seconds
    return IngestionRequest(
        request_id=req_id,
        dataset_code=request.dataset_code,
        domain=domain,
        target_url=target_url,
        period=request.period,
        checksum_sha256=request.checksum_sha256,
        etag=request.etag,
        force_refresh=request.force_refresh,
        mode=eff_mode,
        lock_ttl_seconds=eff_ttl,
    )


def _to_eval_response(evaluation: Any, mode: GatekeeperMode) -> IngestionEvaluateResponse:
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
        mode=mode,
        cached_artifact_id=cached_id,
        in_flight_task_id=evaluation.in_flight_task_id,
        task_id=evaluation.in_flight_task_id,
        status=evaluation.verdict.value,
        subscribers_count=evaluation.subscribers_count,
        derivation_query=evaluation.derivation_query,
        missing_periods=evaluation.missing_periods,
        action_links=evaluation.action_links,
        latency_ms=evaluation.latency_ms,
        dedup_criterion=evaluation.dedup_criterion,
    )


@router.post(
    "/evaluate",
    response_model=IngestionEvaluateResponse,
    summary="Evaluar solicitud de procesamiento antes de descarga (DRY_RUN por defecto)",
)
def evaluate_ingestion(
    request: IngestionEvaluateRequest,
) -> IngestionEvaluateResponse:
    """Ejecuta las reglas de control previo sin generar bloqueos fantasma en DRY_RUN."""
    domain_request = _build_domain_request(request)
    bulkhead = get_bulkhead_manager()
    evaluation = bulkhead.execute_gatekeeper(_gatekeeper.evaluate, domain_request)
    return _to_eval_response(evaluation, domain_request.mode)


@router.post(
    "/evaluate-batch",
    response_model=BatchGatekeeperEvaluateResponse,
    summary="Evaluar en lote hasta 500 solicitudes en el Pre-Flight Gatekeeper",
)
def evaluate_ingestion_batch(
    payload: BatchGatekeeperEvaluateRequest,
) -> BatchGatekeeperEvaluateResponse:
    """Clasifica hasta 500 solicitudes en una sola llamada con SLA < 150 ms por cada 100 ítems."""
    t0 = time.perf_counter()
    domain_requests = [
        _build_domain_request(
            item,
            override_mode=payload.mode,
            override_ttl=payload.lock_ttl_seconds,
            allow_inventory_lookup=item.target_url is None and len(payload.items) <= 25,
        )
        for item in payload.items
    ]
    bulkhead = get_bulkhead_manager()
    evaluations = bulkhead.execute_gatekeeper(_gatekeeper.evaluate_batch, domain_requests)
    responses = [_to_eval_response(ev, payload.mode) for ev in evaluations]

    summary: dict[str, int] = {}
    saved = 0
    for resp in responses:
        k = resp.verdict.value
        summary[k] = summary.get(k, 0) + 1
        if resp.verdict != PreFlightVerdict.PROCEED_DOWNLOAD:
            saved += 1

    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    return BatchGatekeeperEvaluateResponse(
        total=len(responses),
        mode=payload.mode,
        elapsed_ms=elapsed_ms,
        summary=summary,
        saved_downloads=saved,
        results=responses,
    )


@router.get(
    "/metrics",
    summary="Telemetría real p50/p95/p99 y ahorro de red del Pre-Flight Gatekeeper",
)
def get_gatekeeper_metrics() -> dict[str, Any]:
    """Retorna latencias p50/p95/p99 y descargas evitadas por el Gatekeeper."""
    metrics = _gatekeeper.get_metrics()
    if _dedup_repo is not None:
        metrics["deduplication_savings"] = _dedup_repo.get_savings_summary()
    return metrics


@router.get(
    "/deduplication-audit",
    summary="Auditoría histórica de duplicados interceptados y alias SHA-256 (Objetivo 5)",
)
def get_deduplication_audit(
    limit: int = Query(default=100, ge=1, le=500),
    criterion: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """Retorna bitácora y resumen de ahorros de deduplicación en doble capa."""
    if _dedup_repo is None:
        return {
            "summary": {
                "total_intercepted": 0,
                "total_bytes_saved": 0,
                "total_aliases": 0,
                "by_criterion": {},
            },
            "events": [],
            "aliases": [],
        }
    events = _dedup_repo.list_audit(limit=limit, criterion=criterion, source_id=source_id)
    aliases = _dedup_repo.list_aliases()
    return {
        "summary": _dedup_repo.get_savings_summary(),
        "events": [e.model_dump(mode="json") for e in events],
        "aliases": [a.model_dump(mode="json") for a in aliases[:100]],
    }


class EnqueueMissingPeriodsRequest(BaseModel):
    """Solicitud delta para encolar únicamente los periodos faltantes detectados."""

    dataset_code: str = Field(..., examples=["mart_cartera", "catalogo_series_files"])
    period: str = Field(..., examples=["2025", "2024-06..2025-03"])
    source_id: str | None = Field(default=None)
    url_template: str | None = Field(
        default=None,
        description="Plantilla URL opcional con '{period}' para construir enlaces de descarga delta.",
    )
    institution_id: str | None = Field(default=None)
    execute_now: bool = Field(default=True)


@router.post(
    "/inspect",
    response_model=DerivationEvaluationResult,
    summary="Evaluar reutilización de insumos consolidados (rangos arbitrarios y maestros)",
)
def inspect_derivation(
    request: DerivationEvaluationRequest,
) -> DerivationEvaluationResult:
    """Inspecciona la cobertura de particiones aislada en pool Gatekeeper."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_gatekeeper(_derivation_filter.evaluate, request)


@router.post(
    "/enqueue-missing",
    summary="Generar solicitud delta encolando únicamente los periodos faltantes (Objetivo 7)",
)
def enqueue_missing_periods(
    payload: EnqueueMissingPeriodsRequest,
) -> dict[str, Any]:
    """Evalúa cobertura y encola exclusivamente los cortes faltantes en el pool de workers."""
    bulkhead = get_bulkhead_manager()
    evaluation = bulkhead.execute_gatekeeper(
        _derivation_filter.evaluate,
        DerivationEvaluationRequest(
            dataset_code=payload.dataset_code,
            period=payload.period,
            institution_id=payload.institution_id,
        ),
    )
    ds_clean = payload.dataset_code.strip().lower()
    if evaluation.is_derivable:
        return {
            "status": "ALREADY_COMPLETE",
            "dataset_code": payload.dataset_code,
            "target_period": payload.period,
            "coverage_ratio": evaluation.coverage_ratio,
            "available_periods": evaluation.available_periods,
            "missing_periods": [],
            "enqueued_count": 0,
            "enqueued_periods": [],
            "tasks": [],
            "precompiled_sql": evaluation.precompiled_sql,
            "action_links": {
                "parquet_export_url": f"/api/v1/analytics/export/parquet?datamart={ds_clean}",
                "csv_export_url": f"/api/v1/analytics/export/csv?datamart={ds_clean}",
                "jsonl_export_url": f"/api/v1/analytics/export/jsonl?datamart={ds_clean}",
            },
        }

    src = (payload.source_id or ds_clean.upper()).strip().upper()
    pool = get_worker_pool()
    tasks: list[dict[str, Any]] = []
    for missing_p in evaluation.missing_periods:
        if payload.url_template and "{period}" in payload.url_template:
            target_url = payload.url_template.format(period=missing_p)
        else:
            target_url = f"https://datos.{src.lower()}.gob.bo/{ds_clean}/{missing_p}.xlsx"
        rec, coalesced = pool.enqueue(
            source_id=src,
            canonical_url=target_url,
            dataset_code=payload.dataset_code,
            period_label=missing_p,
            auto_wake=payload.execute_now,
        )
        tasks.append(
            {
                "period": missing_p,
                "download_id": rec.download_id,
                "canonical_url": rec.canonical_url,
                "status": rec.status.value,
                "coalesced": coalesced,
            }
        )

    return {
        "status": "DELTA_ENQUEUED",
        "dataset_code": payload.dataset_code,
        "target_period": payload.period,
        "coverage_ratio": evaluation.coverage_ratio,
        "available_periods": evaluation.available_periods,
        "missing_periods": evaluation.missing_periods,
        "enqueued_count": len(tasks),
        "enqueued_periods": list(evaluation.missing_periods),
        "tasks": tasks,
        "precompiled_sql": None,
        "action_links": {
            "stream_url": "/api/v1/downloads/stream",
            "queue_status_url": "/api/v1/downloads/status?include_recent=true",
        },
    }


@router.get(
    "/bulkhead/stats",
    response_model=BulkheadStats,
    summary="Métricas de aislamiento de hilos y memoria",
)
def get_bulkhead_metrics() -> BulkheadStats:
    """Retorna métricas operativas en tiempo real."""
    return get_bulkhead_manager().get_stats()
