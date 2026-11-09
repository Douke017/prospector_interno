"""Router para Conciliación e Integración entre Prospectores."""

from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter, HTTPException

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.schemas import (
    BridgeReconcileRequest,
    BridgeReconcileResponse,
    BridgeSyncRequest,
    BridgeSyncResponse,
)
from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
)
from prospector_interno.adapters.downloader.resumable_stream_downloader import (
    ResumableStreamDownloader,
)
from prospector_interno.application.bridge.diff_engine import DuckDBDiffEngine
from prospector_interno.application.bridge.sync_orchestrator import (
    SyncOrchestrator,
)
from prospector_interno.application.scheduler import DomainTokenBucket
from prospector_interno.domain.contracts.downloader_spec import (
    DomainCircuitStatus,
)
from prospector_interno.domain.exceptions import (
    ExternalCatalogConnectionError,
    ExternalCatalogResponseError,
    ExternalMapCorruptError,
    ExternalMapNotFoundError,
)

router = APIRouter(prefix="/bridge", tags=["5. Reconciliation & Sync Bridge"])

_storage = deps.bronze_storage
_gatekeeper = deps.gatekeeper
_rate_limiter = DomainTokenBucket(default_rate=2.0, default_capacity=5.0)
_circuit_breaker = DomainCircuitBreaker(default_cooldown_seconds=300.0)
_downloader = ResumableStreamDownloader(
    rate_limiter=_rate_limiter,
    circuit_breaker=_circuit_breaker,
)

_diff_engine = DuckDBDiffEngine(
    storage=_storage,
    url_inventory_repo=deps.url_inventory_repo,
)
_sync_orchestrator = SyncOrchestrator(
    gatekeeper=_gatekeeper,
    storage=_storage,
    downloader=_downloader,
    download_tracking=deps.download_tracking,
    url_inventory_repo=deps.url_inventory_repo,
)


@router.post(
    "/reconcile",
    response_model=BridgeReconcileResponse,
    summary="Ejecutar conciliación entre catálogo externo y custodia local",
)
def reconcile_external_map(
    request: BridgeReconcileRequest,
) -> BridgeReconcileResponse:
    """Ejecuta el cruce vectorizado entre el Prospector Externo y Capa Bronze."""
    map_path = (
        Path(request.external_map_path) if request.external_map_path else None
    )
    try:
        report = _diff_engine.compute_diff(
            source_id=request.source_id,
            external_map_path=map_path,
            external_api_url=request.external_api_url,
        )
        msg = (
            f"Conciliación finalizada: {report.total_confirmed} confirmados, "
            f"{report.total_new} nuevos para descarga, "
            f"{report.total_modified} modificados."
        )
        return BridgeReconcileResponse(
            source_id=report.source_id,
            run_id=report.run_id,
            total_external_resources=report.total_external_resources,
            total_confirmed=report.total_confirmed,
            total_new=report.total_new,
            total_modified=report.total_modified,
            summary_message=msg,
            items=report.items,
        )
    except (ExternalCatalogConnectionError, ExternalCatalogResponseError) as err:
        raise HTTPException(status_code=502, detail=str(err)) from err
    except ExternalMapNotFoundError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err
    except ExternalMapCorruptError as err:
        raise HTTPException(status_code=422, detail=str(err)) from err


@router.post(
    "/sync",
    response_model=BridgeSyncResponse,
    summary="Generar y ejecutar plan de sincronización hacia custodia Bronze",
)
def execute_sync_plan(
    request: BridgeSyncRequest,
) -> BridgeSyncResponse:
    """Genera el plan priorizado y ejecuta la transferencia protegida hacia Bronze."""
    map_path = (
        Path(request.external_map_path) if request.external_map_path else None
    )
    try:
        report = _diff_engine.compute_diff(
            source_id=request.source_id,
            external_map_path=map_path,
            external_api_url=request.external_api_url,
        )
        full_plan = _sync_orchestrator.create_plan(report)

        limited_items = full_plan.items[: request.max_downloads]
        plan_to_run = full_plan.model_copy(
            update={"items": limited_items, "total_tasks": len(limited_items)}
        )

        if not request.execute_downloads:
            summary = (
                f"Simulación completada (dry-run) para {plan_to_run.source_id.upper()}: "
                f"{len(limited_items)} documentos identificados para resguardo."
            )
            return BridgeSyncResponse(
                plan_id=plan_to_run.plan_id,
                source_id=plan_to_run.source_id,
                total_tasks=plan_to_run.total_tasks,
                completed_tasks=0,
                failed_tasks=0,
                is_executed=False,
                summary=summary,
            )

        executed_plan = _sync_orchestrator.execute_plan(
            plan_to_run,
            external_api_url=request.external_api_url,
        )

        if executed_plan.completed_tasks > 0 and not deps.is_isolated_test_mode():
            try:
                deps.gold_materializer.consolidate_all_datamarts(
                    _storage.get_artifacts(),
                    target_datamart="mapa_web_52_fuentes",
                    url_inventory_repo=deps.url_inventory_repo,
                )
            except Exception:
                pass

        summary = (
            f"Resguardo completado para {executed_plan.source_id.upper()}: "
            f"{executed_plan.completed_tasks} documentos resguardados exitosamente en Custodia Documental "
            f"y sincronizados con Prospector Externo ({executed_plan.failed_tasks} incidencias de {executed_plan.total_tasks})."
        )

        return BridgeSyncResponse(
            plan_id=executed_plan.plan_id,
            source_id=executed_plan.source_id,
            total_tasks=executed_plan.total_tasks,
            completed_tasks=executed_plan.completed_tasks,
            failed_tasks=executed_plan.failed_tasks,
            is_executed=executed_plan.is_executed,
            summary=summary,
        )
    except (ExternalCatalogConnectionError, ExternalCatalogResponseError) as err:
        raise HTTPException(status_code=502, detail=str(err)) from err
    except ExternalMapNotFoundError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err
    except ExternalMapCorruptError as err:
        raise HTTPException(status_code=422, detail=str(err)) from err


@router.get(
    "/artifacts",
    summary="Listar inventario de artefactos en Capa Bronze",
)
def list_bronze_artifacts(source_id: str | None = None) -> list[dict]:
    """Retorna el inventario físico de archivos custodiados en Bronze."""
    return _storage.get_all_artifacts(source_id=source_id)


@router.get(
    "/circuit-breaker/{domain}",
    response_model=DomainCircuitStatus,
    summary="Consultar estado del disyuntor para un dominio",
)
def get_circuit_breaker_status(domain: str) -> DomainCircuitStatus:
    """Devuelve los metadatos y tiempo restante de enfriamiento."""
    return _circuit_breaker.get_status(domain)


@router.post(
    "/circuit-breaker/{domain}/reset",
    summary="Restablecer manualmente el disyuntor de un dominio",
)
def reset_circuit_breaker(domain: str) -> dict[str, str]:
    """Restablece el disyuntor para el dominio indicado."""
    _circuit_breaker.reset(domain)
    return {
        "status": "RESET",
        "domain": domain,
        "message": f"Disyuntor para '{domain}' restablecido a CLOSED.",
    }
