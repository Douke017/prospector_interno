"""Router para el Reconciliation & Sync Bridge (HUI-18 y HUI-19).

Permite conciliar el inventario del Prospector Interno con los descubrimientos
del Prospector Externo usando DuckDB y ejecutar el SyncPlan hacia Bronze.
"""

from pathlib import Path
from fastapi import APIRouter, HTTPException

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
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.bridge.diff_engine import DuckDBDiffEngine
from prospector_interno.application.bridge.sync_orchestrator import (
    SyncOrchestrator,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
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

# Instancias compartidas del Bridge
_storage = LocalBronzeStorage(base_path=Path("data/bronze"))
_state = InMemoryGatekeeperState()
_gatekeeper = PreFlightGatekeeper(storage=_storage, state_registry=_state)
_rate_limiter = DomainTokenBucket(default_rate=2.0, default_capacity=5.0)
_circuit_breaker = DomainCircuitBreaker(default_cooldown_seconds=300.0)
_downloader = ResumableStreamDownloader(
    rate_limiter=_rate_limiter,
    circuit_breaker=_circuit_breaker,
)

_diff_engine = DuckDBDiffEngine(storage=_storage)
_sync_orchestrator = SyncOrchestrator(
    gatekeeper=_gatekeeper,
    storage=_storage,
    downloader=_downloader,
)


@router.post(
    "/reconcile",
    response_model=BridgeReconcileResponse,
    summary="Ejecutar Diff con DuckDB (HUI-18)",
    description=(
        "Compara las URLs descubiertas en el mapa del Prospector Externo "
        "contra el almacenamiento local en Capa Bronze y categoriza discrepancias."
    ),
)
def reconcile_external_map(
    request: BridgeReconcileRequest,
) -> BridgeReconcileResponse:
    """Ejecuta el diff vectorizado entre el JSON externo y Capa Bronze."""
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
    summary="Generar y ejecutar SyncPlan hacia Bronze (HUI-19)",
    description=(
        "Crea el plan de sincronización priorizado y descarga ordenadamente "
        "los recursos faltantes pasando por el Gatekeeper hacia Capa Bronze."
    ),
)
def execute_sync_plan(
    request: BridgeSyncRequest,
) -> BridgeSyncResponse:
    """Genera el SyncPlan priorizado y ejecuta la ingesta masiva protegida."""
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

        # Acotar tareas al límite solicitado para control de ejecución
        limited_items = full_plan.items[:request.max_downloads]
        plan_to_run = full_plan.model_copy(
            update={"items": limited_items, "total_tasks": len(limited_items)}
        )

        if not request.execute_downloads:
            summary = (
                f"Plan {plan_to_run.plan_id} generado en simulación (dry-run): "
                f"{len(limited_items)} tareas priorizadas listas para descarga."
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

        executed_plan = _sync_orchestrator.execute_plan(plan_to_run)

        summary = (
            f"Plan {executed_plan.plan_id} completado: "
            f"{executed_plan.completed_tasks} exitosos, "
            f"{executed_plan.failed_tasks} fallidos de {executed_plan.total_tasks}."
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
    description=(
        "Devuelve la lista física de archivos resguardados en Capa Bronze "
        "con sus firmas criptográficas SHA-256."
    ),
)
def list_bronze_artifacts(source_id: str | None = None) -> list[dict]:
    """Retorna el inventario físico de archivos custodiados en Bronze."""
    return _storage.get_all_artifacts(source_id=source_id)


@router.get(
    "/circuit-breaker/{domain}",
    response_model=DomainCircuitStatus,
    summary="Consultar estado del disyuntor para un dominio (HUI-09)",
    description=(
        "Permite observar si un dominio está CLOSED (operativo), "
        "OPEN (en enfriamiento tras 429/503) o HALF_OPEN."
    ),
)
def get_circuit_breaker_status(domain: str) -> DomainCircuitStatus:
    """Devuelve los metadatos y tiempo restante de enfriamiento."""
    return _circuit_breaker.get_status(domain)


@router.post(
    "/circuit-breaker/{domain}/reset",
    summary="Restablecer manualmente el disyuntor de un dominio (HUI-09)",
    description="Fuerza el retorno del circuito a CLOSED para un portal.",
)
def reset_circuit_breaker(domain: str) -> dict[str, str]:
    """Restablece el disyuntor para el dominio indicado."""
    _circuit_breaker.reset(domain)
    return {
        "status": "RESET",
        "domain": domain,
        "message": f"Disyuntor para '{domain}' restablecido a CLOSED.",
    }
