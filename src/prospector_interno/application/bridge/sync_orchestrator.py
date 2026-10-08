"""Orquestador de Planes de Sincronización y Adquisición Masiva (HUI-19).

Genera el SyncPlan priorizado y ejecuta la ingesta de recursos hacia la
Capa Bronze coordinando con el Gatekeeper y el Downloader.
"""

from datetime import datetime, timezone
from urllib.parse import urlparse
import uuid

from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.downloader_spec import (
    OffPeakEvaluationRequest,
)
from prospector_interno.domain.models import (
    IngestionRequest,
    PreFlightVerdict,
    ReconciliationDiffReport,
    ResourceChangeCategory,
    SyncPlan,
    SyncPlanItem,
    SyncPriority,
)
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    HttpDownloaderPort,
    OffPeakWindowGuardPort,
    SyncOrchestratorPort,
)


class SyncOrchestrator(SyncOrchestratorPort):
    """Generador y ejecutor de planes de sincronización priorizados."""

    def __init__(
        self,
        gatekeeper: PreFlightGatekeeper,
        storage: BronzeStoragePort,
        downloader: HttpDownloaderPort,
        window_guard: OffPeakWindowGuardPort | None = None,
    ) -> None:
        self._gatekeeper = gatekeeper
        self._storage = storage
        self._downloader = downloader
        self._window_guard = window_guard

    def create_plan(
        self,
        diff_report: ReconciliationDiffReport,
    ) -> SyncPlan:

        """Construye un SyncPlan ordenando descargas por prioridad temporal."""
        plan_id = f"plan_{uuid.uuid4().hex[:8]}"
        items_to_sync: list[SyncPlanItem] = []

        # Filtrar solo recursos pendientes de adquisición (NEW o MODIFIED)
        candidates = [
            item for item in diff_report.items
            if item.category in (
                ResourceChangeCategory.NEW,
                ResourceChangeCategory.MODIFIED,
            )
        ]

        current_year = datetime.now(timezone.utc).year

        for idx, item in enumerate(candidates, start=1):
            parsed = urlparse(item.url)
            domain = parsed.netloc or f"{item.source_id}.org.bo"
            period = item.period_label or "LATEST"

            # Asignación de prioridad por frescura temporal
            if str(current_year) in period:
                priority = SyncPriority.CRITICAL
            elif str(current_year - 1) in period:
                priority = SyncPriority.HIGH
            else:
                priority = SyncPriority.NORMAL

            sync_item = SyncPlanItem(
                item_id=f"{plan_id}_item_{idx:03d}",
                resource_key=item.resource_key,
                url=item.url,
                source_id=item.source_id,
                domain=domain,
                period=period,
                priority=priority,
                status="PENDING",
            )
            items_to_sync.append(sync_item)

        # Ordenar: CRITICAL primero, luego HIGH, luego NORMAL
        priority_weights = {
            SyncPriority.CRITICAL: 1,
            SyncPriority.HIGH: 2,
            SyncPriority.NORMAL: 3,
            SyncPriority.LOW: 4,
        }
        items_to_sync.sort(key=lambda x: priority_weights[x.priority])

        return SyncPlan(
            plan_id=plan_id,
            source_id=diff_report.source_id,
            created_at=datetime.now(timezone.utc),
            total_tasks=len(items_to_sync),
            items=items_to_sync,
            completed_tasks=0,
            failed_tasks=0,
            is_executed=False,
        )

    def execute_plan(self, plan: SyncPlan, force_now: bool = False) -> SyncPlan:
        """Ejecuta ordenadamente las tareas del plan depositándolas en Bronze."""
        if self._window_guard is not None:
            eval_req = OffPeakEvaluationRequest(
                batch_size=len(plan.items),
                force_now=force_now,
                source_id=plan.source_id,
            )
            eval_res = self._window_guard.evaluate_batch(eval_req)
            if not eval_res.is_permitted:
                paused_items = [
                    item.model_copy(
                        update={
                            "status": "PAUSED_WAITING_OFFPEAK",
                            "error_message": eval_res.reason,
                        }
                    )
                    for item in plan.items
                ]
                return plan.model_copy(
                    update={
                        "items": paused_items,
                        "completed_tasks": 0,
                        "failed_tasks": 0,
                        "is_executed": False,
                    }
                )

        executed_items: list[SyncPlanItem] = []
        completed_count = 0
        failed_count = 0

        for item in plan.items:

            req = IngestionRequest(
                request_id=item.item_id,
                dataset_code=f"DS_BO_{item.source_id.upper()}",
                domain=item.domain,
                target_url=item.url,
                period=item.period,
                force_refresh=False,
            )

            # 1. Validación de Pre-vuelo (Gatekeeper HUI-01 / HUI-04)
            evaluation = self._gatekeeper.evaluate(req)

            if evaluation.verdict == PreFlightVerdict.CACHE_HIT_BRONZE:
                executed_items.append(
                    item.model_copy(
                        update={
                            "status": "SKIPPED_CACHE_HIT",
                            "downloaded_artifact": evaluation.cached_artifact,
                        }
                    )
                )
                completed_count += 1
                continue

            if evaluation.verdict == PreFlightVerdict.IN_FLIGHT_LOCKED:
                executed_items.append(
                    item.model_copy(
                        update={
                            "status": "IN_FLIGHT_LOCKED",
                            "error_message": (
                                f"Bloqueo activo por {evaluation.in_flight_task_id}"
                            ),
                        }
                    )
                )
                failed_count += 1
                continue

            # 2. Descarga física protegida
            try:
                stream, status_code, content_type = (
                    self._downloader.download_to_stream(
                        url=item.url,
                        domain=item.domain,
                    )
                )

                # 3. Almacenamiento inmutable en Capa Bronze (HUI-11)
                artifact = self._storage.store_stream(
                    domain=item.domain,
                    dataset_code=f"DS_BO_{item.source_id.upper()}",
                    period=item.period,
                    stream=stream,
                    expected_checksum=item.expected_hash,
                    content_type=content_type,
                    url=item.url,
                    resource_key=item.resource_key,
                )

                executed_items.append(
                    item.model_copy(
                        update={
                            "status": "COMPLETED",
                            "downloaded_artifact": artifact,
                        }
                    )
                )
                completed_count += 1

            except Exception as err:
                executed_items.append(
                    item.model_copy(
                        update={
                            "status": "FAILED",
                            "error_message": str(err),
                        }
                    )
                )
                failed_count += 1

            finally:
                if evaluation.lock_acquired:
                    self._gatekeeper.release_task(req)

        return plan.model_copy(
            update={
                "items": executed_items,
                "completed_tasks": completed_count,
                "failed_tasks": failed_count,
                "is_executed": True,
            }
        )
