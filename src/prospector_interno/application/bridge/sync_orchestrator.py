"""Orquestador de Planes de Sincronización y Adquisición Masiva.

Genera el SyncPlan priorizado y ejecuta la ingesta de recursos hacia la
Capa Bronze coordinando con el Gatekeeper, el seguimiento en vivo de cola
(`download_tracking`) y el inventario relacional (`url_inventory_map`).
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any
from urllib.parse import urlparse
import uuid

import httpx

from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
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
    DownloadTrackingPort,
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
        download_tracking: DownloadTrackingPort | None = None,
        url_inventory_repo: Any = None,
    ) -> None:
        self._gatekeeper = gatekeeper
        self._storage = storage
        self._downloader = downloader
        self._window_guard = window_guard
        self._download_tracking = download_tracking
        self._url_inventory_repo = url_inventory_repo

    @staticmethod
    def _notify_external_resource_status(
        resource_key: str,
        processing_status: str,
        external_api_url: str | None = None,
        error_detail: str | None = None,
    ) -> None:
        """Sincroniza el estado de procesamiento hacia el Prospector Externo (best-effort)."""
        if not resource_key:
            return
        base = (
            external_api_url
            or os.getenv("PROSPECTOR_EXTERNO_URL", "http://127.0.0.1:8000")
        ).rstrip("/")
        try:
            payload: dict[str, Any] = {"processing_status": processing_status}
            if error_detail:
                payload["error_detail"] = error_detail[:250]
            httpx.patch(
                f"{base}/catalog/resources/{resource_key}/status",
                json=payload,
                timeout=1.5,
            )
        except Exception:
            pass

    def create_plan(
        self,
        diff_report: ReconciliationDiffReport,
    ) -> SyncPlan:
        """Construye un SyncPlan ordenando descargas por prioridad temporal."""
        plan_id = f"plan_{uuid.uuid4().hex[:8]}"
        items_to_sync: list[SyncPlanItem] = []

        candidates = [
            item
            for item in diff_report.items
            if item.category
            in (
                ResourceChangeCategory.NEW,
                ResourceChangeCategory.MODIFIED,
            )
        ]

        current_year = datetime.now(timezone.utc).year

        for idx, item in enumerate(candidates, start=1):
            parsed = urlparse(item.url)
            domain = parsed.netloc or f"{item.source_id}.org.bo"
            period = item.period_label or "LATEST"

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

    def execute_plan(
        self,
        plan: SyncPlan,
        force_now: bool = False,
        external_api_url: str | None = None,
    ) -> SyncPlan:
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

            evaluation = self._gatekeeper.evaluate(req)

            if evaluation.verdict in (
                PreFlightVerdict.CACHE_HIT_BRONZE,
                PreFlightVerdict.REUSE_DERIVATION,
                PreFlightVerdict.CACHE_HIT_GOLD,
            ):
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

            if evaluation.verdict in (
                PreFlightVerdict.IN_FLIGHT_LOCKED,
                PreFlightVerdict.IN_PROGRESS,
            ):
                executed_items.append(
                    item.model_copy(
                        update={
                            "status": "IN_FLIGHT_LOCKED",
                            "error_message": (
                                f"Transferencia concurrente activa en {evaluation.in_flight_task_id}"
                            ),
                        }
                    )
                )
                failed_count += 1
                continue

            dl_id = f"dl_{item.item_id}"
            if self._download_tracking is not None:
                try:
                    self._download_tracking.record_download(
                        DownloadRecord(
                            download_id=dl_id,
                            source_id=item.source_id.upper(),
                            canonical_url=item.url,
                            status=DownloadLifecycleStatus.DOWNLOADING,
                            task_id=plan.plan_id,
                            started_at=datetime.now(timezone.utc),
                        )
                    )
                except Exception:
                    pass

            if external_api_url:
                self._notify_external_resource_status(
                    item.resource_key,
                    "EN_PROCESO",
                    external_api_url=external_api_url,
                )

            try:
                stream, status_code, content_type = (
                    self._downloader.download_to_stream(
                        url=item.url,
                        domain=item.domain,
                    )
                )

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

                if self._download_tracking is not None:
                    try:
                        self._download_tracking.update_status(
                            download_id=dl_id,
                            status=DownloadLifecycleStatus.COMPLETED,
                            bytes_downloaded=artifact.size_bytes,
                            total_bytes=artifact.size_bytes,
                            local_path=str(artifact.file_path),
                            sha256_checksum=artifact.sha256_hash,
                        )
                    except Exception:
                        pass

                if self._url_inventory_repo is not None:
                    try:
                        self._url_inventory_repo.upsert_downloaded_url(
                            source_id=item.source_id,
                            canonical_url=item.url,
                            sha256_hash=artifact.sha256_hash,
                            storage_path=str(artifact.file_path),
                        )
                    except Exception:
                        pass

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
                if self._download_tracking is not None:
                    try:
                        self._download_tracking.update_status(
                            download_id=dl_id,
                            status=DownloadLifecycleStatus.FAILED,
                            error_code=type(err).__name__,
                        )
                    except Exception:
                        pass

                if external_api_url:
                    self._notify_external_resource_status(
                        item.resource_key,
                        "ERROR",
                        external_api_url=external_api_url,
                        error_detail=str(err),
                    )

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
