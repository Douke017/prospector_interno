"""Servicio de Aplicación: Pre-flight Gatekeeper (Objetivos 1 y 5).

Separa explícitamente `mode="DRY_RUN"` (inspección pura sin locks fantasma) de
`mode="ACQUIRE_LOCK"` (reserva atómica con TTL en PostgreSQL `gatekeeper_locks`),
registra auditoría de deduplicación, genera enlaces accionables y calcula
telemetría p50/p95/p99 de latencia en tiempo real.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
import re
import threading
import time
from typing import Any

from prospector_interno.domain.contracts.derivation_spec import (
    DerivationEvaluationRequest,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadStateEvaluationRequest,
)
from prospector_interno.domain.gatekeeper.derivation_filter import (
    AssetDerivationFilter,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.models import (
    IngestionRequest,
    PreFlightEvaluation,
    PreFlightVerdict,
)
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    GatekeeperStatePort,
)


class PreFlightGatekeeper:
    """Orquestador de pre-vuelo para blindar descargas innecesarias y medir ahorros."""

    def __init__(
        self,
        storage: BronzeStoragePort,
        state_registry: GatekeeperStatePort,
        derivation_filter: AssetDerivationFilter | None = None,
        download_state_filter: DownloadStateFilter | None = None,
        gold_partition_checker: Any = None,
        dedup_repo: Any = None,
    ) -> None:
        self._storage = storage
        self._state_registry = state_registry
        self._derivation_filter = derivation_filter
        self._download_state_filter = download_state_filter
        self._gold_checker = gold_partition_checker
        self._dedup_repo = dedup_repo
        self._metrics_lock = threading.Lock()
        self._latencies_ms: deque[float] = deque(maxlen=2000)
        self._verdict_counts: dict[str, int] = {}
        self._bytes_saved_total: int = 0

    def _record_metric(
        self,
        verdict: PreFlightVerdict,
        elapsed_ms: float,
        bytes_saved: int = 0,
    ) -> None:
        with self._metrics_lock:
            self._latencies_ms.append(elapsed_ms)
            key = verdict.value
            self._verdict_counts[key] = self._verdict_counts.get(key, 0) + 1
            self._bytes_saved_total += max(0, int(bytes_saved))

    def get_metrics(self) -> dict[str, Any]:
        """Devuelve telemetría real de latencia p50/p95/p99 y descargas ahorradas."""
        with self._metrics_lock:
            samples = sorted(self._latencies_ms)
            counts = dict(self._verdict_counts)
            bytes_saved = self._bytes_saved_total

        def _pct(sorted_vals: list[float], pct: float) -> float:
            if not sorted_vals:
                return 0.0
            idx = min(len(sorted_vals) - 1, max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))))
            return round(sorted_vals[idx], 3)

        total = sum(counts.values())
        cache_hits = (
            counts.get(PreFlightVerdict.CACHE_HIT_BRONZE.value, 0)
            + counts.get(PreFlightVerdict.CACHE_HIT_GOLD.value, 0)
            + counts.get(PreFlightVerdict.REUSE_DERIVATION.value, 0)
        )
        in_progress = (
            counts.get(PreFlightVerdict.IN_PROGRESS.value, 0)
            + counts.get(PreFlightVerdict.IN_FLIGHT_LOCKED.value, 0)
        )
        return {
            "total_evaluations": total,
            "p50_ms": _pct(samples, 50.0),
            "p95_ms": _pct(samples, 95.0),
            "p99_ms": _pct(samples, 99.0),
            "cache_hits": cache_hits,
            "in_progress_hits": in_progress,
            "proceed_downloads": counts.get(PreFlightVerdict.PROCEED_DOWNLOAD.value, 0),
            "network_calls_saved": cache_hits + in_progress,
            "estimated_bytes_saved": bytes_saved,
            "by_verdict": counts,
        }

    def _record_dedup(
        self,
        *,
        request: IngestionRequest,
        criterion: str,
        reused_asset_id: str | None = None,
        reused_sha256: str | None = None,
        bytes_saved: int = 0,
        detail: str | None = None,
    ) -> None:
        if self._dedup_repo is None:
            return
        try:
            self._dedup_repo.record_audit(
                request_id=request.request_id,
                source_id=request.domain or request.dataset_code,
                canonical_url=request.target_url,
                criterion=criterion,
                reused_asset_id=reused_asset_id,
                reused_sha256=reused_sha256,
                bytes_saved=bytes_saved,
                detail=detail,
            )
        except Exception:
            return

    def evaluate(self, request: IngestionRequest) -> PreFlightEvaluation:
        """Evalúa una solicitud antes de permitir cualquier llamada a la red."""
        t0 = time.perf_counter()
        key = request.idempotency_key
        missing_periods_list: list[str] = []
        ds_lower = request.dataset_code.strip().lower()

        def _finish(
            ev: PreFlightEvaluation,
            *,
            bytes_saved: int = 0,
        ) -> PreFlightEvaluation:
            elapsed = round((time.perf_counter() - t0) * 1000.0, 3)
            self._record_metric(ev.verdict, elapsed, bytes_saved=bytes_saved)
            return ev.model_copy(update={"latency_ms": elapsed})

        # 1. Filtro 2: Derivación y Reutilización sobre Capa Gold
        if not request.force_refresh and self._derivation_filter and request.period:
            try:
                deriv_res = self._derivation_filter.evaluate(
                    DerivationEvaluationRequest(
                        dataset_code=request.dataset_code,
                        period=request.period,
                    )
                )
                if deriv_res.is_derivable:
                    self._record_dedup(
                        request=request,
                        criterion="PERIOD_CONSOLIDATED",
                        reused_asset_id=ds_lower,
                        detail=deriv_res.explanation,
                    )
                    return _finish(
                        PreFlightEvaluation(
                            verdict=PreFlightVerdict.REUSE_DERIVATION,
                            idempotency_key=key,
                            reason=deriv_res.explanation,
                            derivation_query=deriv_res.precompiled_sql,
                            lock_acquired=False,
                            dedup_criterion="PERIOD_CONSOLIDATED",
                            action_links={
                                "parquet_export_url": f"/api/v1/analytics/export/parquet?datamart={ds_lower}",
                                "csv_export_url": f"/api/v1/analytics/export/csv?datamart={ds_lower}",
                                "sql_console_endpoint": "/api/v1/analytics/sql",
                            },
                        )
                    )
                missing_periods_list = deriv_res.missing_periods
            except ValueError:
                pass

        # 1.5. Verificación en Repositorio Consolidado Gold
        if (
            not request.force_refresh
            and self._gold_checker is not None
            and request.period
            and (
                ds_lower.startswith("mart_")
                or ds_lower
                in (
                    "catalogo_series_files",
                    "historial_descargas_bruto",
                    "reportes_conversiones",
                    "bases_datos_cubos",
                    "mapa_web_52_fuentes",
                )
            )
            and re.fullmatch(r"\d{4}-\d{2}", request.period.strip())
        ):
            g_yr, g_mo = request.period.strip().split("-")
            if self._gold_checker.has_partition(ds_lower, int(g_yr), int(g_mo)):
                self._record_dedup(
                    request=request,
                    criterion="PERIOD_CONSOLIDATED",
                    reused_asset_id=f"{ds_lower}:{request.period}",
                    detail=f"Partición {request.period} ya consolidada en {ds_lower}.",
                )
                return _finish(
                    PreFlightEvaluation(
                        verdict=PreFlightVerdict.CACHE_HIT_GOLD,
                        idempotency_key=key,
                        reason=(
                            f"Corte {request.period} ya consolidado "
                            f"y disponible en el repositorio '{ds_lower}'."
                        ),
                        lock_acquired=False,
                        dedup_criterion="PERIOD_CONSOLIDATED",
                        action_links={
                            "parquet_export_url": f"/api/v1/analytics/export/parquet?datamart={ds_lower}",
                            "csv_export_url": f"/api/v1/analytics/export/csv?datamart={ds_lower}",
                        },
                    )
                )

        # 2. Filtro 1: Verificación de Cache Hit Bronze por SHA-256, URL, Alias/ETag o Periodo
        if not request.force_refresh:
            dedup_criterion: str | None = None
            cached = None
            if request.checksum_sha256:
                cached = self._storage.find_by_checksum(request.checksum_sha256)
                if cached is not None:
                    dedup_criterion = "SHA256_EXACT"
            if cached is None:
                cached = self._storage.find_by_url(request.target_url)
                if cached is not None:
                    dedup_criterion = "URL_CANONICAL"
            if cached is None and self._dedup_repo is not None:
                alias = self._dedup_repo.find_alias_by_url(request.target_url)
                if alias is not None:
                    cached = self._storage.find_by_checksum(alias.target_sha256)
                    if cached is not None:
                        dedup_criterion = "SHA256_EXACT"
                elif request.etag:
                    etag_alias = self._dedup_repo.find_alias_by_etag(request.etag)
                    if etag_alias is not None:
                        cached = self._storage.find_by_checksum(etag_alias.target_sha256)
                        if cached is not None:
                            dedup_criterion = "ETAG_MATCH"
            if cached is None and request.period and request.period != "LATEST":
                cached = self._storage.find_by_period(
                    domain=request.domain,
                    dataset_code=request.dataset_code,
                    period=request.period,
                )
                if cached is not None:
                    dedup_criterion = "PERIOD_CONSOLIDATED"
            if cached is not None:
                self._record_dedup(
                    request=request,
                    criterion=dedup_criterion or "URL_CANONICAL",
                    reused_asset_id=cached.artifact_id,
                    reused_sha256=cached.sha256_hash,
                    bytes_saved=cached.size_bytes,
                    detail="Artefacto encontrado en custodia Bronze verificada.",
                )
                return _finish(
                    PreFlightEvaluation(
                        verdict=PreFlightVerdict.CACHE_HIT_BRONZE,
                        idempotency_key=key,
                        reason="Artefacto encontrado en almacenamiento local validado.",
                        cached_artifact=cached,
                        lock_acquired=False,
                        missing_periods=missing_periods_list,
                        dedup_criterion=dedup_criterion,
                        action_links={
                            "raw_download_url": f"/api/v1/catalog/assets/{cached.artifact_id}/download",
                            "profile_url": f"/api/v1/catalog/assets/{cached.artifact_id}/profile",
                        },
                    ),
                    bytes_saved=cached.size_bytes,
                )

        # 3. Filtro 3: Monitoreo del Ciclo de Ingesta y Suscripción Concurrente
        if not request.force_refresh and self._download_state_filter:
            lifecycle = self._download_state_filter.evaluate(
                DownloadStateEvaluationRequest(
                    canonical_url=request.target_url,
                    request_id=request.request_id,
                    source_id=request.domain,
                )
            )
            if lifecycle.is_in_progress:
                return _finish(
                    PreFlightEvaluation(
                        verdict=PreFlightVerdict.IN_PROGRESS,
                        idempotency_key=key,
                        reason=lifecycle.explanation,
                        in_flight_task_id=lifecycle.task_id,
                        subscribers_count=lifecycle.subscribers_count,
                        lock_acquired=False,
                        missing_periods=missing_periods_list,
                        action_links={
                            "tracking_id": lifecycle.download_id,
                            "tracking_url": (
                                f"/api/v1/downloads/tracking/{lifecycle.download_id}"
                                if lifecycle.download_id
                                else None
                            ),
                            "stream_url": "/api/v1/downloads/stream",
                        },
                    )
                )
            if lifecycle.verdict_status == "COMPLETED":
                return _finish(
                    PreFlightEvaluation(
                        verdict=PreFlightVerdict.CACHE_HIT_BRONZE,
                        idempotency_key=key,
                        reason=lifecycle.explanation,
                        in_flight_task_id=lifecycle.task_id,
                        lock_acquired=False,
                        missing_periods=missing_periods_list,
                        dedup_criterion="URL_CANONICAL",
                        action_links={
                            "tracking_id": lifecycle.download_id,
                            "tracking_url": (
                                f"/api/v1/downloads/tracking/{lifecycle.download_id}"
                                if lifecycle.download_id
                                else None
                            ),
                        },
                    )
                )

        # 4. Modo DRY_RUN: inspeccionar si existe un lock activo SIN adquirir ninguno nuevo
        if request.mode == "DRY_RUN":
            in_flight_id = self._state_registry.get_in_flight_task(key)
            if in_flight_id is not None:
                return _finish(
                    PreFlightEvaluation(
                        verdict=PreFlightVerdict.IN_FLIGHT_LOCKED,
                        idempotency_key=key,
                        reason=(
                            f"Descarga idéntica en progreso por la tarea '{in_flight_id}'. "
                            "Inspección en modo DRY_RUN sin alterar bloqueos."
                        ),
                        in_flight_task_id=in_flight_id,
                        lock_acquired=False,
                        missing_periods=missing_periods_list,
                    )
                )
            return _finish(
                PreFlightEvaluation(
                    verdict=PreFlightVerdict.PROCEED_DOWNLOAD,
                    idempotency_key=key,
                    reason=(
                        "Recurso no encontrado localmente. Inspección DRY_RUN completada "
                        "(sin bloqueo retenido); listo para encolar descarga real."
                    ),
                    lock_acquired=False,
                    missing_periods=missing_periods_list,
                    action_links={
                        "enqueue_url": "/api/v1/downloads/queue",
                    },
                )
            )

        # 5. Modo ACQUIRE_LOCK: reserva atómica con TTL
        lock_acquired = self._state_registry.try_acquire_lock(
            idempotency_key=key,
            task_id=request.request_id,
            ttl_seconds=request.lock_ttl_seconds,
        )
        if not lock_acquired:
            in_flight_id = self._state_registry.get_in_flight_task(key)
            return _finish(
                PreFlightEvaluation(
                    verdict=PreFlightVerdict.IN_FLIGHT_LOCKED,
                    idempotency_key=key,
                    reason=(
                        f"Descarga idéntica en progreso por la tarea '{in_flight_id}'. "
                        "Colisión prevenida."
                    ),
                    in_flight_task_id=in_flight_id,
                    lock_acquired=False,
                    missing_periods=missing_periods_list,
                )
            )

        return _finish(
            PreFlightEvaluation(
                verdict=PreFlightVerdict.PROCEED_DOWNLOAD,
                idempotency_key=key,
                reason="Lock en vuelo concedido con TTL. Autorizada descarga de red.",
                lock_acquired=True,
                missing_periods=missing_periods_list,
                action_links={
                    "enqueue_url": "/api/v1/downloads/queue",
                },
            )
        )

    def evaluate_batch(self, requests: list[IngestionRequest]) -> list[PreFlightEvaluation]:
        """Evalúa hasta 500 solicitudes en lote manteniendo latencia mínima."""
        return [self.evaluate(req) for req in requests]

    def release_task(self, request: IngestionRequest) -> None:
        """Libera el lock de la tarea en vuelo."""
        self._state_registry.release_lock(
            idempotency_key=request.idempotency_key,
            task_id=request.request_id,
        )

    @contextmanager
    def execution_lease(self, request: IngestionRequest):
        """Context manager para gestionar el ciclo de vida del lease."""
        evaluation = self.evaluate(request)
        try:
            yield evaluation
        finally:
            if evaluation.lock_acquired:
                self.release_task(request)
