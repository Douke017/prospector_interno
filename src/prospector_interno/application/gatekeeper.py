"""Servicio de Aplicación: Pre-flight Gatekeeper.

Implementa HUI-01 (Detección de Cache Hit) y HUI-04 (Deduplicación Idempotente
y Locks en Vuelo) para evitar tráfico redundante y colisiones concurrentes.
"""

from contextlib import contextmanager

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
    """Orquestador de pre-vuelo para blindar descargas innecesarias."""

    def __init__(
        self,
        storage: BronzeStoragePort,
        state_registry: GatekeeperStatePort,
        derivation_filter: AssetDerivationFilter | None = None,
        download_state_filter: DownloadStateFilter | None = None,
    ) -> None:
        self._storage = storage
        self._state_registry = state_registry
        self._derivation_filter = derivation_filter
        self._download_state_filter = download_state_filter

    def evaluate(self, request: IngestionRequest) -> PreFlightEvaluation:
        """Evalúa una solicitud antes de permitir cualquier llamada a la red.

        Reglas de Decisión:
        1. Evalúa si la solicitud puede derivarse de tablas en Gold (HUI-02).
        2. Si no es derivable, consulta si ya existe en Bronze (HUI-01).
        3. Si no existe en almacenamiento, adquiere lock en vuelo (HUI-04).
        """

        key = request.idempotency_key
        missing_periods_list: list[str] = []

        # 1. Filtro 2: Derivación y Reutilización sobre Capa Gold (HUI-02)
        if not request.force_refresh and self._derivation_filter and request.period:
            try:
                deriv_res = self._derivation_filter.evaluate(
                    DerivationEvaluationRequest(
                        dataset_code=request.dataset_code,
                        period=request.period,
                    )
                )
                if deriv_res.is_derivable:
                    return PreFlightEvaluation(
                        verdict=PreFlightVerdict.REUSE_DERIVATION,
                        idempotency_key=key,
                        reason=deriv_res.explanation,
                        derivation_query=deriv_res.precompiled_sql,
                        lock_acquired=False,
                    )
                missing_periods_list = deriv_res.missing_periods
            except ValueError:
                # El formato de periodo no es macro-temporal estructurado; seguir
                pass

        # 2. Filtro 1: Verificación de Cache Hit Bronze (HUI-01)
        if not request.force_refresh:
            cached = self._storage.find_by_url(request.target_url)
            if cached is None and request.period and request.period != "LATEST":
                cached = self._storage.find_by_period(
                    domain=request.domain,
                    dataset_code=request.dataset_code,
                    period=request.period,
                )
            if cached is not None:
                return PreFlightEvaluation(
                    verdict=PreFlightVerdict.CACHE_HIT_BRONZE,
                    idempotency_key=key,
                    reason="Artefacto encontrado en almacenamiento local validado.",
                    cached_artifact=cached,
                    lock_acquired=False,
                    missing_periods=missing_periods_list,
                )

        # 3. Filtro 3: Monitoreo del Ciclo de Ingesta y Suscripción Concurrente (HUI-03)
        if not request.force_refresh and self._download_state_filter:
            lifecycle = self._download_state_filter.evaluate(
                DownloadStateEvaluationRequest(
                    canonical_url=request.target_url,
                    request_id=request.request_id,
                    source_id=request.domain,
                )
            )
            if lifecycle.is_in_progress:
                return PreFlightEvaluation(
                    verdict=PreFlightVerdict.IN_PROGRESS,
                    idempotency_key=key,
                    reason=lifecycle.explanation,
                    in_flight_task_id=lifecycle.task_id,
                    subscribers_count=lifecycle.subscribers_count,
                    lock_acquired=False,
                    missing_periods=missing_periods_list,
                )

        # 4. Verificación de colisión concurrente en vuelo (HUI-04)
        lock_acquired = self._state_registry.try_acquire_lock(
            idempotency_key=key,
            task_id=request.request_id,
        )

        if not lock_acquired:
            in_flight_id = self._state_registry.get_in_flight_task(key)
            return PreFlightEvaluation(
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

        return PreFlightEvaluation(
            verdict=PreFlightVerdict.PROCEED_DOWNLOAD,
            idempotency_key=key,
            reason="Lock en vuelo concedido. Autorizada descarga de red.",
            lock_acquired=True,
            missing_periods=missing_periods_list,
        )

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
