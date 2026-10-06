"""Servicio de Aplicación: Pre-flight Gatekeeper.

Implementa HUI-01 (Detección de Cache Hit) y HUI-04 (Deduplicación Idempotente
y Locks en Vuelo) para evitar tráfico redundante y colisiones concurrentes.
"""

from contextlib import contextmanager

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
    ) -> None:
        self._storage = storage
        self._state_registry = state_registry

    def evaluate(self, request: IngestionRequest) -> PreFlightEvaluation:
        """Evalúa una solicitud antes de permitir cualquier llamada a la red.

        Reglas de Decisión:
        1. Si `force_refresh` es False, consulta si el artefacto ya existe
           íntegramente en la Capa Bronze/Gold para el periodo solicitado.
        2. Si no existe en almacenamiento, intenta adquirir el lock en vuelo
           atómico para evitar que dos corrutinas descarguen lo mismo en paralelo.
        3. Si el lock ya lo tiene otro proceso, emite IN_FLIGHT_LOCKED.
        4. Si el lock es concedido, emite PROCEED_DOWNLOAD.
        """
        key = request.idempotency_key

        # 1. Verificación de Cache Hit (HUI-01)
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
                )

        # 2. Verificación de colisión concurrente en vuelo (HUI-04)
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
            )

        return PreFlightEvaluation(
            verdict=PreFlightVerdict.PROCEED_DOWNLOAD,
            idempotency_key=key,
            reason="Lock en vuelo concedido. Autorizada descarga de red.",
            lock_acquired=True,
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
