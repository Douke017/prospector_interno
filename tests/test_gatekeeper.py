"""Pruebas unitarias para HUI-01 y HUI-04: Pre-flight Gatekeeper."""

import io

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.models import (
    IngestionRequest,
    PreFlightVerdict,
)


def test_gatekeeper_proceed_download_when_empty(
    gatekeeper: PreFlightGatekeeper,
    sample_request: IngestionRequest,
) -> None:
    """Verifica que si no hay cache ni lock previo, se autoriza la descarga."""
    eval_result = gatekeeper.evaluate(sample_request)

    assert eval_result.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert eval_result.lock_acquired is True
    assert eval_result.cached_artifact is None

    # Liberar el lock
    gatekeeper.release_task(sample_request)


def test_gatekeeper_prevents_in_flight_concurrency_collision(
    gatekeeper: PreFlightGatekeeper,
    sample_request: IngestionRequest,
) -> None:
    """Verifica HUI-04: Dos solicitudes idénticas concurrentes no duplican descarga."""
    # Primer worker adquiere el lock
    first_eval = gatekeeper.evaluate(sample_request)
    assert first_eval.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert first_eval.lock_acquired is True

    # Segundo worker con la misma clave solicita descarga simultáneamente
    concurrent_request = sample_request.model_copy(
        update={"request_id": "req-test-002-concurrent"}
    )
    second_eval = gatekeeper.evaluate(concurrent_request)

    assert second_eval.verdict == PreFlightVerdict.IN_FLIGHT_LOCKED
    assert second_eval.lock_acquired is False
    assert second_eval.in_flight_task_id == "req-test-001"
    assert "Colisión prevenida" in second_eval.reason

    # Al liberar el primer worker, el segundo o futuros pueden avanzar
    gatekeeper.release_task(sample_request)

    third_eval = gatekeeper.evaluate(concurrent_request)
    assert third_eval.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert third_eval.lock_acquired is True
    gatekeeper.release_task(concurrent_request)


def test_gatekeeper_cache_hit_prevents_network_call(
    gatekeeper: PreFlightGatekeeper,
    bronze_storage: LocalBronzeStorage,
    sample_request: IngestionRequest,
) -> None:
    """Verifica HUI-01: Si el archivo ya existe en almacenamiento,
    se retorna CACHE_HIT.
    """
    # Precargar el archivo en almacenamiento local
    stream = io.BytesIO(b"DATO_PRECARGADO")
    artifact = bronze_storage.store_stream(
        domain=sample_request.domain,
        dataset_code=sample_request.dataset_code,
        period=sample_request.period,
        stream=stream,
    )

    # Evaluar con el Gatekeeper
    eval_result = gatekeeper.evaluate(sample_request)

    assert eval_result.verdict == PreFlightVerdict.CACHE_HIT_BRONZE
    assert eval_result.lock_acquired is False
    assert eval_result.cached_artifact is not None
    assert eval_result.cached_artifact.sha256_hash == artifact.sha256_hash


def test_gatekeeper_force_refresh_bypasses_cache_hit(
    gatekeeper: PreFlightGatekeeper,
    bronze_storage: LocalBronzeStorage,
    sample_request: IngestionRequest,
) -> None:
    """Verifica que force_refresh=True ignora el caché existente y adquiere lock."""
    # Precargar el archivo
    stream = io.BytesIO(b"DATO_ANTIGUO")
    bronze_storage.store_stream(
        domain=sample_request.domain,
        dataset_code=sample_request.dataset_code,
        period=sample_request.period,
        stream=stream,
    )

    # Solicitud con forzado de refresco
    forced_request = sample_request.model_copy(update={"force_refresh": True})
    eval_result = gatekeeper.evaluate(forced_request)

    assert eval_result.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert eval_result.lock_acquired is True

    gatekeeper.release_task(forced_request)


def test_gatekeeper_execution_lease_context_manager(
    gatekeeper: PreFlightGatekeeper,
    sample_request: IngestionRequest,
) -> None:
    """Verifica que el context manager libera el lock automáticamente."""
    with gatekeeper.execution_lease(sample_request) as lease:
        assert lease.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
        assert lease.lock_acquired is True

    # Inmediatamente después del bloque with, el lock debe haber sido liberado
    second_eval = gatekeeper.evaluate(sample_request)
    assert second_eval.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert second_eval.lock_acquired is True
    gatekeeper.release_task(sample_request)
