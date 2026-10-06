"""Pruebas unitarias para el Disyuntor Adaptativo por Dominio (HUI-09)."""

import time
from unittest.mock import patch
import pytest

from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
    parse_retry_after,
)
from prospector_interno.domain.contracts.downloader_spec import CircuitState
from prospector_interno.domain.exceptions import CircuitOpenError


def test_initial_state_is_closed_and_executable() -> None:
    """Verifica que el estado inicial de cualquier dominio sea CLOSED."""
    cb = DomainCircuitBreaker()
    assert cb.can_execute("asfi.gob.bo") is True
    status = cb.get_status("asfi.gob.bo")
    assert status.state == CircuitState.CLOSED
    assert status.failure_count == 0


def test_trip_on_429_with_retry_after_header() -> None:
    """Verifica que un HTTP 429 active el disyuntor y respete Retry-After."""
    cb = DomainCircuitBreaker(default_cooldown_seconds=300.0)

    cb.record_failure("asfi.gob.bo", status_code=429, retry_after_seconds=120.0)

    assert cb.can_execute("asfi.gob.bo") is False
    status = cb.get_status("asfi.gob.bo")
    assert status.state == CircuitState.OPEN
    assert status.failure_count == 1
    assert status.last_status_code == 429
    assert 118.0 <= status.cooldown_seconds <= 120.0

    with pytest.raises(CircuitOpenError) as exc_info:
        cb.verify_can_execute("asfi.gob.bo")

    assert exc_info.value.domain == "asfi.gob.bo"
    assert exc_info.value.retry_after_seconds > 0


def test_trip_on_503_uses_default_cooldown() -> None:
    """Verifica que un HTTP 503 use el tiempo de enfriamiento por defecto."""
    cb = DomainCircuitBreaker(default_cooldown_seconds=60.0)

    cb.record_failure("bcb.gob.bo", status_code=503)

    assert cb.can_execute("bcb.gob.bo") is False
    status = cb.get_status("bcb.gob.bo")
    assert status.state == CircuitState.OPEN
    assert status.failure_count == 1
    assert 58.0 <= status.cooldown_seconds <= 60.0


def test_domain_isolation_does_not_affect_other_domains() -> None:
    """Verifica que la apertura de un dominio no interrumpa a otros portales."""
    cb = DomainCircuitBreaker()

    cb.record_failure("asfi.gob.bo", status_code=429, retry_after_seconds=300.0)

    assert cb.can_execute("asfi.gob.bo") is False
    assert cb.can_execute("finrural-bo.org") is True
    assert cb.can_execute("bcb.gob.bo") is True


def test_cooldown_expiration_transitions_to_half_open() -> None:
    """Verifica que tras expirar el enfriamiento el circuito pase a HALF_OPEN."""
    cb = DomainCircuitBreaker(default_cooldown_seconds=10.0)
    current_time = 1000.0

    with patch("time.time", return_value=current_time):
        cb.record_failure("asfi.gob.bo", status_code=429, retry_after_seconds=10.0)
        assert cb.can_execute("asfi.gob.bo") is False

    # Avanzar el tiempo 11 segundos
    with patch("time.time", return_value=current_time + 11.0):
        assert cb.can_execute("asfi.gob.bo") is True
        status = cb.get_status("asfi.gob.bo")
        assert status.state == CircuitState.HALF_OPEN


def test_half_open_success_closes_circuit() -> None:
    """Verifica que una respuesta exitosa en HALF_OPEN cierre el circuito."""
    cb = DomainCircuitBreaker(default_cooldown_seconds=10.0)
    current_time = 1000.0

    with patch("time.time", return_value=current_time):
        cb.record_failure("asfi.gob.bo", status_code=429, retry_after_seconds=10.0)

    with patch("time.time", return_value=current_time + 12.0):
        assert cb.can_execute("asfi.gob.bo") is True
        # La sonda tiene éxito
        cb.record_success("asfi.gob.bo")

        status = cb.get_status("asfi.gob.bo")
        assert status.state == CircuitState.CLOSED
        assert status.failure_count == 0
        assert cb.can_execute("asfi.gob.bo") is True


def test_half_open_failure_applies_exponential_backoff() -> None:
    """Verifica que un fallo subsiguiente en HALF_OPEN reabra con retroceso."""
    cb = DomainCircuitBreaker(
        default_cooldown_seconds=10.0,
        exponential_backoff_factor=2.0,
    )
    current_time = 1000.0

    with patch("time.time", return_value=current_time):
        cb.record_failure("asfi.gob.bo", status_code=429)  # 1er fallo: 10s

    with patch("time.time", return_value=current_time + 12.0):
        assert cb.can_execute("asfi.gob.bo") is True  # HALF_OPEN
        # Falla la sonda
        cb.record_failure("asfi.gob.bo", status_code=429)  # 2do fallo: 10 * 2 = 20s

        status = cb.get_status("asfi.gob.bo")
        assert status.state == CircuitState.OPEN
        assert status.failure_count == 2
        assert status.cooldown_seconds == 20.0


def test_non_overload_status_codes_do_not_trip_circuit() -> None:
    """Verifica que errores como 404 o 400 no abran el disyuntor."""
    cb = DomainCircuitBreaker()
    cb.record_failure("asfi.gob.bo", status_code=404)

    assert cb.can_execute("asfi.gob.bo") is True
    status = cb.get_status("asfi.gob.bo")
    assert status.state == CircuitState.CLOSED
    assert status.failure_count == 0


def test_reset_circuit_breaker() -> None:
    """Verifica el restablecimiento manual de un dominio y de todos."""
    cb = DomainCircuitBreaker()
    cb.record_failure("asfi.gob.bo", status_code=429)
    cb.record_failure("bcb.gob.bo", status_code=503)

    assert cb.can_execute("asfi.gob.bo") is False
    assert cb.can_execute("bcb.gob.bo") is False

    cb.reset("asfi.gob.bo")
    assert cb.can_execute("asfi.gob.bo") is True
    assert cb.can_execute("bcb.gob.bo") is False

    cb.reset()
    assert cb.can_execute("bcb.gob.bo") is True


def test_parse_retry_after_numeric_and_invalid() -> None:
    """Verifica el parsing de cabeceras Retry-After."""
    assert parse_retry_after("120") == 120.0
    assert parse_retry_after(" 45 ") == 45.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("invalid-format") is None


def test_parse_retry_after_http_date() -> None:
    """Verifica el parsing de fechas HTTP estándar RFC 7231."""
    # Simular una fecha 60 segundos en el futuro
    now = time.time()
    future_gmt = time.strftime(
        "%a, %d %b %Y %H:%M:%S GMT",
        time.gmtime(now + 60),
    )
    parsed = parse_retry_after(future_gmt)
    assert parsed is not None
    assert 55.0 <= parsed <= 65.0
