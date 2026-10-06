"""Disyuntor Adaptativo por Dominio ante HTTP 429/503 (HUI-09).

Mantiene máquinas de estado independientes por host/dominio para proteger
nuestra IP contra baneos y respetar cabeceras Retry-After institucionales.
"""

from email.utils import parsedate_to_datetime
import time
from typing import Any

from prospector_interno.domain.contracts.downloader_spec import (
    CircuitState,
    DomainCircuitStatus,
)
from prospector_interno.domain.exceptions import CircuitOpenError
from prospector_interno.domain.ports import CircuitBreakerPort


def parse_retry_after(header_value: str | None) -> float | None:
    """Parsea el valor de la cabecera Retry-After (segundos o fecha HTTP)."""
    if not header_value:
        return None

    stripped = header_value.strip()
    try:
        # 1. Intentar interpretar como entero/flotante de segundos
        return max(0.0, float(stripped))
    except ValueError:
        pass

    try:
        # 2. Intentar interpretar como fecha HTTP (RFC 2822 / 7231)
        target_dt = parsedate_to_datetime(stripped)
        diff = target_dt.timestamp() - time.time()
        return max(0.0, diff)
    except Exception:
        return None


class DomainCircuitBreaker(CircuitBreakerPort):
    """Implementación del disyuntor adaptativo desacoplado por dominio."""

    def __init__(
        self,
        default_cooldown_seconds: float = 300.0,
        exponential_backoff_factor: float = 2.0,
        max_cooldown_seconds: float = 1800.0,
    ) -> None:
        self.default_cooldown_seconds = default_cooldown_seconds
        self.exponential_backoff_factor = exponential_backoff_factor
        self.max_cooldown_seconds = max_cooldown_seconds
        self._domains: dict[str, dict[str, Any]] = {}

    def _get_entry(self, domain: str) -> dict[str, Any]:
        normalized = domain.lower().strip()
        if normalized not in self._domains:
            self._domains[normalized] = {
                "state": CircuitState.CLOSED,
                "failure_count": 0,
                "retry_after_until": None,
                "cooldown_seconds": self.default_cooldown_seconds,
                "last_status_code": None,
            }
        return self._domains[normalized]

    def can_execute(self, domain: str) -> bool:
        """Determina si un dominio puede admitir nuevas peticiones HTTP."""
        entry = self._get_entry(domain)
        state = entry["state"]

        if state == CircuitState.CLOSED:
            return True

        if state == CircuitState.OPEN:
            until = entry["retry_after_until"]
            now = time.time()
            if until is not None and now >= until:
                # El enfriamiento ha expirado: transicionar a HALF_OPEN
                entry["state"] = CircuitState.HALF_OPEN
                return True
            return False

        if state == CircuitState.HALF_OPEN:
            # En HALF_OPEN permitimos una petición de sondeo (probe)
            return True

        return True

    def verify_can_execute(self, domain: str) -> None:
        """Lanza CircuitOpenError si el circuito para el dominio está bloqueado."""
        if not self.can_execute(domain):
            status = self.get_status(domain)
            raise CircuitOpenError(
                f"El circuito para el dominio '{domain}' está OPEN "
                f"debido a respuestas HTTP {status.last_status_code}. "
                f"Reintento disponible en {status.cooldown_seconds:.1f}s.",
                domain=domain,
                retry_after_seconds=status.cooldown_seconds,
            )

    def record_success(self, domain: str) -> None:
        """Registra una respuesta exitosa, cerrando el circuito si estaba en prueba."""
        normalized = domain.lower().strip()
        if normalized in self._domains:
            entry = self._domains[normalized]
            entry["state"] = CircuitState.CLOSED
            entry["failure_count"] = 0
            entry["retry_after_until"] = None
            entry["cooldown_seconds"] = self.default_cooldown_seconds
            entry["last_status_code"] = None

    def record_failure(
        self,
        domain: str,
        status_code: int,
        retry_after_seconds: float | None = None,
    ) -> None:
        """Registra un fallo (429/503), abre el circuito y fija el enfriamiento."""
        entry = self._get_entry(domain)
        entry["last_status_code"] = status_code

        # Solo códigos de rechazo por sobrecarga/bloqueo activan el disyuntor
        if status_code in (429, 503):
            entry["failure_count"] += 1

            if retry_after_seconds is not None and retry_after_seconds > 0.0:
                cooldown = retry_after_seconds
            else:
                multiplier = self.exponential_backoff_factor ** max(
                    0, entry["failure_count"] - 1
                )
                cooldown = min(
                    self.default_cooldown_seconds * multiplier,
                    self.max_cooldown_seconds,
                )

            entry["cooldown_seconds"] = cooldown
            entry["retry_after_until"] = time.time() + cooldown
            entry["state"] = CircuitState.OPEN

    def get_status(self, domain: str) -> DomainCircuitStatus:
        """Devuelve los metadatos y estado actual del circuito para el dominio."""
        entry = self._get_entry(domain)
        now = time.time()

        # Chequear transición implícita si expiró el tiempo de espera
        if entry["state"] == CircuitState.OPEN:
            until = entry["retry_after_until"]
            if until is not None and now >= until:
                entry["state"] = CircuitState.HALF_OPEN

        remaining_cooldown = 0.0
        if entry["state"] == CircuitState.OPEN and entry["retry_after_until"]:
            remaining_cooldown = max(0.0, entry["retry_after_until"] - now)

        return DomainCircuitStatus(
            domain=domain.lower().strip(),
            state=entry["state"],
            failure_count=entry["failure_count"],
            cooldown_seconds=round(remaining_cooldown, 2),
            last_status_code=entry["last_status_code"],
            retry_after_until=entry["retry_after_until"],
        )

    def reset(self, domain: str | None = None) -> None:
        """Restablece el circuito de un dominio específico o de todos."""
        if domain:
            normalized = domain.lower().strip()
            self._domains.pop(normalized, None)
        else:
            self._domains.clear()
