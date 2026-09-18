"""Servicio de Despacho y Control de Tasa (Polite Scheduler).

Implementa HUI-05 (Round-Robin Interleaving) y HUI-06 (Token Bucket con
Jitter Gaussiano) para proteger la reputación de IP y evitar bloqueos WAF.
"""

import random
import time
from collections import deque
from typing import Generic, TypeVar

from prospector_interno.domain.ports import RateLimiterPort

T = TypeVar("T")


class DomainTokenBucket(RateLimiterPort):
    """Controlador de tasa Token Bucket por dominio con Jitter Gaussiano."""

    def __init__(
        self,
        default_rate: float = 1.0,
        default_capacity: float = 2.0,
        jitter_sigma: float = 0.25,
    ) -> None:
        """Inicializa el regulador de tasa.

        Args:
            default_rate: Tokens generados por segundo (ej. 0.5 = 1 cada 2s).
            default_capacity: Capacidad máxima de ráfaga (burst capacity).
            jitter_sigma: Desviación estándar del jitter gaussiano en segundos.
        """
        self._default_rate = default_rate
        self._default_capacity = default_capacity
        self._jitter_sigma = jitter_sigma

        # Estado por dominio: {domain: (tokens, last_refill_timestamp)}
        self._buckets: dict[str, list[float]] = {}

    def _refill(self, domain: str, now: float) -> None:
        if domain not in self._buckets:
            self._buckets[domain] = [self._default_capacity, now]
            return

        tokens, last_time = self._buckets[domain]
        elapsed = now - last_time
        new_tokens = min(
            self._default_capacity,
            tokens + (elapsed * self._default_rate),
        )
        self._buckets[domain][0] = new_tokens
        self._buckets[domain][1] = now

    def acquire_delay(self, domain: str) -> float:
        """Calcula el tiempo de espera necesario antes de emitir la petición.

        Retorna los segundos de espera requeridos (0.0 si hay tokens disponibles).
        Aplica un jitter estocástico para simular variabilidad humana.
        """
        now = time.monotonic()
        self._refill(domain, now)

        tokens = self._buckets[domain][0]

        if tokens >= 1.0:
            self._buckets[domain][0] = tokens - 1.0
            base_delay = 0.0
        else:
            missing_tokens = 1.0 - tokens
            base_delay = missing_tokens / self._default_rate
            self._buckets[domain][0] = 0.0
            self._buckets[domain][1] = now + base_delay

        # Añadir Jitter Gaussiano centrado en 0, acotado para no ser negativo
        jitter = random.gauss(0.0, self._jitter_sigma)
        total_delay = max(0.0, base_delay + jitter)
        return total_delay


class MultiDomainRoundRobinQueue(Generic[T]):
    """Cola de tareas con despacho intercalado multi-dominio (Round-Robin).

    Garantiza que peticiones masivas al mismo host nunca se despachen de forma
    consecutiva si existen solicitudes para otros dominios en espera.
    """

    def __init__(self) -> None:
        self._domain_queues: dict[str, deque[T]] = {}
        self._active_domains: deque[str] = deque()
        self._total_items: int = 0

    def push(self, domain: str, item: T) -> None:
        """Encola un elemento en el sub-canal del dominio correspondiente."""
        if domain not in self._domain_queues:
            self._domain_queues[domain] = deque()
            self._active_domains.append(domain)

        self._domain_queues[domain].append(item)
        self._total_items += 1

    def pop(self) -> T | None:
        """Despacha el siguiente elemento rotando equitativamente entre dominios."""
        if not self._active_domains:
            return None

        # Rota al siguiente dominio disponible
        current_domain = self._active_domains.popleft()
        queue = self._domain_queues[current_domain]

        item = queue.popleft()
        self._total_items -= 1

        # Si aún quedan tareas en este dominio, lo reinserta al final de la rueda
        if queue:
            self._active_domains.append(current_domain)
        else:
            del self._domain_queues[current_domain]

        return item

    def __len__(self) -> int:
        return self._total_items

    def active_domain_count(self) -> int:
        """Retorna el número de dominios con tareas pendientes."""
        return len(self._active_domains)
