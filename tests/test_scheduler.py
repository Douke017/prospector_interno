"""Pruebas unitarias para HUI-05 y HUI-06: Despacho y Control de Tasa."""

from prospector_interno.application.scheduler import (
    DomainTokenBucket,
    MultiDomainRoundRobinQueue,
)


def test_round_robin_queue_interleaving_multi_domain() -> None:
    """Verifica HUI-05: Tareas de múltiples dominios se intercalan
    circularmente (Round-Robin).
    """
    queue = MultiDomainRoundRobinQueue[str]()

    # Encolar 3 tareas para BCB, 2 para ASFI y 1 para INE
    queue.push("bcb.gob.bo", "bcb-task-1")
    queue.push("bcb.gob.bo", "bcb-task-2")
    queue.push("bcb.gob.bo", "bcb-task-3")

    queue.push("asfi.gob.bo", "asfi-task-1")
    queue.push("asfi.gob.bo", "asfi-task-2")

    queue.push("ine.gob.bo", "ine-task-1")

    assert len(queue) == 6
    assert queue.active_domain_count() == 3

    # Secuencia esperada:
    # Ronda 1: bcb-1, asfi-1, ine-1
    # Ronda 2: bcb-2, asfi-2 (ine ya terminó)
    # Ronda 3: bcb-3 (asfi ya terminó)
    dispatched = []
    while len(queue) > 0:
        item = queue.pop()
        assert item is not None
        dispatched.append(item)

    expected = [
        "bcb-task-1",
        "asfi-task-1",
        "ine-task-1",
        "bcb-task-2",
        "asfi-task-2",
        "bcb-task-3",
    ]

    assert dispatched == expected
    assert len(queue) == 0
    assert queue.pop() is None


def test_domain_token_bucket_allows_burst_then_throttles() -> None:
    """Verifica HUI-06: Permite ráfaga inicial y luego aplica delay con jitter."""
    # Capacidad 2.0, tasa 1.0 token/segundo, jitter mínimo para test determinista
    limiter = DomainTokenBucket(
        default_rate=1.0,
        default_capacity=2.0,
        jitter_sigma=0.01,
    )

    domain = "bcb.gob.bo"

    # Primeras 2 peticiones (dentro de capacidad de ráfaga) deben tener demora ~0
    delay1 = limiter.acquire_delay(domain)
    delay2 = limiter.acquire_delay(domain)

    assert delay1 < 0.1
    assert delay2 < 0.1

    # La tercera petición excede la capacidad inmediata y debe esperar ~1.0 segundo
    delay3 = limiter.acquire_delay(domain)
    assert 0.8 <= delay3 <= 1.2

    # Verificar que otro dominio tiene su propio balde independiente
    other_domain_delay = limiter.acquire_delay("asfi.gob.bo")
    assert other_domain_delay < 0.1  # Primer token de ASFI es inmediato
