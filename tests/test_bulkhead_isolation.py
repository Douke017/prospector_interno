"""Pruebas para Aislamiento de Cargas Bulkhead y Presupuesto de RAM (HUI-23)."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
)
from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.resilience.bulkhead_manager import (
    BulkheadManager,
)
from prospector_interno.domain.contracts.bulkhead_spec import (
    BulkheadConfig,
    BulkheadStats,
)


def test_bulkhead_config_and_stats_validation() -> None:
    """Verifica modelos de configuración y estadísticas del Bulkhead."""
    config = BulkheadConfig(
        gatekeeper_max_workers=6,
        analytics_max_workers=3,
        max_memory_duckdb="4GB",
    )
    assert config.gatekeeper_max_workers == 6
    assert config.analytics_max_workers == 3
    assert config.max_memory_duckdb == "4GB"

    stats = BulkheadStats(
        gatekeeper_active_workers=1,
        gatekeeper_max_workers=6,
        analytics_active_workers=2,
        analytics_max_workers=3,
        max_memory_duckdb="4GB",
    )
    assert stats.gatekeeper_active_workers == 1
    assert stats.analytics_active_workers == 2


def test_duckdb_applies_max_memory_budget() -> None:
    """Verifica que DuckDB aplique el límite estricto de memoria (4GB)."""
    adapter = DuckDBAnalyticsAdapter(max_memory="4GB")
    setting = adapter.get_max_memory_setting()
    # DuckDB retorna '3.7 GiB' para un setting de 4GB (4_000_000_000 bytes)
    assert "GiB" in setting or "GB" in setting
    assert float(setting.split()[0]) >= 3.0


def test_bulkhead_executes_on_distinct_thread_pools() -> None:
    """Verifica que Gatekeeper y Analytics corran en pools de hilos separados."""
    manager = BulkheadManager(
        BulkheadConfig(gatekeeper_max_workers=4, analytics_max_workers=2)
    )

    def _get_thread_name() -> str:
        return threading.current_thread().name

    gk_thread = manager.execute_gatekeeper(_get_thread_name)
    analytics_thread = manager.execute_analytics(_get_thread_name)

    assert gk_thread.startswith("bulkhead-gatekeeper")
    assert analytics_thread.startswith("bulkhead-analytics")
    manager.shutdown()


def test_bulkhead_isolation_prevents_starvation_under_heavy_analytics() -> None:
    """Criterio 1: Carga en Analytics no degrada latencia de Gatekeeper (<100ms)."""
    manager = BulkheadManager(
        BulkheadConfig(gatekeeper_max_workers=8, analytics_max_workers=2)
    )

    # 1. Bloquear intencionalmente los 2 workers de Analytics por 350 ms
    def _heavy_analytics_query() -> None:
        time.sleep(0.35)

    analytics_pool = ThreadPoolExecutor(max_workers=2)
    f1 = analytics_pool.submit(
        lambda: manager.execute_analytics(_heavy_analytics_query)
    )
    f2 = analytics_pool.submit(
        lambda: manager.execute_analytics(_heavy_analytics_query)
    )

    # Esperar brevemente para asegurar que los workers analíticos estén saturados
    time.sleep(0.05)
    stats = manager.get_stats()
    assert stats.analytics_active_workers == 2

    # 2. Ejecutar 20 peticiones concurrentes de Gatekeeper
    def _gatekeeper_fast_check() -> float:
        t0 = time.perf_counter()
        # Simula evaluación rápida de pre-vuelo en RAM
        time.sleep(0.005)
        return (time.perf_counter() - t0) * 1000

    latencies: list[float] = []
    with ThreadPoolExecutor(max_workers=10) as client_pool:
        futures = [
            client_pool.submit(
                lambda: manager.execute_gatekeeper(_gatekeeper_fast_check)
            )
            for _ in range(20)
        ]
        for f in futures:
            latencies.append(f.result())

    # 3. Certificar que ninguna llamada a Gatekeeper sufrió inanición (<100 ms)
    avg_latency = sum(latencies) / len(latencies)
    max_latency = max(latencies)

    # Esperar que liberen los hilos de analytics
    f1.result()
    f2.result()
    analytics_pool.shutdown()
    manager.shutdown()

    assert avg_latency < 50.0, f"Latencia promedio muy alta: {avg_latency} ms"
    assert max_latency < 100.0, f"Latencia máxima excedió 100 ms: {max_latency} ms"


@pytest.mark.anyio
async def test_bulkhead_async_execution() -> None:
    """Verifica wrappers asíncronos execute_gatekeeper_async y analytics_async."""
    manager = BulkheadManager()

    def _sync_work(val: int) -> int:
        return val * 2

    res_gk = await manager.execute_gatekeeper_async(_sync_work, 21)
    res_an = await manager.execute_analytics_async(_sync_work, 50)

    assert res_gk == 42
    assert res_an == 100
    manager.shutdown()


def test_api_bulkhead_stats_endpoint() -> None:
    """Verifica el endpoint GET /api/v1/gatekeeper/bulkhead/stats."""
    client = TestClient(app)
    resp = client.get("/api/v1/gatekeeper/bulkhead/stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["gatekeeper_max_workers"] >= 1
    assert data["analytics_max_workers"] >= 1
    assert data["max_memory_duckdb"] == "4GB"
    assert data["gatekeeper_active_workers"] >= 0
    assert data["analytics_active_workers"] >= 0
