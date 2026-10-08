"""Administrador de Aislamiento de Cargas Bulkhead y Presupuesto de RAM (HUI-23)."""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any

from prospector_interno.domain.contracts.bulkhead_spec import (
    BulkheadConfig,
    BulkheadStats,
)
from prospector_interno.domain.ports import BulkheadExecutorPort


class BulkheadManager(BulkheadExecutorPort):
    """Implementa el patrón Bulkhead aislando pools para Gatekeeper y DuckDB."""

    def __init__(self, config: BulkheadConfig | None = None) -> None:
        self._config = config or BulkheadConfig()
        self._gatekeeper_executor = ThreadPoolExecutor(
            max_workers=self._config.gatekeeper_max_workers,
            thread_name_prefix="bulkhead-gatekeeper",
        )
        self._analytics_executor = ThreadPoolExecutor(
            max_workers=self._config.analytics_max_workers,
            thread_name_prefix="bulkhead-analytics",
        )
        self._lock = threading.Lock()
        self._gatekeeper_active = 0
        self._analytics_active = 0
        self._is_shutdown = False

    @property
    def config(self) -> BulkheadConfig:
        """Configuración activa del Bulkhead."""
        return self._config

    def execute_gatekeeper(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Ejecuta una validación de pre-vuelo en el pool de Gatekeeper."""
        with self._lock:
            self._gatekeeper_active += 1

        def _wrapper() -> Any:
            try:
                return func(*args, **kwargs)
            finally:
                with self._lock:
                    self._gatekeeper_active = max(0, self._gatekeeper_active - 1)

        future = self._gatekeeper_executor.submit(_wrapper)
        return future.result()

    async def execute_gatekeeper_async(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Ejecuta asíncronamente en el pool aislado de Gatekeeper."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.execute_gatekeeper(func, *args, **kwargs),
        )

    def execute_analytics(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Ejecuta una consulta pesada en el pool analítico de DuckDB."""
        with self._lock:
            self._analytics_active += 1

        def _wrapper() -> Any:
            try:
                return func(*args, **kwargs)
            finally:
                with self._lock:
                    self._analytics_active = max(0, self._analytics_active - 1)

        future = self._analytics_executor.submit(_wrapper)
        return future.result()

    async def execute_analytics_async(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Ejecuta asíncronamente en el pool analítico de DuckDB."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.execute_analytics(func, *args, **kwargs),
        )

    def get_stats(self) -> BulkheadStats:
        """Retorna métricas de concurrencia y límites del Bulkhead."""
        with self._lock:
            return BulkheadStats(
                gatekeeper_active_workers=self._gatekeeper_active,
                gatekeeper_max_workers=self._config.gatekeeper_max_workers,
                analytics_active_workers=self._analytics_active,
                analytics_max_workers=self._config.analytics_max_workers,
                max_memory_duckdb=self._config.max_memory_duckdb,
            )

    def shutdown(self, wait: bool = True) -> None:
        """Cierra ambos pools de hilos de forma ordenada."""
        with self._lock:
            if self._is_shutdown:
                return
            self._is_shutdown = True

        self._gatekeeper_executor.shutdown(wait=wait)
        self._analytics_executor.shutdown(wait=wait)

    def __enter__(self) -> "BulkheadManager":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=False)


_default_bulkhead_manager: BulkheadManager | None = None


def get_bulkhead_manager() -> BulkheadManager:
    """Obtiene la instancia global de BulkheadManager (singleton diferido)."""
    global _default_bulkhead_manager
    if _default_bulkhead_manager is None:
        _default_bulkhead_manager = BulkheadManager()
    return _default_bulkhead_manager


def set_bulkhead_manager(manager: BulkheadManager) -> None:
    """Sobrescribe la instancia activa de BulkheadManager (útil para pruebas)."""
    global _default_bulkhead_manager
    _default_bulkhead_manager = manager
