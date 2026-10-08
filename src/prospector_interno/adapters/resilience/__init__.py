"""Módulo de Resiliencia, Aislamiento Bulkhead y Tolerancia a Fallos."""

from prospector_interno.adapters.resilience.bulkhead_manager import (
    BulkheadManager,
    get_bulkhead_manager,
    set_bulkhead_manager,
)

__all__ = [
    "BulkheadManager",
    "get_bulkhead_manager",
    "set_bulkhead_manager",
]
