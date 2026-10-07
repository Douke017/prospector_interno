"""Adaptadores analíticos y motores de cómputo vectorizado."""

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
    DuckDBGoldPartitionAdapter,
)

__all__ = ["DuckDBAnalyticsAdapter", "DuckDBGoldPartitionAdapter"]
