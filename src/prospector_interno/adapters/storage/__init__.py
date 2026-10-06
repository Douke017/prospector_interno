"""Adaptadores de almacenamiento de Prospector Interno."""

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)

__all__ = ["LocalBronzeStorage", "SqliteLineageAdapter"]
