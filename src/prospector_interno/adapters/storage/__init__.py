"""Adaptadores de almacenamiento de Prospector Interno."""

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)

__all__ = [
    "LocalBronzeStorage",
    "SqliteDownloadTrackingAdapter",
    "SqliteLineageAdapter",
]
