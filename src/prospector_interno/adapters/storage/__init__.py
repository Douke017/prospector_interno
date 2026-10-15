"""Adaptadores de almacenamiento de Prospector Interno."""

from prospector_interno.adapters.storage.db_connection import (
    DatabaseConfig,
    get_database_config,
    get_readonly_connection,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)

__all__ = [
    "DatabaseConfig",
    "LocalBronzeStorage",
    "SqliteDownloadTrackingAdapter",
    "SqliteLineageAdapter",
    "get_database_config",
    "get_readonly_connection",
]
