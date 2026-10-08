from prospector_interno.adapters.api.routers.analytics_router import (
    router as analytics_router,
)
from prospector_interno.adapters.api.routers.bridge_router import (
    router as bridge_router,
)
from prospector_interno.adapters.api.routers.catalog_router import (
    router as catalog_router,
)
from prospector_interno.adapters.api.routers.dictionary_router import (
    router as dictionary_router,
)
from prospector_interno.adapters.api.routers.downloads_router import (
    router as downloads_router,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    router as gatekeeper_router,
)
from prospector_interno.adapters.api.routers.reconciliation_router import (
    router as reconciliation_router,
)
from prospector_interno.adapters.api.routers.search_router import (
    router as search_router,
)

__all__ = [
    "analytics_router",
    "bridge_router",
    "catalog_router",
    "dictionary_router",
    "downloads_router",
    "gatekeeper_router",
    "reconciliation_router",
    "search_router",
]
