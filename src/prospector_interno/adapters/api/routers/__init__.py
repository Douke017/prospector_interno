from prospector_interno.adapters.api.routers.bridge_router import (
    router as bridge_router,
)
from prospector_interno.adapters.api.routers.catalog_router import (
    router as catalog_router,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    router as gatekeeper_router,
)
from prospector_interno.adapters.api.routers.reconciliation_router import (
    router as reconciliation_router,
)

__all__ = [
    "bridge_router",
    "catalog_router",
    "gatekeeper_router",
    "reconciliation_router",
]
