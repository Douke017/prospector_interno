"""Adaptadores de descarga HTTP para Prospector Interno."""

from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
)
from prospector_interno.adapters.downloader.polite_downloader import (
    PoliteHttpDownloader,
)
from prospector_interno.adapters.downloader.resumable_stream_downloader import (
    ResumableStreamDownloader,
)
from prospector_interno.adapters.downloader.transport import (
    RealBrowserTransport,
)
from prospector_interno.adapters.downloader.window_guard import (
    OffPeakWindowGuard,
)

__all__ = [
    "DomainCircuitBreaker",
    "OffPeakWindowGuard",
    "PoliteHttpDownloader",
    "RealBrowserTransport",
    "ResumableStreamDownloader",
]
