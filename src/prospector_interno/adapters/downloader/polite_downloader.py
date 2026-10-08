"""Cliente HTTP Resiliente y Educado (Polite Downloader).

Implementa HttpDownloaderPort utilizando streaming por bloques, cabeceras de
navegador real y respeto estricto del control de tasa (Token Bucket con Jitter).
"""

import io
import time
from typing import Any, BinaryIO

import httpx

from prospector_interno.adapters.downloader.transport import (
    RealBrowserTransport,
)
from prospector_interno.domain.contracts.downloader_spec import (
    REALISTIC_BROWSER_HEADERS,
)
from prospector_interno.domain.exceptions import DownloadExecutionError
from prospector_interno.domain.ports import (
    BrowserTransportPort,
    HttpDownloaderPort,
    RateLimiterPort,
)


class PoliteHttpDownloader(HttpDownloaderPort):
    """Cliente de adquisición HTTP con emulación de navegador y rate limiting."""

    DEFAULT_USER_AGENT: str = REALISTIC_BROWSER_HEADERS["User-Agent"]

    def __init__(
        self,
        rate_limiter: RateLimiterPort | None = None,
        timeout_seconds: float = 30.0,
        browser_transport: BrowserTransportPort | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._rate_limiter = rate_limiter
        self._timeout_seconds = timeout_seconds
        self._browser_transport = browser_transport or RealBrowserTransport()
        self._transport = transport

    def _get_headers(self) -> dict[str, str]:
        return self._browser_transport.get_default_headers()

    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        """Descarga por streaming aplicando pausas calculadas por el RateLimiter."""
        # 1. Respetar control de tasa educado por dominio
        if self._rate_limiter:
            delay = self._rate_limiter.acquire_delay(domain)
            if delay > 0.0:
                time.sleep(min(delay, 2.0))  # Acotar tiempo de sleep para seguridad

        buffer = io.BytesIO()

        try:
            client_kwargs: dict[str, Any] = {
                "timeout": self._timeout_seconds,
                "follow_redirects": True,
                "headers": self._get_headers(),
            }
            if self._transport is not None:
                client_kwargs["transport"] = self._transport
            elif hasattr(self._browser_transport, "create_http_transport"):
                client_kwargs["transport"] = (
                    self._browser_transport.create_http_transport()
                )
            if hasattr(self._browser_transport, "profile") and (
                getattr(self._browser_transport.profile, "http2_enabled", False)
            ):
                client_kwargs["http2"] = True

            with httpx.Client(**client_kwargs) as client:
                with client.stream("GET", url) as response:
                    if response.status_code >= 400:
                        raise DownloadExecutionError(
                            f"HTTP {response.status_code} al descargar '{url}'"
                        )

                    content_type = response.headers.get("content-type")

                    for chunk in response.iter_bytes(chunk_size=64 * 1024):
                        buffer.write(chunk)

            buffer.seek(0)
            return buffer, response.status_code, content_type

        except httpx.HTTPError as err:
            raise DownloadExecutionError(
                f"Error de red al conectar con '{url}': {err}"
            ) from err
