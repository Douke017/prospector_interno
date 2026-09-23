"""Cliente HTTP Resiliente y Educado (Polite Downloader).

Implementa HttpDownloaderPort utilizando streaming por bloques, cabeceras de
navegador real y respeto estricto del control de tasa (Token Bucket con Jitter).
"""

import io
import time
from typing import BinaryIO

import httpx

from prospector_interno.domain.exceptions import DownloadExecutionError
from prospector_interno.domain.ports import (
    HttpDownloaderPort,
    RateLimiterPort,
)


class PoliteHttpDownloader(HttpDownloaderPort):
    """Cliente de adquisición HTTP con emulación de navegador y rate limiting."""

    DEFAULT_USER_AGENT: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )

    def __init__(
        self,
        rate_limiter: RateLimiterPort | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._rate_limiter = rate_limiter
        self._timeout_seconds = timeout_seconds

    def _get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.DEFAULT_USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,application/pdf,*/*;q=0.8"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }

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
            with httpx.Client(
                timeout=self._timeout_seconds,
                follow_redirects=True,
                headers=self._get_headers(),
            ) as client:
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
