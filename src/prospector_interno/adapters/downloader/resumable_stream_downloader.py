"""Descargador HTTP Resiliente con Streaming, Reanudación y Checksum en Vuelo (HUI-08).

Implementa descargas por bloques de 64 KB directamente a disco (.tmp),
soporte de cabeceras Range para reanudación ante cortes, cómputo estricto
de SHA-256 en vuelo sin consumir memoria RAM (< 5 MB) y renombrado atómico.
Integrado con DomainCircuitBreaker (HUI-09) y RateLimiterPort (HUI-06).
"""

import hashlib
import os
from pathlib import Path
import tempfile
import time
from typing import BinaryIO

import httpx

from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
    parse_retry_after,
)
from prospector_interno.domain.contracts.downloader_spec import (
    ResumableDownloadResult,
)
from prospector_interno.domain.exceptions import (
    ChecksumMismatchError,
    DownloadExecutionError,
)
from prospector_interno.domain.ports import (
    CircuitBreakerPort,
    HttpDownloaderPort,
    RateLimiterPort,
    ResumableDownloaderPort,
)


class ResumableStreamDownloader(ResumableDownloaderPort, HttpDownloaderPort):
    """Cliente de adquisición HTTP con streaming a disco, Range y disyuntor."""

    DEFAULT_USER_AGENT: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
    DEFAULT_CHUNK_SIZE: int = 64 * 1024  # 64 KB

    def __init__(
        self,
        rate_limiter: RateLimiterPort | None = None,
        circuit_breaker: CircuitBreakerPort | None = None,
        timeout_seconds: float = 30.0,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._rate_limiter = rate_limiter
        self._circuit_breaker = circuit_breaker
        self._timeout_seconds = timeout_seconds
        self._chunk_size = chunk_size
        self._transport = transport

    def _get_headers(self, range_offset: int | None = None) -> dict[str, str]:
        headers = {
            "User-Agent": self.DEFAULT_USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,application/pdf,*/*;q=0.8"
            ),
            "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": (
                '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"'
            ),
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        if range_offset is not None and range_offset > 0:
            headers["Range"] = f"bytes={range_offset}-"
        return headers

    def download_file(
        self,
        url: str,
        domain: str,
        destination_path: Path,
        expected_checksum: str | None = None,
        allow_resume: bool = True,
    ) -> ResumableDownloadResult:
        """Descarga streaming a archivo temporal con reanudación y validación."""
        start_time = time.time()

        # 1. Verificar estado del Disyuntor Adaptativo (HUI-09)
        if self._circuit_breaker:
            if isinstance(self._circuit_breaker, DomainCircuitBreaker):
                self._circuit_breaker.verify_can_execute(domain)
            elif not self._circuit_breaker.can_execute(domain):
                raise DownloadExecutionError(
                    f"El circuito para '{domain}' está OPEN (enfriamiento activo)."
                )

        # 2. Respetar control de tasa educado por dominio (HUI-06)
        if self._rate_limiter:
            delay = self._rate_limiter.acquire_delay(domain)
            if delay > 0.0:
                time.sleep(min(delay, 2.0))

        destination_path = Path(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination_path.with_name(destination_path.name + ".tmp")

        # 3. Evaluar existencia de archivo parcial para reanudación (Range)
        existing_offset = 0
        was_resumed = False
        hasher = hashlib.sha256()

        if allow_resume and tmp_path.exists():
            existing_offset = tmp_path.stat().st_size
            if existing_offset > 0:
                # Leer bytes existentes para inicializar el hash acumulado
                with open(tmp_path, "rb") as existing_file:
                    while chunk := existing_file.read(self._chunk_size):
                        hasher.update(chunk)
                was_resumed = True

        headers = self._get_headers(
            range_offset=existing_offset if was_resumed else None
        )
        bytes_transferred_this_session = 0

        try:
            with httpx.Client(
                timeout=self._timeout_seconds,
                follow_redirects=True,
                headers=headers,
                transport=self._transport,
            ) as client:
                with client.stream("GET", url) as response:
                    status_code = response.status_code

                    # Manejo de fallos con notificación al Circuit Breaker
                    if status_code in (429, 503):
                        retry_after = parse_retry_after(
                            response.headers.get("Retry-After")
                        )
                        if self._circuit_breaker:
                            self._circuit_breaker.record_failure(
                                domain=domain,
                                status_code=status_code,
                                retry_after_seconds=retry_after,
                            )
                        raise DownloadExecutionError(
                            f"HTTP {status_code} al descargar '{url}'. "
                            f"Servidor sobrecargado."
                        )

                    if status_code >= 400:
                        raise DownloadExecutionError(
                            f"HTTP {status_code} al descargar '{url}'"
                        )

                    # Si el servidor devolvió 200 en vez de 206, reinicia desde byte 0
                    mode = "ab"
                    if status_code == 200 and was_resumed:
                        mode = "wb"
                        existing_offset = 0
                        was_resumed = False
                        hasher = hashlib.sha256()
                    elif not was_resumed:
                        mode = "wb"

                    content_type = response.headers.get("content-type")

                    # Streaming por chunks planos a disco actualizando SHA-256 en vuelo
                    with open(tmp_path, mode) as tmp_file:
                        for chunk in response.iter_bytes(chunk_size=self._chunk_size):
                            if chunk:
                                tmp_file.write(chunk)
                                hasher.update(chunk)
                                bytes_transferred_this_session += len(chunk)

            # Notificar éxito al disyuntor
            if self._circuit_breaker:
                self._circuit_breaker.record_success(domain)

            # 4. Validar integridad criptográfica final
            calculated_checksum = hasher.hexdigest().lower()
            if (
                expected_checksum
                and calculated_checksum != expected_checksum.lower()
            ):
                if tmp_path.exists():
                    tmp_path.unlink()
                raise ChecksumMismatchError(
                    f"Integridad corrupta para '{url}': "
                    f"esperado={expected_checksum}, calculado={calculated_checksum}",
                    expected=expected_checksum,
                    calculated=calculated_checksum,
                )

            # 5. Renombrado atómico final
            total_size = tmp_path.stat().st_size
            os.replace(tmp_path, destination_path)
            elapsed = time.time() - start_time

            return ResumableDownloadResult(
                url=url,
                domain=domain,
                file_path=str(destination_path),
                bytes_downloaded=bytes_transferred_this_session,
                total_file_size=total_size,
                was_resumed=was_resumed,
                sha256_hash=calculated_checksum,
                content_type=content_type,
                elapsed_seconds=round(elapsed, 4),
            )

        except (httpx.HTTPError, OSError) as err:
            # En caso de error, el archivo .tmp se preserva para reanudación futura
            # a menos que sea una corrupción de checksum explícita
            raise DownloadExecutionError(
                f"Fallo durante la transferencia streaming de '{url}': {err}"
            ) from err

    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        """Implementa HttpDownloaderPort usando un archivo temporal sin inflar RAM."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            result = self.download_file(
                url=url,
                domain=domain,
                destination_path=tmp_path,
                allow_resume=False,
            )
            stream = open(tmp_path, "rb")
            return stream, 200, result.content_type
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
