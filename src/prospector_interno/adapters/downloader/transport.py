"""Adaptador de Transporte HTTP con Emulación de Navegador Real y HTTP/2 (HUI-07).

Implementa BrowserTransportPort configurando httpx con firmas de navegador de
escritorio (Chrome 128 sobre Windows x64), pool de conexiones TCP persistentes
(Keep-Alive) y soporte para multiplexación HTTP/2.
"""

import httpx

from prospector_interno.domain.contracts.downloader_spec import (
    BrowserFingerprintProfile,
)
from prospector_interno.domain.ports import BrowserTransportPort


class RealBrowserTransport(BrowserTransportPort):

    """Transporte HTTP con anti-fingerprinting, pool persistente y HTTP/2."""

    def __init__(
        self,
        profile: BrowserFingerprintProfile | None = None,
        verify_ssl: bool = True,
    ) -> None:
        self._profile = profile or BrowserFingerprintProfile()
        self._verify_ssl = verify_ssl

        # Configuración del pool de conexiones persistentes (Keep-Alive)
        self._limits = httpx.Limits(
            max_connections=self._profile.max_connections,
            max_keepalive_connections=self._profile.max_keepalive_connections,
            keepalive_expiry=self._profile.keepalive_expiry_seconds,
        )

    @property
    def profile(self) -> BrowserFingerprintProfile:
        """Retorna el perfil de huella HTTP activo."""
        return self._profile

    def get_default_headers(self) -> dict[str, str]:
        """Retorna las cabeceras canónicas de emulación de navegador real."""
        return dict(self._profile.headers)

    def create_http_transport(self) -> httpx.HTTPTransport:
        """Crea un transporte síncrono configurado con pool y HTTP/2."""
        return httpx.HTTPTransport(
            verify=self._verify_ssl,
            http2=self._profile.http2_enabled,
            limits=self._limits,
            retries=0,
        )

    def create_async_http_transport(self) -> httpx.AsyncHTTPTransport:
        """Crea un transporte asíncrono configurado con pool y HTTP/2."""
        return httpx.AsyncHTTPTransport(
            verify=self._verify_ssl,
            http2=self._profile.http2_enabled,
            limits=self._limits,
            retries=0,
        )

    def create_client(
        self,
        timeout_seconds: float = 30.0,
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        transport: httpx.BaseTransport | None = None,
    ) -> httpx.Client:
        """Crea un cliente HTTP síncrono con pool de conexiones y HTTP/2."""
        merged_headers = self.get_default_headers()
        if headers:
            merged_headers.update(headers)

        active_transport = transport or self.create_http_transport()

        return httpx.Client(
            transport=active_transport,
            headers=merged_headers,
            timeout=timeout_seconds,
            follow_redirects=follow_redirects,
            http2=self._profile.http2_enabled,
        )

    def create_async_client(
        self,
        timeout_seconds: float = 30.0,
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> httpx.AsyncClient:
        """Crea un cliente HTTP asíncrono con pool de conexiones y HTTP/2."""
        merged_headers = self.get_default_headers()
        if headers:
            merged_headers.update(headers)

        active_transport = transport or self.create_async_http_transport()

        return httpx.AsyncClient(
            transport=active_transport,
            headers=merged_headers,
            timeout=timeout_seconds,
            follow_redirects=follow_redirects,
            http2=self._profile.http2_enabled,
        )
