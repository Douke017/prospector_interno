"""Pruebas unitarias de Transporte HTTP con Emulación de Navegador Real (HUI-07).

Verifica que el transporte inyecte todas las cabeceras canónicas de Chrome 128
sobre Windows x64, configure el pool de conexiones TCP persistentes (Keep-Alive)
y habilite multiplexación HTTP/2 sin fugas de huellas automatizadas.
"""

from pathlib import Path
import pytest
import httpx

from prospector_interno.adapters.downloader.polite_downloader import (
    PoliteHttpDownloader,
)
from prospector_interno.adapters.downloader.resumable_stream_downloader import (
    ResumableStreamDownloader,
)
from prospector_interno.adapters.downloader.transport import (
    RealBrowserTransport,
)
from prospector_interno.domain.contracts.downloader_spec import (
    BrowserFingerprintProfile,
)


def test_browser_fingerprint_profile_defaults() -> None:

    """Verifica que el perfil por defecto cumpla estrictamente la Sección 8.3."""
    profile = BrowserFingerprintProfile()

    assert profile.profile_name == "chrome_windows_128"
    assert "Windows NT 10.0" in profile.user_agent
    assert "Chrome/128" in profile.user_agent
    assert profile.http2_enabled is True
    assert profile.keep_alive_enabled is True
    assert profile.max_connections == 20
    assert profile.max_keepalive_connections == 10
    assert profile.keepalive_expiry_seconds == 30.0

    # Comprobar cabeceras canónicas mínimas requeridas por Criterio 1
    headers = profile.headers
    assert headers["Sec-Ch-Ua"] == (
        '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"'
    )
    assert headers["Sec-Ch-Ua-Mobile"] == "?0"
    assert headers["Sec-Ch-Ua-Platform"] == '"Windows"'
    assert headers["Accept-Language"] == "es-BO,es;q=0.9,en;q=0.8"
    assert headers["Upgrade-Insecure-Requests"] == "1"
    assert headers["Sec-Fetch-Dest"] == "document"
    assert headers["Sec-Fetch-Mode"] == "navigate"
    assert headers["Sec-Fetch-Site"] == "none"
    assert headers["Sec-Fetch-User"] == "?1"


def test_real_browser_transport_creates_configured_transports() -> None:
    """Verifica creación de transportes síncronos y asíncronos con HTTP/2 y pool."""
    transport_adapter = RealBrowserTransport()

    sync_transport = transport_adapter.create_http_transport()
    assert isinstance(sync_transport, httpx.HTTPTransport)
    assert getattr(sync_transport._pool, "_http2", False) is True

    async_transport = transport_adapter.create_async_http_transport()
    assert isinstance(async_transport, httpx.AsyncHTTPTransport)
    assert getattr(async_transport._pool, "_http2", False) is True


def test_real_browser_transport_creates_client_with_headers() -> None:

    """Verifica que el cliente generado contenga las cabeceras realistas."""
    transport_adapter = RealBrowserTransport()
    client = transport_adapter.create_client(
        timeout_seconds=15.0,
        headers={"X-Custom-Tracking": "Test-123"},
    )

    try:
        assert client.timeout.read == 15.0
        assert "Sec-Ch-Ua" in client.headers
        assert client.headers["Accept-Language"] == "es-BO,es;q=0.9,en;q=0.8"
        assert client.headers["X-Custom-Tracking"] == "Test-123"
        assert "Windows" in client.headers["User-Agent"]
    finally:
        client.close()


@pytest.mark.anyio
async def test_real_browser_transport_creates_async_client() -> None:
    """Verifica que el cliente asíncrono generado sea válido y tipado."""
    transport_adapter = RealBrowserTransport()
    async_client = transport_adapter.create_async_client(timeout_seconds=20.0)

    try:
        assert async_client.timeout.read == 20.0
        assert "Sec-Ch-Ua" in async_client.headers
        assert async_client.headers["Sec-Ch-Ua-Platform"] == '"Windows"'
    finally:
        await async_client.aclose()


def test_resumable_downloader_sends_all_realistic_headers(tmp_path: Path) -> None:
    """Certifica Criterio 1: ResumableStreamDownloader envía todas las cabeceras."""
    captured_headers: dict[str, str] = {}

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        return httpx.Response(
            200,
            content=b"CONTENIDO BINARIO VALIDO",
            headers={"content-type": "application/pdf"},
        )

    mock_transport = httpx.MockTransport(mock_handler)
    downloader = ResumableStreamDownloader(transport=mock_transport)

    dest_file = tmp_path / "descarga_test.pdf"
    downloader.download_file(
        url="https://servidor-bancario.bo/reporte.pdf",
        domain="servidor-bancario.bo",
        destination_path=dest_file,
    )

    # Verificación estricta de Criterio 1
    assert captured_headers["sec-ch-ua"] == (
        '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"'
    )
    assert captured_headers["sec-ch-ua-mobile"] == "?0"
    assert captured_headers["sec-ch-ua-platform"] == '"Windows"'
    assert captured_headers["accept-language"] == "es-BO,es;q=0.9,en;q=0.8"
    assert captured_headers["upgrade-insecure-requests"] == "1"
    assert "Windows NT 10.0" in captured_headers["user-agent"]
    assert captured_headers["sec-fetch-dest"] == "document"
    assert captured_headers["sec-fetch-mode"] == "navigate"


def test_polite_http_downloader_sends_realistic_headers() -> None:
    """Verifica que PoliteHttpDownloader use las cabeceras canónicas."""
    captured_headers: dict[str, str] = {}

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        return httpx.Response(
            200,
            content=b"STREAM_BYTES_TEST",
            headers={"content-type": "application/pdf"},
        )

    mock_transport = httpx.MockTransport(mock_handler)
    downloader = PoliteHttpDownloader(transport=mock_transport)

    stream, status, content_type = downloader.download_to_stream(
        url="https://finrural.org.bo/doc.pdf",
        domain="finrural.org.bo",
    )

    assert status == 200
    assert content_type == "application/pdf"
    assert stream.read() == b"STREAM_BYTES_TEST"
    assert captured_headers["sec-ch-ua-platform"] == '"Windows"'
    assert captured_headers["accept-language"] == "es-BO,es;q=0.9,en;q=0.8"
