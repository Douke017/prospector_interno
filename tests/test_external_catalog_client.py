"""Pruebas unitarias para HttpExternalCatalogClient (Comunicación REST)."""

from typing import Any
import httpx
import pytest

from prospector_interno.adapters.external.rest_prospector_client import (
    HttpExternalCatalogClient,
)
from prospector_interno.domain.exceptions import (
    ExternalCatalogConnectionError,
    ExternalCatalogResponseError,
)


def _build_mock_client(handler: Any) -> HttpExternalCatalogClient:
    """Construye un cliente con transporte simulado en memoria."""
    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    return HttpExternalCatalogClient(
        base_url="http://mock-externo:8000",
        client=client,
    )


def test_check_health_success() -> None:
    """Verifica que check_health retorna True ante respuesta HTTP 200 UP."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/health"
        return httpx.Response(200, json={"status": "UP"})

    client = _build_mock_client(handler)
    assert client.check_health() is True


def test_check_health_failure_status() -> None:
    """Verifica que check_health retorna False ante error 500 o fallo de red."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"status": "DOWN"})

    client = _build_mock_client(handler)
    assert client.check_health() is False


def test_get_latest_run_id_returns_report_id() -> None:
    """Verifica que get_latest_run_id extrae el run_id del último reporte."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/runs"
        return httpx.Response(200, json=[{"run_id": "run_20260923_test_01"}])

    client = _build_mock_client(handler)
    run_id = client.get_latest_run_id()
    assert run_id == "run_20260923_test_01"


def test_get_latest_run_id_fallback_on_network_error() -> None:
    """Verifica fallback seguro si falla la consulta de runs."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Network unreachable")

    client = _build_mock_client(handler)
    run_id = client.get_latest_run_id()
    assert run_id == "run_remote_latest"


def test_fetch_resources_parses_candidates() -> None:
    """Verifica la transformación de candidatos JSON a modelos de dominio."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog/resources"
        assert request.url.params.get("source_id") == "bbv"
        return httpx.Response(
            200,
            json=[
                {
                    "resource_key": "res_bbv_01",
                    "url": "https://www.bbv.com.bo/boletin_2026.pdf",
                    "source_id": "bbv",
                    "title": "Boletín Diario BBV",
                    "file_extension": ".pdf",
                    "content_length_bytes": 1024,
                    "period_label": "2026-01",
                    "change_status": "NEW",
                }
            ],
        )

    client = _build_mock_client(handler)
    items = client.fetch_resources(source_id="bbv")

    assert len(items) == 1
    assert items[0].resource_key == "res_bbv_01"
    assert items[0].url == "https://www.bbv.com.bo/boletin_2026.pdf"
    assert items[0].period_label == "2026-01"
    assert items[0].change_status == "NEW"


def test_fetch_resources_returns_empty_on_404() -> None:
    """Verifica que una fuente desconocida o sin recursos retorna lista vacía."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "Fuente no encontrada"})

    client = _build_mock_client(handler)
    items = client.fetch_resources(source_id="inexistente")
    assert items == []


def test_fetch_resources_raises_connection_error_on_network_failure() -> None:
    """Verifica que fallas de conexión disparan ExternalCatalogConnectionError."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused by port 8000")

    client = _build_mock_client(handler)
    with pytest.raises(ExternalCatalogConnectionError) as exc_info:
        client.fetch_resources(source_id="bbv")

    assert "No fue posible conectar con la Catalog API Facade" in str(
        exc_info.value
    )


def test_fetch_resources_raises_response_error_on_server_error() -> None:
    """Verifica que respuestas 500 disparan ExternalCatalogResponseError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Catalog Database Error")

    client = _build_mock_client(handler)
    with pytest.raises(ExternalCatalogResponseError) as exc_info:
        client.fetch_resources(source_id="bbv")

    assert "código de error 500" in str(exc_info.value)
