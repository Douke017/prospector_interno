"""Cliente HTTP REST para consumir la Catalog API Facade del Prospector Externo.

Implementa ExternalCatalogClientPort desacoplando completamente ambos sistemas
a nivel de despliegue y red (puerto 8000).
"""

import os
from typing import Any
import httpx

from prospector_interno.domain.exceptions import (
    ExternalCatalogConnectionError,
    ExternalCatalogResponseError,
)
from prospector_interno.domain.models import DiscoveredResourceItem
from prospector_interno.domain.ports import ExternalCatalogClientPort


class HttpExternalCatalogClient(ExternalCatalogClientPort):
    """Cliente HTTP resiliente para comunicación REST con Prospector Externo."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._default_base_url = (
            base_url
            or os.environ.get("PROSPECTOR_EXTERNO_URL", "http://localhost:8000")
        ).rstrip("/")
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=self._timeout)

    def _resolve_url(self, base_url: str | None) -> str:
        """Determina la URL base efectiva para la solicitud."""
        if base_url:
            return base_url.rstrip("/")
        return self._default_base_url

    def check_health(self, base_url: str | None = None) -> bool:
        """Verifica la conectividad y disponibilidad de la Catalog API Facade."""
        target_url = f"{self._resolve_url(base_url)}/health"
        try:
            resp = self._client.get(target_url, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("status") == "UP"
            return False
        except (httpx.RequestError, httpx.HTTPError, ValueError):
            return False

    def get_latest_run_id(
        self,
        source_id: str | None = None,
        base_url: str | None = None,
    ) -> str | None:
        """Consulta el identificador de la corrida más reciente en el catálogo."""
        target_url = f"{self._resolve_url(base_url)}/runs"
        try:
            resp = self._client.get(target_url, params={"limit": 1})
            if resp.status_code == 200:
                runs: list[dict[str, Any]] = resp.json()
                if runs and isinstance(runs, list):
                    return str(runs[0].get("run_id", "run_remote_latest"))
            return "run_remote_latest"
        except (httpx.RequestError, httpx.HTTPError, ValueError):
            return "run_remote_latest"

    def fetch_resources(
        self,
        source_id: str,
        base_url: str | None = None,
    ) -> list[DiscoveredResourceItem]:
        """Obtiene candidatos descubiertos para una fuente o todo el catálogo desde la Catalog API."""
        resolved_base = self._resolve_url(base_url)
        target_url = f"{resolved_base}/catalog/resources"
        clean_src = (source_id or "").strip()
        params: dict[str, Any] = (
            {}
            if not clean_src or clean_src.upper() == "ALL"
            else {"source_id": clean_src}
        )

        try:
            resp = self._client.get(target_url, params=params)
        except httpx.RequestError as err:
            raise ExternalCatalogConnectionError(
                f"No fue posible conectar con la Catalog API Facade en "
                f"'{resolved_base}': {err}"
            ) from err

        if resp.status_code == 404:
            return []

        if resp.status_code != 200:
            raise ExternalCatalogResponseError(
                f"La Catalog API Facade respondió con código de error "
                f"{resp.status_code}: {resp.text}"
            )

        try:
            raw_items: list[dict[str, Any]] = resp.json()
        except ValueError as err:
            raise ExternalCatalogResponseError(
                f"Respuesta JSON malformada recibida de '{target_url}': {err}"
            ) from err

        items: list[DiscoveredResourceItem] = []
        for raw in raw_items:
            items.append(
                DiscoveredResourceItem(
                    resource_key=str(raw.get("resource_key", "")),
                    url=str(raw.get("url", "")),
                    source_id=str(raw.get("source_id", source_id)),
                    title=str(raw.get("title", "")),
                    file_extension=str(raw.get("file_extension", "")),
                    content_type=raw.get("content_type"),
                    content_length_bytes=raw.get("content_length_bytes"),
                    last_modified_header=raw.get("last_modified_header"),
                    etag=raw.get("etag"),
                    period_label=raw.get("period_label"),
                    content_hash=raw.get("content_hash"),
                    change_status=str(raw.get("change_status", "UNCHANGED")),
                )
            )

        return items
