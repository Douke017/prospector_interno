"""Pruebas de integración para la API REST y documentación Swagger."""

import json
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app

client = TestClient(app)


def test_health_check() -> None:
    """Verifica endpoint /health."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "prospector-interno"


def test_swagger_docs_available() -> None:
    """Verifica que la documentación interactiva Swagger (/docs) esté activa."""
    response = client.get("/docs")
    assert response.status_code == 200
    assert "swagger-ui" in response.text.lower()


def test_openapi_schema_generation() -> None:
    """Verifica que el esquema OpenAPI se genere con los tags definidos."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["info"]["title"] == "DATAX Prospector Interno & Portal Nexus API"
    assert "/api/v1/catalog/sources" in schema["paths"]
    assert "/api/v1/gatekeeper/evaluate" in schema["paths"]
    assert "/api/v1/reconciliation/diff" in schema["paths"]


def test_catalog_stats_endpoint() -> None:
    """Verifica endpoint de estadísticas consolidadas desde la vista materializada."""
    response = client.get("/api/v1/catalog/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["total_active_sources"] > 0
    assert data["total_canonical_datasets"] > 0
    assert data["view_status"] == "MATERIALIZED_IN_POSTGRESQL"


def test_catalog_sources_filtering() -> None:
    """Verifica que solo retorna fuentes activas con series vigentes."""
    response = client.get("/api/v1/catalog/sources")
    assert response.status_code == 200
    sources = response.json()
    assert len(sources) > 0

    # BCB debe encabezar la lista
    bcb = next((s for s in sources if s["short_name"] == "BCB"), None)
    assert bcb is not None
    assert bcb["total_active_datasets"] > 500


def test_catalog_datasets_search() -> None:
    """Verifica búsqueda parametrizada de series en la vista materializada."""
    response = client.get("/api/v1/catalog/datasets?source_code=BCB&limit=5")
    assert response.status_code == 200
    datasets = response.json()
    assert len(datasets) == 5
    for ds in datasets:
        assert ds["source_code"] == "BCB"
        assert ds["canonical_dataset_code"].startswith("DS_BO_BCB_")


def test_gatekeeper_evaluate_endpoint() -> None:
    """Verifica evaluación de pre-vuelo vía API REST."""
    payload = {
        "dataset_code": "DS_BO_INE_SEGUROS",
        "domain": "ine.gob.bo",
        "target_url": "https://www.ine.gob.bo/seguros.xlsx",
        "period": "2026-05",
        "force_refresh": False,
    }
    response = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["verdict"] == "PROCEED_DOWNLOAD"
    assert data["lock_acquired"] is True


def test_reconciliation_duckdb_diff(tmp_path: Path) -> None:
    """Verifica consulta ultrarrápida con DuckDB sobre JSON del Prospector Externo."""
    # Crear un map_metadata.json sintético
    test_json_data = [
        {
            "url": "https://www.ine.gob.bo/descargas/seguros_2026_05.xlsx",
            "content_length": 1048576,
            "last_modified": "2026-05-30T10:00:00Z",
            "file_extension": "xlsx",
        },
        {
            "url": "https://www.ine.gob.bo/descargas/ipc_2026_05.xlsx",
            "content_length": 524288,
            "last_modified": "2026-05-30T11:00:00Z",
            "file_extension": "xlsx",
        },
    ]

    json_file = tmp_path / "map_metadata.json"
    json_file.write_text(json.dumps(test_json_data), encoding="utf-8")

    payload = {
        "source_code": "INE",
        "map_metadata_json_path": str(json_file),
        "search_pattern": "%seguros%",
    }
    response = client.post("/api/v1/reconciliation/diff", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["source_code"] == "INE"
    assert data["matches_count"] == 1
    assert data["duration_ms"] >= 0.0
    assert len(data["items"]) == 1
    assert "seguros" in data["items"][0]["url"]
    assert data["items"][0]["status"] == "VERIFIED_IN_MAP"


def test_gatekeeper_evaluate_with_aliases_and_omitted_domain() -> None:
    """Verifica que Gatekeeper acepte aliases dataset_id y temporal_period."""
    payload = {
        "source_id": "ine",
        "dataset_id": "estadistica_seguros",
        "temporal_period": "2026-05",
    }
    response = client.post("/api/v1/gatekeeper/evaluate", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["verdict"] in ("PROCEED_DOWNLOAD", "CACHE_HIT_BRONZE")


def test_bridge_sync_dry_run_endpoint() -> None:
    """Verifica que Bridge /sync en modo dry-run no ejecute descargas."""
    payload = {
        "source_id": "finrural",
        "external_map_path": "../prospector_externo/output/finrural/mapa_finrural.json",
        "batch_size": 5,
        "execute_downloads": False,
    }
    response = client.post("/api/v1/bridge/sync", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["source_id"] == "finrural"
    assert data["total_tasks"] == 5
    assert data["completed_tasks"] == 0
    assert data["is_executed"] is False
    assert "dry-run" in data["summary"]


def test_bridge_reconcile_rest_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que POST /bridge/reconcile funcione vía REST sin external_map_path."""
    import sys
    from prospector_interno.domain.models import DiscoveredResourceItem

    br_module = sys.modules[
        "prospector_interno.adapters.api.routers.bridge_router"
    ]
    mock_items = [
        DiscoveredResourceItem(
            resource_key="rest_item_1",
            url="https://www.bbv.com.bo/rest_item_1.pdf",
            source_id="bbv",
            title="Item 1",
            file_extension=".pdf",
            period_label="2099-12",
        )
    ]

    monkeypatch.setattr(
        br_module._diff_engine._external_client,
        "fetch_resources",
        lambda source_id, base_url=None: mock_items,
    )
    monkeypatch.setattr(
        br_module._diff_engine._external_client,
        "get_latest_run_id",
        lambda source_id=None, base_url=None: "run_mock_rest_01",
    )

    payload = {"source_id": "bbv"}
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["source_id"] == "bbv"
    assert data["run_id"] == "run_mock_rest_01"
    assert data["total_external_resources"] == 1
    assert data["total_new"] == 1


def test_bridge_reconcile_502_on_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifica respuesta 502 si el Prospector Externo no responde en red."""
    import sys
    from prospector_interno.domain.exceptions import (
        ExternalCatalogConnectionError,
    )

    br_module = sys.modules[
        "prospector_interno.adapters.api.routers.bridge_router"
    ]

    def mock_fetch_fail(source_id: str, base_url: str | None = None) -> list:
        raise ExternalCatalogConnectionError("Connection refused on port 8000")

    monkeypatch.setattr(
        br_module._diff_engine._external_client,
        "fetch_resources",
        mock_fetch_fail,
    )

    payload = {"source_id": "bbv"}
    response = client.post("/api/v1/bridge/reconcile", json=payload)
    assert response.status_code == 502
    assert "Connection refused on port 8000" in response.json()["detail"]
