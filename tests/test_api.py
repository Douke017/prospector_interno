"""Pruebas de integración para la API REST y documentación Swagger."""

import json
from pathlib import Path
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
