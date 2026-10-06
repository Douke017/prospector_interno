"""Pruebas unitarias e integración para el Diccionario Semántico (HUI-16)."""

from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.application.catalog.semantic_service import (
    SemanticDictionaryService,
)
from prospector_interno.domain.contracts.semantic_spec import (
    MetricDataType,
)

client = TestClient(app)


def test_semantic_service_list_metrics() -> None:
    """Verifica que el servicio cargue las 13 métricas oficiales."""
    service = SemanticDictionaryService()
    resp = service.list_metrics()

    assert resp.total_metrics == 13
    assert "CARTERA" in resp.categories
    assert "BALANCE" in resp.categories
    assert "SOLVENCIA" in resp.categories
    assert "RENTABILIDAD" in resp.categories


def test_semantic_service_filter_by_category() -> None:
    """Valida el filtrado de métricas por categoría analítica."""
    service = SemanticDictionaryService()

    resp_cartera = service.list_metrics(category="CARTERA")
    assert resp_cartera.total_metrics >= 6
    assert all(m.category == "CARTERA" for m in resp_cartera.metrics)

    resp_solvencia = service.list_metrics(category="solvencia")
    assert resp_solvencia.total_metrics >= 1
    assert any(m.metric_code == "ratio_cobertura" for m in resp_solvencia.metrics)


def test_semantic_service_get_metric_by_code() -> None:
    """Verifica la recuperación exacta de especificaciones por código."""
    service = SemanticDictionaryService()

    metric = service.get_metric("ratio_mora")
    assert metric is not None
    assert metric.canonical_name == "Ratio de Morosidad de Cartera"
    assert metric.data_type == MetricDataType.PERCENTAGE
    assert "cartera_en_mora" in metric.required_fields
    assert "cartera_bruta" in metric.required_fields
    assert r"\frac{\text{cartera\_en\_mora}}" in metric.formula_latex

    # Caso inexistente
    assert service.get_metric("metrica_inventada") is None


def test_api_list_metrics_endpoint() -> None:
    """Verifica endpoint GET /api/v1/dictionary/metrics."""
    response = client.get("/api/v1/dictionary/metrics")
    assert response.status_code == 200
    data = response.json()
    assert data["total_metrics"] == 13
    assert len(data["metrics"]) == 13


def test_api_list_metrics_with_category_filter() -> None:
    """Verifica endpoint GET /api/v1/dictionary/metrics?category=BALANCE."""
    response = client.get("/api/v1/dictionary/metrics?category=BALANCE")
    assert response.status_code == 200
    data = response.json()
    assert data["total_metrics"] == 3
    codes = {m["metric_code"] for m in data["metrics"]}
    assert codes == {"total_activo", "total_pasivo", "total_patrimonio"}


def test_api_get_metric_detail_endpoint() -> None:
    """Verifica endpoint GET /api/v1/dictionary/metrics/{metric_code}."""
    response = client.get("/api/v1/dictionary/metrics/ratio_cobertura")
    assert response.status_code == 200
    data = response.json()
    assert data["metric_code"] == "ratio_cobertura"
    assert data["category"] == "SOLVENCIA"
    assert "prevision_cartera" in data["required_fields"]


def test_api_get_metric_not_found_returns_404() -> None:
    """Verifica respuesta 404 cuando la métrica solicitada no existe."""
    response = client.get("/api/v1/dictionary/metrics/metrica_falsa_xyz")
    assert response.status_code == 404
    data = response.json()
    assert "no existe en el diccionario" in data["detail"]
