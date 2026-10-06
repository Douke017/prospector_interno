"""Pruebas unitarias e integración para Vistas Analíticas Predefinidas (HUI-22)."""

from decimal import Decimal
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.application.analytics.views_service import (
    PredefinedViewsService,
    ViewNotFoundError,
)
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    SilverFinancialRecord,
    StatementType,
)
from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewQueryRequest,
)

client = TestClient(app)


def _seed_test_data(gold_path: Path) -> None:
    """Siembra datos sintéticos en Capa Gold para pruebas deterministas."""
    materializer = GoldMaterializer(base_gold_path=gold_path)
    records = [
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="CRECER",
            institution_name="Crecer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_BRUTA",
            amount_bob=Decimal("1000000.00"),
            source_file_sha256="sha_1",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="CRECER",
            institution_name="Crecer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_VENCIDA",
            amount_bob=Decimal("20000.00"),
            source_file_sha256="sha_1",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="CRECER",
            institution_name="Crecer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="PREVISION_CARTERA",
            amount_bob=Decimal("-30000.00"),
            source_file_sha256="sha_1",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="PRO_MUJER",
            institution_name="Pro Mujer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_BRUTA",
            amount_bob=Decimal("2000000.00"),
            source_file_sha256="sha_2",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="PRO_MUJER",
            institution_name="Pro Mujer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_VENCIDA",
            amount_bob=Decimal("100000.00"),
            source_file_sha256="sha_2",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="PRO_MUJER",
            institution_name="Pro Mujer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="PREVISION_CARTERA",
            amount_bob=Decimal("-100000.00"),
            source_file_sha256="sha_2",
        ),
    ]
    gold_records = materializer.transform_to_mart_cartera(records)
    materializer.materialize_mart_cartera(gold_records)


def test_views_service_catalog() -> None:
    """Verifica que el catálogo exponga las 3 vistas de negocio estándar."""
    service = PredefinedViewsService()
    views = service.list_views()
    assert len(views) == 3
    view_ids = {v.view_id for v in views}
    assert view_ids == {
        "evolucion-mora-temporal",
        "ranking-solvencia-cobertura",
        "comparativo-entidades-resumen",
    }


def test_views_service_get_view() -> None:
    """Valida obtención de metadata de vistas individuales."""
    service = PredefinedViewsService()
    view = service.get_view("ranking-solvencia-cobertura")
    assert view is not None
    assert view.category == "SOLVENCIA"
    assert "ratio_cobertura" in view.output_columns

    assert service.get_view("vista-inexistente") is None


def test_views_service_execute_unknown_view_raises_error() -> None:
    """Verifica que invocar una vista no registrada lance ViewNotFoundError."""
    service = PredefinedViewsService()
    with pytest.raises(ViewNotFoundError, match="no existe en el catálogo"):
        service.execute_view("vista_fantasma_123")


def test_views_service_execute_empty_gold(tmp_path: Path) -> None:
    """Verifica retorno graceful sin error cuando Gold no tiene particiones."""
    service = PredefinedViewsService(base_gold_path=tmp_path)
    res = service.execute_view("evolucion-mora-temporal")
    assert res.total_records == 0
    assert res.records == []


def test_views_service_execute_ranking_cobertura(tmp_path: Path) -> None:
    """Verifica ejecución y ordenamiento del ranking de solvencia/cobertura."""
    _seed_test_data(tmp_path)
    service = PredefinedViewsService(base_gold_path=tmp_path)

    # CRECER tiene 30k previsión / 20k mora = 1.5 ratio_cobertura
    # PRO_MUJER tiene 100k previsión / 100k mora = 1.0 ratio_cobertura
    req = PredefinedViewQueryRequest(gestion=2026, mes=1, limit=5)
    res = service.execute_view("ranking-solvencia-cobertura", req)

    assert res.total_records == 2
    assert res.execution_time_ms >= 0.0
    first = res.records[0]
    assert first["institucion_id"] == "CRECER"
    assert first["ratio_cobertura"] == 1.5


def test_api_list_predefined_views() -> None:
    """Verifica endpoint GET /api/v1/analytics/views."""
    response = client.get("/api/v1/analytics/views")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3


def test_api_get_predefined_view_detail() -> None:
    """Verifica endpoint GET /api/v1/analytics/views/{view_id}."""
    response = client.get("/api/v1/analytics/views/evolucion-mora-temporal")
    assert response.status_code == 200
    data = response.json()
    assert data["view_id"] == "evolucion-mora-temporal"
    assert data["category"] == "RIESGO"


def test_api_get_predefined_view_not_found() -> None:
    """Verifica 404 al consultar metadatos de vista inexistente."""
    response = client.get("/api/v1/analytics/views/no-existe")
    assert response.status_code == 404
    data = response.json()
    assert "no existe en el catálogo" in data["detail"]


def test_api_execute_predefined_view_endpoint() -> None:
    """Verifica endpoint POST /api/v1/analytics/views/{view_id}/execute."""
    payload = {"gestion": 2026, "limit": 10}
    response = client.post(
        "/api/v1/analytics/views/ranking-solvencia-cobertura/execute",
        json=payload,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["view_id"] == "ranking-solvencia-cobertura"
    assert "execution_time_ms" in data
    assert "records" in data


def test_api_execute_predefined_view_not_found() -> None:
    """Verifica 404 al ejecutar vista no registrada."""
    response = client.post("/api/v1/analytics/views/no-existe/execute", json={})
    assert response.status_code == 404
    data = response.json()
    assert "no existe en el catálogo" in data["detail"]
