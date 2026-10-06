"""Pruebas para el adaptador analítico DuckDB y la API de autoservicio (HUI-21)."""

from decimal import Decimal
from pathlib import Path
from starlette.testclient import TestClient

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
)
from prospector_interno.adapters.api.main import app
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
)
from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    SilverFinancialRecord,
    StatementType,
)

client = TestClient(app)


def _seed_test_gold_mart(gold_dir: Path) -> None:
    """Genera datos sintéticos en Capa Gold para pruebas deterministas."""
    materializer = GoldMaterializer(base_gold_path=gold_dir)
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
            amount_bob=Decimal("1500000.00"),
            source_file_sha256="sha_crecer_01",
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
            amount_bob=Decimal("30000.00"),
            source_file_sha256="sha_crecer_01",
        ),
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="PRO_MUJER",
            institution_name="Pro Mujer IFD",
            period="2026-02",
            gestion=2026,
            mes=2,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_BRUTA",
            amount_bob=Decimal("2000000.00"),
            source_file_sha256="sha_promujer_02",
        ),
    ]
    gold_records = materializer.transform_to_mart_cartera(records)
    materializer.materialize_mart_cartera(gold_records)


def test_duckdb_adapter_has_data(tmp_path: Path) -> None:
    """Verifica detección de particiones en Capa Gold."""
    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)
    assert not adapter.has_data("mart_cartera")

    _seed_test_gold_mart(tmp_path)
    assert adapter.has_data("mart_cartera")
    assert adapter.has_data("mart_cartera", gestion=2026)
    assert adapter.has_data("mart_cartera", gestion=2026, mes=1)
    assert not adapter.has_data("mart_cartera", gestion=2020)


def test_duckdb_adapter_get_dimensions(tmp_path: Path) -> None:
    """Verifica consulta de dimensiones agregadas."""
    _seed_test_gold_mart(tmp_path)
    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)
    dims = adapter.get_dimensions("mart_cartera")

    assert "CRECER" in dims["instituciones"]
    assert "PRO_MUJER" in dims["instituciones"]
    assert 2026 in dims["gestiones"]
    assert 1 in dims["meses"]
    assert 2 in dims["meses"]


def test_duckdb_adapter_execute_query_with_filters(tmp_path: Path) -> None:
    """Valida la ejecución de consultas con filtros parametrizados."""
    _seed_test_gold_mart(tmp_path)
    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)

    req = AnalyticsQueryRequest(
        datamart="mart_cartera",
        institutions=["CRECER"],
        gestiones=[2026],
        meses=[1],
        metrics=["institucion_id", "cartera_bruta", "ratio_mora"],
    )
    res = adapter.execute_query(req)

    assert res.total_records == 1
    assert res.execution_time_ms >= 0.0
    rec = res.records[0]
    assert rec["institucion_id"] == "CRECER"
    assert rec["cartera_bruta"] == 1500000.0
    assert rec["ratio_mora"] == 0.02


def test_duckdb_adapter_non_existent_mart(tmp_path: Path) -> None:
    """Verifica manejo seguro cuando el datamart no existe."""
    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)
    req = AnalyticsQueryRequest(datamart="no_existe")
    res = adapter.execute_query(req)
    assert res.total_records == 0
    assert res.records == []


def test_api_dimensions_endpoint() -> None:
    """Verifica endpoint GET /api/v1/analytics/dimensions."""
    response = client.get("/api/v1/analytics/dimensions")
    assert response.status_code == 200
    data = response.json()
    assert "instituciones" in data
    assert "gestiones" in data
    assert "meses" in data


def test_api_query_endpoint() -> None:
    """Verifica endpoint POST /api/v1/analytics/query."""
    payload = {
        "datamart": "mart_cartera",
        "institutions": ["CRECER"],
    }
    response = client.post("/api/v1/analytics/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["datamart"] == "mart_cartera"
    assert "total_records" in data
    assert "execution_time_ms" in data
    assert "records" in data


def test_api_materialize_endpoint() -> None:
    """Verifica endpoint POST /api/v1/analytics/materialize."""
    response = client.post("/api/v1/analytics/materialize")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert "total_bronze_artifacts" in data
    assert "total_silver_records" in data
    assert "total_gold_records" in data
    assert "mart_path" in data
