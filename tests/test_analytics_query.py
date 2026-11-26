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


def test_analytics_query_server_search_and_pagination(tmp_path: Path) -> None:
    """Verifica búsqueda de texto libre insensible a mayúsculas y paginación en DuckDB."""
    import duckdb
    from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
        DuckDBAnalyticsAdapter,
    )
    from prospector_interno.domain.contracts.analytics_spec import AnalyticsQueryRequest

    part_dir = tmp_path / "catalogo_series_files" / "gestion=2025" / "mes=01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pq_file = part_dir / "part_0.parquet"

    con = duckdb.connect(":memory:")
    con.execute(
        f"""
        COPY (
            SELECT 'ADUANA' AS institucion_id, 'Aduana Nacional' AS institucion_nombre,
                   'D_BO_000000012' AS codigo_serie, 'Boletin de recaudaciones' AS nombre_serie,
                   'MANUAL_FILE' AS tipo_serie, '.xlsx' AS formato_archivo,
                   'Mensual' AS frecuencia_publicacion, 'activo' AS estado_serie,
                   '2025-01-31' AS actualizado_hasta, 'https://www.aduana.gob.bo/' AS url_principal,
                   '/mnt/spim' AS ruta_servidor_spim, 12 AS total_descargas_bruto,
                   4 AS total_reportes_derivados, 2025 AS gestion, 1 AS mes,
                   '{"a" * 64}' AS source_file_sha256
            UNION ALL
            SELECT 'BCB', 'Banco Central', 'D_BO_000000484', 'Tasas diarias',
                   'MANUAL_FILE', '.xlsx', 'Diaria', 'activo', '2025-01-30',
                   'https://www.bcb.gob.bo/', '/mnt/spim', 30, 2, 2025, 1, '{"b" * 64}'
            UNION ALL
            SELECT 'INE', 'INE Bolivia', 'D_BO_000000016', 'IPC Mensual',
                   'AUTOMATIZADO_DAG', '.xlsx', 'Mensual', 'activo', '2025-01-29',
                   'https://www.ine.gob.bo/', '/mnt/spim', 25, 3, 2025, 1, '{"c" * 64}'
        ) TO '{pq_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    con.close()

    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)

    res_search = adapter.execute_query(
        AnalyticsQueryRequest(datamart="catalogo_series_files", search="d_bo_000000012")
    )
    assert res_search.total_records == 1
    assert res_search.records[0]["codigo_serie"] == "D_BO_000000012"

    res_page = adapter.execute_query(
        AnalyticsQueryRequest(datamart="catalogo_series_files", limit=2, offset=1)
    )
    assert res_page.total_records == 3
    assert len(res_page.records) == 2


def test_analytics_search_spim_code_in_robots_and_datasets(tmp_path) -> None:
    """Verifica búsqueda por código SPIM (ej. S_BOBBV_44_000580) en Robots y Datasets SPIM."""
    import duckdb
    from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
        DuckDBAnalyticsAdapter,
    )
    from prospector_interno.domain.contracts.analytics_spec import AnalyticsQueryRequest

    robots_dir = tmp_path / "catalogo_series_files" / "gestion=2025" / "mes=12"
    robots_dir.mkdir(parents=True, exist_ok=True)
    spim_dir = tmp_path / "bases_datos_cubos" / "gestion=2025" / "mes=12"
    spim_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute(
        f"""
        COPY (
            SELECT 'BBV' AS institucion_id, 'Bolsa Boliviana de Valores' AS institucion_nombre,
                   'D_BO_000000417' AS codigo_serie, 'S_BOBBV_44_000580' AS codigos_spim,
                   'Inversionistas Institucionales' AS nombre_serie,
                   'MANUAL_FILE' AS tipo_serie, '.pdf' AS formato_archivo,
                   'Mensual' AS frecuencia_publicacion, 'Actualizado' AS estado_serie,
                   '2026-03-31' AS actualizado_hasta, 'https://www2.bbv.com.bo/' AS url_principal,
                   '/mnt/spim' AS ruta_servidor_spim, 10 AS total_descargas_bruto,
                   1 AS total_reportes_derivados, 2025 AS gestion, 12 AS mes,
                   '{"a" * 64}' AS source_file_sha256
        ) TO '{robots_dir / "part_0.parquet"}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT 'BBV' AS institucion_id, 'BBV' AS institucion_nombre,
                   'S_BOBBV_44_000580' AS codigo_base,
                   'OLD166 Valores Inscritos en Bolsa por Emisor' AS nombre_base,
                   'Diario' AS frecuencia_datos, 'Mensual' AS frecuencia_actualizacion,
                   '-' AS datos_desde, '2024-06-30' AS actualizado_hasta,
                   '-' AS proxima_actualizacion, 'ACTUALIZADO' AS estado_cubo,
                   'D_BO_000000417_01' AS reportes_insumo, 86 AS total_actualizaciones,
                   'https://www2.bbv.com.bo/invers.pdf' AS ultima_url_origen,
                   2025 AS gestion, 12 AS mes, '{"b" * 64}' AS source_file_sha256
        ) TO '{spim_dir / "part_0.parquet"}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    con.close()

    adapter = DuckDBAnalyticsAdapter(base_gold_path=tmp_path)

    res_robot = adapter.execute_query(
        AnalyticsQueryRequest(datamart="catalogo_series_files", search="S_BOBBV_44_000580")
    )
    assert res_robot.total_records == 1
    assert res_robot.records[0]["codigo_serie"] == "D_BO_000000417"
    assert res_robot.records[0]["codigos_spim"] == "S_BOBBV_44_000580"

    res_spim = adapter.execute_query(
        AnalyticsQueryRequest(datamart="bases_datos_cubos", search="s_bobbv_44_000580")
    )
    assert res_spim.total_records == 1
    assert res_spim.records[0]["codigo_base"] == "S_BOBBV_44_000580"
    assert res_spim.records[0]["institucion_nombre"] == "BBV"
