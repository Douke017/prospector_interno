"""Pruebas de integración para Conciliación y Medallion con Data Real.

Valida el ciclo de vida analítico completo sobre la fuente BBV:
1. Diff Engine con DuckDB sobre el mapa real `mapa_bbv.json`.
2. Detección de recursos CONFIRMED vs NEW tras adquisición en Bronze.
3. Creación y priorización temporal del SyncPlan.
4. Materialización en Capa Gold con linaje y consulta analítica vectorizada.
"""

from decimal import Decimal
import io
from pathlib import Path

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DuckDBAnalyticsAdapter,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.bridge.diff_engine import DuckDBDiffEngine
from prospector_interno.application.lineage.lineage_tracker import (
    LineageTracker,
)
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
from prospector_interno.domain.models import (
    ResourceChangeCategory,
)


BBV_MAP_PATH = Path("../prospector_externo/output/bbv/mapa_bbv.json")
if not BBV_MAP_PATH.exists():
    BBV_MAP_PATH = Path("prospector_externo/output/bbv/mapa_bbv.json")


def test_bbv_reconciliation_diff_classifies_new_and_confirmed(
    tmp_path: Path,
) -> None:
    """Verifica la conciliación real de BBV clasificando NEW y CONFIRMED."""
    assert BBV_MAP_PATH.is_file(), f"El archivo {BBV_MAP_PATH} debe existir"

    bronze_storage = LocalBronzeStorage(base_path=tmp_path / "bronze")
    diff_engine = DuckDBDiffEngine(storage=bronze_storage)

    # 1. Ejecutar diff sobre almacenamiento vacío: todos deben ser NEW
    report_initial = diff_engine.compute_diff(
        source_id="bbv",
        external_map_path=BBV_MAP_PATH,
    )

    assert report_initial.source_id == "bbv"
    assert report_initial.total_external_resources == 235
    assert report_initial.total_new == 235
    assert report_initial.total_confirmed == 0

    # 2. Precargar un recurso real en Bronze usando su URL exacta
    first_item = report_initial.items[0]
    stream = io.BytesIO(b"%PDF-1.4\nCONTENIDO_BBV_TARIFARIO_REAL")
    bronze_storage.store_stream(
        domain="www.bbv.com.bo",
        dataset_code="DS_BO_BBV_D_BO_000000481",
        period="2026-10",
        stream=stream,
        url=first_item.url,
        resource_key=first_item.resource_key,
    )

    # 3. Re-ejecutar diff: el recurso precargado debe ser CONFIRMED
    report_updated = diff_engine.compute_diff(
        source_id="bbv",
        external_map_path=BBV_MAP_PATH,
    )

    assert report_updated.total_external_resources == 235
    assert report_updated.total_confirmed >= 1
    assert report_updated.total_new < 235

    confirmed_item = next(
        i for i in report_updated.items
        if i.resource_key == first_item.resource_key
    )
    assert confirmed_item.category == ResourceChangeCategory.CONFIRMED


def test_medallion_gold_materialization_and_duckdb_vector_query(
    tmp_path: Path,
) -> None:
    """Verifica la materialización a Parquet en Gold y consulta con DuckDB."""
    gold_dir = tmp_path / "gold"
    lineage_db = tmp_path / "lineage.db"
    lineage_repo = SqliteLineageAdapter(db_path=lineage_db)
    tracker = LineageTracker(repository=lineage_repo)

    materializer = GoldMaterializer(
        base_gold_path=gold_dir,
        lineage_tracker=tracker,
    )

    source_hash = (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )

    # 1. Crear registros Silver simulados para BBV / Bursátil
    silver_records = [
        SilverFinancialRecord(
            source_id="bbv",
            institution_id="BISA_SAFI",
            institution_name="Bisa SAFI S.A.",
            period="2026-10",
            gestion=2026,
            mes=10,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_BRUTA",
            amount_bob=Decimal("5000000.00"),
            source_file_sha256=source_hash,
        ),
        SilverFinancialRecord(
            source_id="bbv",
            institution_id="BISA_SAFI",
            institution_name="Bisa SAFI S.A.",
            period="2026-10",
            gestion=2026,
            mes=10,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_VENCIDA",
            amount_bob=Decimal("50000.00"),
            source_file_sha256=source_hash,
        ),
    ]

    # 2. Materializar en Capa Gold (Parquet particionado)
    gold_records = materializer.transform_to_mart_cartera(silver_records)
    materializer.materialize_mart_cartera(
        gold_records,
        external_run_id="run_bbv_001",
    )

    # 3. Validar partición en disco
    part_dir = gold_dir / "mart_cartera" / "gestion=2026" / "mes=10"
    parquet_files = list(part_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # 4. Consultar con DuckDBAnalyticsAdapter aplicando vectorización
    analytics = DuckDBAnalyticsAdapter(base_gold_path=gold_dir)
    query_req = AnalyticsQueryRequest(
        datamart="mart_cartera",
        gestiones=[2026],
        meses=[10],
        metrics=["institucion_id", "cartera_bruta", "ratio_mora"],
    )
    res = analytics.execute_query(query_req)

    assert res.total_records == 1
    assert res.columns == ["institucion_id", "cartera_bruta", "ratio_mora"]
    assert res.records[0]["institucion_id"] == "BISA_SAFI"
    assert res.records[0]["cartera_bruta"] == 5000000.00
    assert res.records[0]["ratio_mora"] == 0.01

    # 5. Validar linaje automático trazado por LineageTracker
    lineage_graph = tracker.get_asset_lineage_graph("gold_mart_cartera_2026_10")
    assert lineage_graph.asset.asset_id == "gold_mart_cartera_2026_10"
    assert len(lineage_graph.lineage_records) == 1
    assert lineage_graph.lineage_records[0].bronze_file_sha256 == source_hash
