"""Pruebas del Pipeline Medallion: Silver (normalización) y Gold (datamarts)."""

from decimal import Decimal
from pathlib import Path
import duckdb

from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.application.pipelines.silver_pipeline import (
    SilverTransformPipeline,
)
from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    SilverFinancialRecord,
    StatementType,
)
from prospector_interno.domain.models import BronzeArtifact


def _make_silver_record(
    account_name: str,
    amount: Decimal,
    category: AccountCategory = AccountCategory.CARTERA,
) -> SilverFinancialRecord:
    return SilverFinancialRecord(
        source_id="finrural",
        institution_id="CRECER",
        institution_name="Crecer IFD",
        period="2026-01",
        gestion=2026,
        mes=1,
        statement_type=StatementType.SITUACION_PATRIMONIAL,
        account_category=category,
        account_name=account_name,
        amount_bob=amount,
        source_file_sha256="hash_silver_test",
    )


def test_silver_pipeline_ignores_non_existent_file() -> None:
    """Verifica que el pipeline omita artefactos inexistentes."""
    pipeline = SilverTransformPipeline()
    artifact = BronzeArtifact(
        artifact_id="art_1",
        domain="www.finrural.org.bo",
        dataset_code="cuadro_1",
        file_path=Path("/tmp/non_existent_file_xyz.pdf"),
        size_bytes=100,
        sha256_hash="dummy_hash",
        content_type="application/pdf",
    )
    records = pipeline.process_artifact(artifact)
    assert records == []


def test_silver_pipeline_ignores_unsupported_domain() -> None:
    """Verifica que dominios no reconocidos no generen registros."""
    pipeline = SilverTransformPipeline()
    artifact = BronzeArtifact(
        artifact_id="art_2",
        domain="www.desconocido.com",
        dataset_code="datos",
        file_path=Path("/tmp/file.pdf"),
        size_bytes=100,
        sha256_hash="dummy_hash",
        content_type="application/pdf",
    )
    records = pipeline.process_artifact(artifact)
    assert records == []


def test_gold_transformation_calculates_ratios_correctly() -> None:
    """Valida cálculo de cartera en mora y ratios financieros derivados."""
    materializer = GoldMaterializer()
    silver_records = [
        _make_silver_record("CARTERA_BRUTA", Decimal("1000.00")),
        _make_silver_record("CARTERA_VIGENTE", Decimal("900.00")),
        _make_silver_record("CARTERA_VENCIDA", Decimal("60.00")),
        _make_silver_record("CARTERA_EN_EJECUCION", Decimal("40.00")),
        _make_silver_record(
            "PREVISION_CARTERA",
            Decimal("-150.00"),
            AccountCategory.CARTERA,
        ),
        _make_silver_record(
            "TOTAL_ACTIVO",
            Decimal("1200.00"),
            AccountCategory.ACTIVO,
        ),
        _make_silver_record(
            "TOTAL_PASIVO",
            Decimal("800.00"),
            AccountCategory.PASIVO,
        ),
        _make_silver_record(
            "TOTAL_PATRIMONIO",
            Decimal("400.00"),
            AccountCategory.PATRIMONIO,
        ),
    ]

    gold_records = materializer.transform_to_mart_cartera(silver_records)
    assert len(gold_records) == 1
    g = gold_records[0]

    assert g.institucion_id == "CRECER"
    assert g.cartera_bruta == Decimal("1000.00")
    assert g.cartera_en_mora == Decimal("100.00")  # 60 + 40
    assert g.prevision_cartera == Decimal("150.00")  # abs(-150)
    assert g.ratio_mora == Decimal("0.1000")  # 100 / 1000
    assert g.ratio_cobertura == Decimal("1.5000")  # 150 / 100


def test_gold_transformation_zero_division_safety() -> None:
    """Verifica que montos en cero no provoquen excepciones por división por cero."""
    materializer = GoldMaterializer()
    silver_records = [
        _make_silver_record("CARTERA_BRUTA", Decimal("0.00")),
        _make_silver_record("CARTERA_VENCIDA", Decimal("0.00")),
    ]
    gold_records = materializer.transform_to_mart_cartera(silver_records)
    assert len(gold_records) == 1
    g = gold_records[0]
    assert g.ratio_mora == Decimal("0.0000")
    assert g.ratio_cobertura == Decimal("0.0000")


def test_gold_materialize_empty_records(tmp_path: Path) -> None:
    """Verifica materialización sin registros retorna el directorio sin fallar."""
    materializer = GoldMaterializer(base_gold_path=tmp_path)
    res_path = materializer.materialize_mart_cartera([])
    assert res_path == tmp_path / "mart_cartera"
    assert res_path.exists()


def test_gold_materialize_writes_partitioned_parquet(tmp_path: Path) -> None:
    """Verifica la escritura de Parquet particionado con DuckDB."""
    materializer = GoldMaterializer(base_gold_path=tmp_path)
    silver_records = [
        _make_silver_record("CARTERA_BRUTA", Decimal("5000.00")),
        _make_silver_record("CARTERA_VIGENTE", Decimal("4800.00")),
        _make_silver_record("CARTERA_VENCIDA", Decimal("120.00")),
        _make_silver_record("CARTERA_EN_EJECUCION", Decimal("80.00")),
        _make_silver_record("TOTAL_ACTIVO", Decimal("6000.00")),
    ]
    gold_records = materializer.transform_to_mart_cartera(silver_records)
    mart_dir = materializer.materialize_mart_cartera(gold_records)

    assert mart_dir.exists()
    parquet_files = list(mart_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 1

    # Verificar lectura e integridad con DuckDB
    con = duckdb.connect(":memory:")
    glob_pattern = f"{mart_dir.as_posix()}/**/*.parquet"
    res = con.execute(
        f"SELECT count(*), sum(cartera_bruta) FROM read_parquet('{glob_pattern}')"
    ).fetchone()
    assert res is not None
    assert res[0] == 1
    assert res[1] == 5000.0
