"""Pruebas unitarias y de integración para LineageTracker (HUI-17)."""

from decimal import Decimal
import hashlib
from pathlib import Path
import tempfile
import pytest

from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.lineage.lineage_tracker import (
    LineageTracker,
)
from prospector_interno.application.pipelines.gold_materializer import (
    GoldMaterializer,
)
from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
)
from prospector_interno.domain.contracts.lineage_spec import StorageLayer
from prospector_interno.domain.exceptions import EntityNotFoundError


@pytest.fixture
def lineage_service() -> tuple[LineageTracker, SqliteLineageAdapter]:
    """Fixture con servicio LineageTracker conectado a SQLite en memoria."""
    adapter = SqliteLineageAdapter(":memory:")
    tracker = LineageTracker(adapter)
    return tracker, adapter


def test_register_asset_calculates_hash_from_disk(
    lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica que register_asset calcule automáticamente el SHA-256 en disco."""
    tracker, _ = lineage_service
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(b"CONTENIDO_REAL_FINANCIERO_GOLD")
        temp_path = Path(f.name)

    expected_sha = hashlib.sha256(b"CONTENIDO_REAL_FINANCIERO_GOLD").hexdigest()

    try:
        asset = tracker.register_asset(
            asset_id="gold_mart_2025_01",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-01-01",
            period_end="2025-01-31",
            storage_layer=StorageLayer.GOLD,
            file_path=temp_path,
            row_count=50,
        )
        assert asset.content_hash_sha256 == expected_sha
        assert asset.size_bytes == len(b"CONTENIDO_REAL_FINANCIERO_GOLD")
    finally:
        temp_path.unlink(missing_ok=True)


def test_link_bronze_source_and_get_graph(
    lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica el grafo de linaje completo y la cadena de custodia de 4 pasos."""
    tracker, _ = lineage_service

    gold_sha = "a" * 64
    bronze_sha = "b" * 64

    tracker.register_asset(
        asset_id="gold_mart_cartera_2025_05",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-05-01",
        period_end="2025-05-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold/mart_cartera/gestion=2025/mes=5/data.parquet",
        content_hash_sha256=gold_sha,
        row_count=20,
        size_bytes=4096,
    )

    tracker.link_bronze_source(
        gold_asset_id="gold_mart_cartera_2025_05",
        bronze_source_file="finrural_2025_05.pdf",
        bronze_file_sha256=bronze_sha,
        external_run_id="RUN_20250501_FINRURAL_01",
        pipeline_version="v1.0.0",
    )

    graph = tracker.get_asset_lineage_graph("gold_mart_cartera_2025_05")

    assert graph.asset.asset_id == "gold_mart_cartera_2025_05"
    assert len(graph.lineage_records) == 1
    assert graph.upstream_bronze_files == ["finrural_2025_05.pdf"]
    assert graph.upstream_sha256_digests == [bronze_sha]
    assert graph.external_run_ids == ["RUN_20250501_FINRURAL_01"]

    # Verificar pasos de la cadena de custodia
    step_names = [s.step_name for s in graph.provenance_chain]
    assert "EXTERNAL_DISCOVERY" in step_names
    assert "BRONZE_PERSISTENCE" in step_names
    assert "SILVER_NORMALIZATION" in step_names
    assert "GOLD_MATERIALIZATION" in step_names


def test_get_graph_not_found_raises_entity_not_found(
    lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica que consultar un activo inexistente levante EntityNotFoundError."""
    tracker, _ = lineage_service
    with pytest.raises(EntityNotFoundError):
        tracker.get_asset_lineage_graph("activo_inexistente")


def test_verify_asset_integrity_success_and_tampered(
    lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica detección de archivos auténticos vs archivos alterados en disco."""
    tracker, _ = lineage_service

    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_path = Path(tmp_dir)
        gold_file = dir_path / "gold.parquet"
        bronze_file = dir_path / "bronze.pdf"

        gold_content = b"ORIGINAL_GOLD_CONTENT"
        bronze_content = b"ORIGINAL_BRONZE_PDF"

        gold_file.write_bytes(gold_content)
        bronze_file.write_bytes(bronze_content)

        gold_sha = hashlib.sha256(gold_content).hexdigest()
        bronze_sha = hashlib.sha256(bronze_content).hexdigest()

        tracker.register_asset(
            asset_id="audit_asset",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-01-01",
            period_end="2025-01-31",
            storage_layer=StorageLayer.GOLD,
            file_path=gold_file,
            content_hash_sha256=gold_sha,
            size_bytes=len(gold_content),
        )

        tracker.link_bronze_source(
            gold_asset_id="audit_asset",
            bronze_source_file=str(bronze_file),
            bronze_file_sha256=bronze_sha,
            external_run_id="RUN_AUDIT_01",
        )

        # 1. Auditoría exitosa
        result_valid = tracker.verify_asset_integrity("audit_asset")
        assert result_valid.is_valid is True
        assert result_valid.all_upstream_intact is True

        # 2. Modificar archivo Bronze (simular corrupción/alteración)
        bronze_file.write_bytes(b"CONTENIDO_ALTERADO_HACK")
        result_tampered = tracker.verify_asset_integrity("audit_asset")
        assert result_tampered.is_valid is False
        assert result_tampered.all_upstream_intact is False


def test_gold_materializer_automatic_lineage_tracking(
    lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Prueba de integración: GoldMaterializer registra linaje automáticamente."""
    tracker, adapter = lineage_service

    with tempfile.TemporaryDirectory() as tmp_dir:
        gold_path = Path(tmp_dir) / "gold"
        materializer = GoldMaterializer(
            base_gold_path=gold_path,
            lineage_tracker=tracker,
        )

        dummy_sha = "f" * 64
        record = GoldMartCarteraRecord(
            gestion=2025,
            mes=8,
            institucion_id="CRECER",
            institucion_nombre="Crecer IFD",
            cartera_bruta=Decimal("1000.00"),
            cartera_vigente=Decimal("950.00"),
            cartera_vencida=Decimal("30.00"),
            cartera_en_ejecucion=Decimal("20.00"),
            cartera_en_mora=Decimal("50.00"),
            prevision_cartera=Decimal("40.00"),
            total_activo=Decimal("1500.00"),
            total_pasivo=Decimal("1200.00"),
            total_patrimonio=Decimal("300.00"),
            ratio_mora=Decimal("0.0500"),
            ratio_cobertura=Decimal("0.8000"),
            source_file_sha256=dummy_sha,
        )

        materializer.materialize_mart_cartera(
            gold_records=[record],
            external_run_id="RUN_TEST_MATERIALIZATION_01",
        )

        asset_id = "gold_mart_cartera_2025_08"
        asset = adapter.get_asset(asset_id)
        assert asset is not None
        assert asset.source_id == "finrural"
        assert asset.storage_layer == StorageLayer.GOLD
        assert asset.row_count == 1

        lineage_records = adapter.get_lineage_by_asset(asset_id)
        assert len(lineage_records) == 1
        assert lineage_records[0].bronze_file_sha256 == dummy_sha
        assert (
            lineage_records[0].external_run_id
            == "RUN_TEST_MATERIALIZATION_01"
        )
