"""Pruebas de especificación para contratos de linaje y catálogo (SDD HUI-17)."""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from prospector_interno.domain.contracts.lineage_spec import (
    AssetLineageGraphResponse,
    DataLineageRecord,
    InternalAssetRecord,
    LineageIntegrityCheckResult,
    ProvenanceStep,
    StorageLayer,
)


def test_internal_asset_record_valid() -> None:
    """Valida la creación correcta de un registro canónico de activo."""
    now = datetime.now(timezone.utc)
    dummy_sha = "a" * 64
    asset = InternalAssetRecord(
        asset_id="gold_mart_cartera_2025_05",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-05-01",
        period_end="2025-05-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold/mart_cartera/gestion=2025/mes=5/data.parquet",
        content_hash_sha256=dummy_sha,
        row_count=150,
        size_bytes=10240,
        created_at=now,
        last_updated_at=now,
    )
    assert asset.asset_id == "gold_mart_cartera_2025_05"
    assert asset.storage_layer == StorageLayer.GOLD
    assert asset.row_count == 150
    assert asset.content_hash_sha256 == dummy_sha


def test_internal_asset_record_forbids_extra_fields() -> None:
    """Verifica que el contrato del activo rechace campos arbitrarios no tipados."""
    with pytest.raises(ValidationError):
        InternalAssetRecord(
            asset_id="gold_asset",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-05-01",
            period_end="2025-05-31",
            storage_layer=StorageLayer.GOLD,
            file_path="path",
            content_hash_sha256="a" * 64,
            size_bytes=100,
            campo_desconocido="hack",
        )


def test_internal_asset_record_validates_sha256_length() -> None:
    """Valida que el hash de contenido tenga estrictamente 64 caracteres hex."""
    with pytest.raises(ValidationError):
        InternalAssetRecord(
            asset_id="gold_asset",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-05-01",
            period_end="2025-05-31",
            storage_layer=StorageLayer.GOLD,
            file_path="path",
            content_hash_sha256="hash_invalido_corto",
            size_bytes=100,
        )


def test_data_lineage_record_valid() -> None:
    """Valida la creación inmutable de un nodo de enlace de linaje."""
    dummy_sha = "b" * 64
    lineage = DataLineageRecord(
        lineage_id="lin_node_001",
        gold_asset_id="gold_mart_cartera_2025_05",
        bronze_source_file="finrural_2025_05.pdf",
        bronze_file_sha256=dummy_sha,
        external_run_id="RUN_20250501_FINRURAL_01",
        pipeline_version="v1.0.0",
    )
    assert lineage.lineage_id == "lin_node_001"
    assert lineage.external_run_id == "RUN_20250501_FINRURAL_01"
    assert lineage.bronze_file_sha256 == dummy_sha


def test_data_lineage_record_forbids_extra_fields() -> None:
    """Verifica que el nodo de linaje rechace campos adicionales."""
    with pytest.raises(ValidationError):
        DataLineageRecord(
            lineage_id="lin_001",
            gold_asset_id="gold_01",
            bronze_source_file="doc.pdf",
            bronze_file_sha256="c" * 64,
            external_run_id="run_1",
            pipeline_version="v1.0.0",
            extra="malicious",
        )


def test_asset_lineage_graph_response_model() -> None:
    """Valida la respuesta consolidada del grafo de linaje y cadena de custodia."""
    sha_gold = "1" * 64
    sha_bronze = "2" * 64
    now = datetime.now(timezone.utc)

    asset = InternalAssetRecord(
        asset_id="gold_asset_1",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-01-01",
        period_end="2025-01-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold.parquet",
        content_hash_sha256=sha_gold,
        row_count=10,
        size_bytes=5000,
    )
    lineage_record = DataLineageRecord(
        lineage_id="lin_1",
        gold_asset_id="gold_asset_1",
        bronze_source_file="bronze_1.pdf",
        bronze_file_sha256=sha_bronze,
        external_run_id="EXT_RUN_99",
        pipeline_version="v1.0.0",
    )
    step = ProvenanceStep(
        step_order=1,
        step_name="BRONZE_INGESTION",
        component="Downloader",
        timestamp=now,
        input_digest=None,
        output_digest=sha_bronze,
        metadata={"url": "https://test.com/file.pdf"},
    )

    graph = AssetLineageGraphResponse(
        asset=asset,
        lineage_records=[lineage_record],
        upstream_bronze_files=["bronze_1.pdf"],
        upstream_sha256_digests=[sha_bronze],
        external_run_ids=["EXT_RUN_99"],
        provenance_chain=[step],
    )

    assert graph.asset.asset_id == "gold_asset_1"
    assert len(graph.lineage_records) == 1
    assert graph.upstream_sha256_digests == [sha_bronze]
    assert len(graph.provenance_chain) == 1


def test_lineage_integrity_check_result_model() -> None:
    """Valida el modelo de auditoría de integridad física."""
    res = LineageIntegrityCheckResult(
        asset_id="gold_asset_1",
        is_valid=True,
        recorded_sha256="d" * 64,
        current_disk_sha256="d" * 64,
        all_upstream_intact=True,
        upstream_details=[{"file": "source.pdf", "is_intact": True}],
        verification_message="Todo íntegro",
    )
    assert res.is_valid is True
    assert res.all_upstream_intact is True
