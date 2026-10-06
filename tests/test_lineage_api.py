"""Pruebas de integración HTTP para los endpoints de linaje y catálogo (HUI-17)."""

import hashlib
from pathlib import Path
import tempfile
from fastapi.testclient import TestClient
import pytest

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.catalog_router import (
    set_lineage_tracker,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.lineage.lineage_tracker import (
    LineageTracker,
)
from prospector_interno.domain.contracts.lineage_spec import StorageLayer

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_lineage_service() -> tuple[LineageTracker, SqliteLineageAdapter]:
    """Inyecta un servicio de linaje SQLite en memoria aislado para cada test."""
    adapter = SqliteLineageAdapter(":memory:")
    tracker = LineageTracker(adapter)
    set_lineage_tracker(tracker)
    return tracker, adapter


def test_list_assets_empty_and_populated(
    setup_lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica listar activos catalogados con filtros opcionales."""
    tracker, _ = setup_lineage_service

    # Inicialmente vacío
    res = client.get("/api/v1/catalog/assets")
    assert res.status_code == 200
    assert res.json() == []

    # Insertar activo
    tracker.register_asset(
        asset_id="gold_mart_2025_01",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-01-01",
        period_end="2025-01-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold/test.parquet",
        content_hash_sha256="1" * 64,
        row_count=100,
        size_bytes=2048,
    )

    res = client.get("/api/v1/catalog/assets")
    assert res.status_code == 200
    assets = res.json()
    assert len(assets) == 1
    assert assets[0]["asset_id"] == "gold_mart_2025_01"
    assert assets[0]["storage_layer"] == "GOLD"

    # Filtrar por capa
    res_gold = client.get("/api/v1/catalog/assets?storage_layer=GOLD")
    assert res_gold.status_code == 200
    assert len(res_gold.json()) == 1

    res_silver = client.get("/api/v1/catalog/assets?storage_layer=SILVER")
    assert res_silver.status_code == 200
    assert len(res_silver.json()) == 0


def test_get_asset_lineage_success(
    setup_lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica consulta de grafo de linaje y cadena de custodia (HUI-17 Criterio 1)."""
    tracker, _ = setup_lineage_service

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

    res = client.get("/api/v1/catalog/assets/gold_mart_cartera_2025_05/lineage")
    assert res.status_code == 200
    data = res.json()

    assert data["asset"]["asset_id"] == "gold_mart_cartera_2025_05"
    assert data["upstream_bronze_files"] == ["finrural_2025_05.pdf"]
    assert data["upstream_sha256_digests"] == [bronze_sha]
    assert data["external_run_ids"] == ["RUN_20250501_FINRURAL_01"]

    # Verificar que los 4 pasos cronológicos de la cadena de custodia están presentes
    steps = data["provenance_chain"]
    assert len(steps) >= 4
    step_names = [s["step_name"] for s in steps]
    assert "EXTERNAL_DISCOVERY" in step_names
    assert "BRONZE_PERSISTENCE" in step_names
    assert "SILVER_NORMALIZATION" in step_names
    assert "GOLD_MATERIALIZATION" in step_names


def test_get_asset_lineage_not_found() -> None:
    """Verifica respuesta 404 para activo no catalogado."""
    res = client.get("/api/v1/catalog/assets/activo_inexistente/lineage")
    assert res.status_code == 404
    assert "no encontrado" in res.json()["detail"]


def test_verify_asset_integrity_endpoint(
    setup_lineage_service: tuple[LineageTracker, SqliteLineageAdapter],
) -> None:
    """Verifica endpoint de auditoría criptográfica en disco."""
    tracker, _ = setup_lineage_service

    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_path = Path(tmp_dir)
        gold_file = dir_path / "gold.parquet"
        bronze_file = dir_path / "insumo.pdf"

        gold_file.write_bytes(b"GOLD_DATA")
        bronze_file.write_bytes(b"BRONZE_DATA")

        gold_sha = hashlib.sha256(b"GOLD_DATA").hexdigest()
        bronze_sha = hashlib.sha256(b"BRONZE_DATA").hexdigest()

        tracker.register_asset(
            asset_id="asset_to_verify",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-01-01",
            period_end="2025-01-31",
            storage_layer=StorageLayer.GOLD,
            file_path=gold_file,
            content_hash_sha256=gold_sha,
            size_bytes=len(b"GOLD_DATA"),
        )
        tracker.link_bronze_source(
            gold_asset_id="asset_to_verify",
            bronze_source_file=str(bronze_file),
            bronze_file_sha256=bronze_sha,
            external_run_id="RUN_EXT_123",
        )

        res = client.get(
            f"/api/v1/catalog/assets/asset_to_verify/verify?bronze_dir={tmp_dir}"
        )
        assert res.status_code == 200
        data = res.json()
        assert data["asset_id"] == "asset_to_verify"
        assert data["is_valid"] is True
        assert data["all_upstream_intact"] is True
