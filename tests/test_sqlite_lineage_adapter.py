"""Pruebas unitarias para el adaptador de persistencia SQLite de Linaje (HUI-17)."""

from pathlib import Path
import tempfile
import pytest

from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.domain.contracts.lineage_spec import (
    DataLineageRecord,
    InternalAssetRecord,
    StorageLayer,
)


@pytest.fixture
def memory_repo() -> SqliteLineageAdapter:
    """Fixture que provee un repositorio SQLite en memoria aislado."""
    repo = SqliteLineageAdapter(":memory:")
    yield repo
    repo.close()


def test_sqlite_record_and_get_asset(
    memory_repo: SqliteLineageAdapter,
) -> None:
    """Verifica la inserción y recuperación exacta de un activo."""
    sha = "e" * 64
    asset = InternalAssetRecord(
        asset_id="gold_mart_cartera_2024_12",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2024-12-01",
        period_end="2024-12-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold/mart_cartera/gestion=2024/mes=12",
        content_hash_sha256=sha,
        row_count=85,
        size_bytes=40960,
    )
    memory_repo.record_asset(asset)

    retrieved = memory_repo.get_asset("gold_mart_cartera_2024_12")
    assert retrieved is not None
    assert retrieved.asset_id == "gold_mart_cartera_2024_12"
    assert retrieved.source_id == "finrural"
    assert retrieved.storage_layer == StorageLayer.GOLD
    assert retrieved.content_hash_sha256 == sha
    assert retrieved.row_count == 85
    assert retrieved.size_bytes == 40960


def test_sqlite_get_asset_not_found(
    memory_repo: SqliteLineageAdapter,
) -> None:
    """Verifica que un activo inexistente retorne None."""
    assert memory_repo.get_asset("no_existe") is None


def test_sqlite_upsert_asset(memory_repo: SqliteLineageAdapter) -> None:
    """Valida la idempotencia y actualización de activo por asset_id."""
    sha1 = "1" * 64
    sha2 = "2" * 64
    asset1 = InternalAssetRecord(
        asset_id="asset_test",
        source_id="asfi",
        dataset_id="boletin",
        period_start="2025-01-01",
        period_end="2025-01-31",
        storage_layer=StorageLayer.SILVER,
        file_path="old/path.parquet",
        content_hash_sha256=sha1,
        row_count=10,
        size_bytes=1000,
    )
    memory_repo.record_asset(asset1)

    asset2 = InternalAssetRecord(
        asset_id="asset_test",
        source_id="asfi",
        dataset_id="boletin",
        period_start="2025-01-01",
        period_end="2025-01-31",
        storage_layer=StorageLayer.SILVER,
        file_path="new/path.parquet",
        content_hash_sha256=sha2,
        row_count=15,
        size_bytes=1500,
    )
    memory_repo.record_asset(asset2)

    updated = memory_repo.get_asset("asset_test")
    assert updated is not None
    assert updated.file_path == "new/path.parquet"
    assert updated.content_hash_sha256 == sha2
    assert updated.row_count == 15


def test_sqlite_list_assets_with_filters(
    memory_repo: SqliteLineageAdapter,
) -> None:
    """Verifica el filtrado de activos por capa de almacenamiento y fuente."""
    for i, layer in enumerate(
        [StorageLayer.BRONZE, StorageLayer.SILVER, StorageLayer.GOLD]
    ):
        memory_repo.record_asset(
            InternalAssetRecord(
                asset_id=f"asset_{i}",
                source_id="finrural" if i < 2 else "asfi",
                dataset_id="ds",
                period_start=f"2025-0{i+1}-01",
                period_end=f"2025-0{i+1}-28",
                storage_layer=layer,
                file_path=f"path_{i}",
                content_hash_sha256=f"{i}" * 64,
                size_bytes=1000 * (i + 1),
            )
        )

    all_assets = memory_repo.list_assets()
    assert len(all_assets) == 3

    gold_assets = memory_repo.list_assets(storage_layer="GOLD")
    assert len(gold_assets) == 1
    assert gold_assets[0].asset_id == "asset_2"

    finrural_assets = memory_repo.list_assets(source_id="finrural")
    assert len(finrural_assets) == 2


def test_sqlite_record_and_get_lineage(
    memory_repo: SqliteLineageAdapter,
) -> None:
    """Verifica la inserción y consulta de enlaces de linaje hacia Bronze."""
    # Primero registrar el asset padre requerido por FK
    gold_sha = "9" * 64
    bronze_sha = "8" * 64
    asset = InternalAssetRecord(
        asset_id="gold_mart_cartera_2025_01",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-01-01",
        period_end="2025-01-31",
        storage_layer=StorageLayer.GOLD,
        file_path="gold/path.parquet",
        content_hash_sha256=gold_sha,
        size_bytes=5000,
    )
    memory_repo.record_asset(asset)

    lineage = DataLineageRecord(
        lineage_id="lin_001",
        gold_asset_id="gold_mart_cartera_2025_01",
        bronze_source_file="finrural_2025_01.pdf",
        bronze_file_sha256=bronze_sha,
        external_run_id="RUN_EXT_202501",
        pipeline_version="v1.2.0",
    )
    memory_repo.record_lineage(lineage)

    records = memory_repo.get_lineage_by_asset("gold_mart_cartera_2025_01")
    assert len(records) == 1
    assert records[0].lineage_id == "lin_001"
    assert records[0].bronze_source_file == "finrural_2025_01.pdf"
    assert records[0].bronze_file_sha256 == bronze_sha
    assert records[0].external_run_id == "RUN_EXT_202501"

    by_checksum = memory_repo.get_lineage_by_checksum(bronze_sha)
    assert len(by_checksum) == 1
    assert by_checksum[0].gold_asset_id == "gold_mart_cartera_2025_01"


def test_sqlite_file_persistence() -> None:
    """Verifica persistencia efectiva en archivo físico en disco."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = Path(tmp_dir) / "test_catalog.db"
        repo1 = SqliteLineageAdapter(db_file)
        repo1.record_asset(
            InternalAssetRecord(
                asset_id="persisted_asset",
                source_id="finrural",
                dataset_id="test",
                period_start="2025-01-01",
                period_end="2025-01-31",
                storage_layer=StorageLayer.BRONZE,
                file_path="file.pdf",
                content_hash_sha256="7" * 64,
                size_bytes=1200,
            )
        )
        repo1.close()

        # Reabrir en nueva instancia y verificar presencia
        repo2 = SqliteLineageAdapter(db_file)
        retrieved = repo2.get_asset("persisted_asset")
        assert retrieved is not None
        assert retrieved.content_hash_sha256 == "7" * 64
        repo2.close()
