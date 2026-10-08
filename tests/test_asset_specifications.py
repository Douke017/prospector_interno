"""Pruebas unitarias para el Inventario de Activos con Specification Pattern (HUI-15).

Verifica la evaluación de predicados en memoria, la compilación de filtros SQL
para SQLite, la composición lógica (&, |, ~) y la integración del servicio
y endpoints API de búsqueda avanzada.
"""

from datetime import datetime, timezone
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.catalog_router import (
    set_asset_catalog_service,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.catalog.asset_catalog_service import (
    AssetCatalogService,
)
from prospector_interno.domain.catalog.specifications import (
    AssetSpecification,
)

from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.exceptions import EntityNotFoundError


@pytest.fixture
def sample_assets() -> list[InternalAssetRecord]:
    """Genera activos de muestra para pruebas de especificación."""
    now = datetime.now(timezone.utc)
    return [
        InternalAssetRecord(
            asset_id="gold_mart_cartera_2024_06",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2024-06-01",
            period_end="2024-06-30",
            storage_layer=StorageLayer.GOLD,
            file_path="gold/mart_cartera/gestion=2024/mes=06/part.parquet",
            content_hash_sha256="a" * 64,
            row_count=1200,
            size_bytes=45000,
            created_at=now,
            last_updated_at=now,
        ),
        InternalAssetRecord(
            asset_id="gold_mart_cartera_2025_05",
            source_id="finrural",
            dataset_id="mart_cartera",
            period_start="2025-05-01",
            period_end="2025-05-31",
            storage_layer=StorageLayer.GOLD,
            file_path="gold/mart_cartera/gestion=2025/mes=05/part.parquet",
            content_hash_sha256="b" * 64,
            row_count=1450,
            size_bytes=52000,
            created_at=now,
            last_updated_at=now,
        ),
        InternalAssetRecord(
            asset_id="bronze_asfi_2023_12",
            source_id="asfi",
            dataset_id="boletin_mensual",
            period_start="2023-12-01",
            period_end="2023-12-31",
            storage_layer=StorageLayer.BRONZE,
            file_path="bronze/asfi/2023/boletin_12.pdf",
            content_hash_sha256="c" * 64,
            row_count=None,
            size_bytes=890000,
            created_at=now,
            last_updated_at=now,
        ),
        InternalAssetRecord(
            asset_id="silver_bbv_2025_01",
            source_id="bbv",
            dataset_id="operaciones_rueda",
            period_start="2025-01-01",
            period_end="2025-01-31",
            storage_layer=StorageLayer.SILVER,
            file_path="silver/bbv/2025/rueda_01.parquet",
            content_hash_sha256="d" * 64,
            row_count=320,
            size_bytes=15000,
            created_at=now,
            last_updated_at=now,
        ),
    ]


def test_in_memory_specification_evaluation(
    sample_assets: list[InternalAssetRecord],
) -> None:
    """Verifica evaluación precisa en memoria del Specification Pattern."""
    # Criterio 1: by_source("finrural").in_range("2024-01-01", "2025-12-31")
    spec_crit1 = AssetSpecification.by_source("finrural").in_range(
        "2024-01-01", "2025-12-31"
    )
    matched = [a for a in sample_assets if spec_crit1.is_satisfied_by(a)]
    assert len(matched) == 2
    assert {a.asset_id for a in matched} == {
        "gold_mart_cartera_2024_06",
        "gold_mart_cartera_2025_05",
    }

    # Filtro por capa
    spec_gold = AssetSpecification.by_layer("GOLD")
    assert sum(spec_gold.is_satisfied_by(a) for a in sample_assets) == 2

    # Filtro por mínimo de filas
    spec_rows = AssetSpecification.with_min_rows(1000)
    assert sum(spec_rows.is_satisfied_by(a) for a in sample_assets) == 2

    # Composición con operadores bitwise (&, |, ~)
    spec_composed = (
        AssetSpecification.by_source("finrural")
        & AssetSpecification.by_layer(StorageLayer.GOLD)
        & ~AssetSpecification.in_range("2025-01-01", "2025-12-31")
    )
    matched_composed = [a for a in sample_assets if spec_composed.is_satisfied_by(a)]
    assert len(matched_composed) == 1
    assert matched_composed[0].asset_id == "gold_mart_cartera_2024_06"


def test_sqlite_specification_query(
    tmp_path: Path,
    sample_assets: list[InternalAssetRecord],
) -> None:
    """Certifica Criterio 1: consulta contra base de datos SQLite indexada."""
    db_file = tmp_path / "test_spec_catalog.db"
    adapter = SqliteLineageAdapter(db_file)

    for asset in sample_assets:
        adapter.record_asset(asset)

    service = AssetCatalogService(adapter)

    # Criterio 1 exacto
    spec = AssetSpecification.by_source("finrural").in_range(
        "2024-01-01", "2025-12-31"
    )
    results = service.find_assets(spec)

    assert len(results) == 2
    assert results[0].source_id == "finrural"
    assert results[1].source_id == "finrural"
    assert results[0].period_start >= "2024-01-01"
    assert results[1].period_end <= "2025-12-31"

    # Conteo con especificación
    count = service.count_assets(AssetSpecification.by_source("asfi"))
    assert count == 1

    # Resumen de inventario
    summary = service.get_inventory_summary()
    assert summary["total_assets"] == 4
    assert summary["layers_distribution"]["GOLD"] == 2
    assert summary["layers_distribution"]["BRONZE"] == 1
    assert summary["layers_distribution"]["SILVER"] == 1
    assert summary["sources_distribution"]["finrural"] == 2
    assert summary["total_row_count"] == 1200 + 1450 + 320
    assert summary["earliest_period"] == "2023-12-01"
    assert summary["latest_period"] == "2025-05-31"


def test_asset_catalog_service_get_by_id(
    tmp_path: Path,
    sample_assets: list[InternalAssetRecord],
) -> None:
    """Verifica obtención por ID y excepción si no existe."""
    adapter = SqliteLineageAdapter(tmp_path / "test_get_id.db")

    adapter.record_asset(sample_assets[0])
    service = AssetCatalogService(adapter)

    found = service.get_asset_by_id("gold_mart_cartera_2024_06")
    assert found.asset_id == "gold_mart_cartera_2024_06"

    with pytest.raises(EntityNotFoundError):
        service.get_asset_by_id("id_inexistente")


def test_api_assets_search_and_summary_endpoints(
    tmp_path: Path,
    sample_assets: list[InternalAssetRecord],
) -> None:
    """Verifica endpoints REST /assets/search y /assets/summary."""
    adapter = SqliteLineageAdapter(tmp_path / "test_api_spec.db")
    for asset in sample_assets:
        adapter.record_asset(asset)

    service = AssetCatalogService(adapter)
    set_asset_catalog_service(service)

    client = TestClient(app)

    # 1. Búsqueda con parámetros combinados
    response = client.get(
        "/api/v1/catalog/assets/search",
        params={
            "source_id": "finrural",
            "start_date": "2024-01-01",
            "end_date": "2025-12-31",
            "min_rows": 1300,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["asset_id"] == "gold_mart_cartera_2025_05"
    assert data[0]["row_count"] == 1450

    # 2. Resumen de inventario
    sum_resp = client.get("/api/v1/catalog/assets/summary")
    assert sum_resp.status_code == 200
    sum_data = sum_resp.json()
    assert sum_data["total_assets"] == 4
    assert sum_data["layers_distribution"]["GOLD"] == 2
    assert "earliest_period" in sum_data
