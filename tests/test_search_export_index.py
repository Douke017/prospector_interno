"""Pruebas unitarias y de integración para el Contrato Canónico de Búsqueda (HUI-20)."""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.search_router import (
    set_search_index_service,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.search.search_index_service import (
    SearchIndexService,
)
from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.contracts.search_spec import (
    UnifiedSearchDocument,
)


@pytest.fixture
def in_memory_sqlite_repo() -> SqliteLineageAdapter:
    """Crea una base de datos SQLite en memoria para tests."""
    return SqliteLineageAdapter(Path(":memory:"))


@pytest.fixture
def search_service(
    in_memory_sqlite_repo: SqliteLineageAdapter,
) -> SearchIndexService:
    """Instancia del servicio de búsqueda con repositorio en memoria."""
    return SearchIndexService(in_memory_sqlite_repo)


@pytest.fixture
def sample_gold_asset() -> InternalAssetRecord:
    """Crea un registro de activo Gold para pruebas."""
    now = datetime.now(timezone.utc)
    return InternalAssetRecord(
        asset_id="gold_mart_cartera_2025_05",
        source_id="finrural",
        dataset_id="mart_cartera",
        period_start="2025-05-01",
        period_end="2025-05-31",
        storage_layer=StorageLayer.GOLD,
        file_path="data/gold/mart_cartera/gestion=2025/mes=05/part.parquet",
        content_hash_sha256="a" * 64,
        row_count=120,
        size_bytes=45000,
        created_at=now,
        last_updated_at=now,
    )


@pytest.fixture
def sample_bronze_asset() -> InternalAssetRecord:
    """Crea un registro de activo Bronze para pruebas."""
    now = datetime.now(timezone.utc)
    return InternalAssetRecord(
        asset_id="bronze_finrural_2025_05_pdf",
        source_id="finrural",
        dataset_id="boletin_mensual",
        period_start="2025-05-01",
        period_end="2025-05-31",
        storage_layer=StorageLayer.BRONZE,
        file_path="data/bronze/finrural/2025/boletin_05.pdf",
        content_hash_sha256="b" * 64,
        row_count=None,
        size_bytes=1048576,
        created_at=now - timedelta(days=2),
        last_updated_at=now - timedelta(days=2),
    )


def test_unified_search_document_validation(
    sample_gold_asset: InternalAssetRecord,
) -> None:
    """Verifica la validación estricta y formato de UnifiedSearchDocument."""
    doc = UnifiedSearchDocument(
        search_id="finrural:mart_cartera:2025-05-31:aaaaaaaaaaaaaaaa",
        origin="INTERNAL_GOLD",
        source_id="finrural",
        title="FINRURAL - Mart Cartera (2025-05-31)",
        document_type="PARQUET",
        period_end="2025-05-31",
        sha256="a" * 64,
        local_path=sample_gold_asset.file_path,
        gold_table="mart_cartera",
        entities_mentioned=["CRECER", "PRO MUJER"],
        tags=["finrural", "mart_cartera", "gold"],
    )
    assert doc.search_id == "finrural:mart_cartera:2025-05-31:aaaaaaaaaaaaaaaa"
    assert doc.origin == "INTERNAL_GOLD"
    assert doc.document_type == "PARQUET"
    assert len(doc.entities_mentioned) == 2


def test_unified_search_document_forbids_extra_fields() -> None:
    """Verifica que el contrato prohíbe campos no definidos (extra=forbid)."""
    with pytest.raises(ValidationError):
        UnifiedSearchDocument(
            search_id="test:1",
            origin="INTERNAL_GOLD",
            source_id="test",
            title="Test",
            document_type="PDF",
            period_end="2025-05-31",
            sha256="c" * 64,
            unexpected_field="disallowed",  # type: ignore
        )


def test_unified_search_document_validates_sha256_length() -> None:
    """Verifica que el hash SHA-256 debe tener exactamente 64 caracteres."""
    with pytest.raises(ValidationError):
        UnifiedSearchDocument(
            search_id="test:1",
            origin="INTERNAL_GOLD",
            source_id="test",
            title="Test",
            document_type="PDF",
            period_end="2025-05-31",
            sha256="short_hash",
        )


def test_search_service_build_search_id_format(
    search_service: SearchIndexService,
) -> None:
    """Criterio 1: Identificador canónico source_id:dataset_id:period:sha256[:16]."""
    full_sha = "abcdef1234567890" + "0" * 48
    search_id = search_service.build_search_id(
        source_id="finrural",
        dataset_id="reporte_mensual",
        period_end="2025-05-31",
        sha256=full_sha,
    )
    assert search_id == "finrural:reporte_mensual:2025-05-31:abcdef1234567890"


def test_search_service_project_gold_document(
    search_service: SearchIndexService,
    sample_gold_asset: InternalAssetRecord,
) -> None:
    """Verifica la proyección de un activo Gold con metadatos dimensionales."""
    doc = search_service.project_document(sample_gold_asset)
    assert doc.search_id.startswith("finrural:mart_cartera:2025-05-31:")
    assert doc.origin == "INTERNAL_GOLD"
    assert doc.document_type == "PARQUET"
    assert doc.gold_table == "mart_cartera"
    assert "CRECER" in doc.entities_mentioned
    assert "analytics" in doc.tags


def test_search_service_project_bronze_pdf(
    search_service: SearchIndexService,
    sample_bronze_asset: InternalAssetRecord,
) -> None:
    """Verifica la proyección de un archivo crudo PDF en Bronze."""
    doc = search_service.project_document(sample_bronze_asset)
    assert doc.origin == "INTERNAL_BRONZE"
    assert doc.document_type == "PDF"
    assert doc.gold_table is None
    assert doc.local_path == sample_bronze_asset.file_path


def test_search_service_export_index_with_incremental_filter(
    search_service: SearchIndexService,
    in_memory_sqlite_repo: SqliteLineageAdapter,
    sample_gold_asset: InternalAssetRecord,
    sample_bronze_asset: InternalAssetRecord,
) -> None:
    """Verifica la exportación del índice con filtro temporal since_timestamp."""
    in_memory_sqlite_repo.record_asset(sample_gold_asset)
    in_memory_sqlite_repo.record_asset(sample_bronze_asset)

    # Exportar sin filtro
    all_export = search_service.export_index()
    assert all_export.total_documents == 2
    assert len(all_export.documents) == 2

    # Filtrar solo activos modificados en las últimas 24 horas
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_export = search_service.export_index(since_timestamp=cutoff)
    assert recent_export.total_documents == 1
    assert recent_export.documents[0].search_id.startswith("finrural:mart_cartera")


def test_api_export_index_endpoints(
    search_service: SearchIndexService,
    in_memory_sqlite_repo: SqliteLineageAdapter,
    sample_gold_asset: InternalAssetRecord,
) -> None:
    """Verifica los endpoints REST de exportación.

    Cubre tanto /api/v1/search/export-index como /search/export-index.
    """
    in_memory_sqlite_repo.record_asset(sample_gold_asset)
    set_search_index_service(search_service)

    client = TestClient(app)

    # 1. Endpoint prefijado /api/v1/search/export-index
    resp = client.get("/api/v1/search/export-index")
    assert resp.status_code == 200
    data = resp.json()
    assert data["origin"] == "INTERNAL_PROSPECTOR"
    assert data["total_documents"] >= 1
    first_doc = data["documents"][0]
    assert first_doc["search_id"].startswith("finrural:mart_cartera:2025-05-31:")
    assert first_doc["sha256"] == "a" * 64

    # 2. Endpoint directo /search/export-index (especificado en arquitectura 10.1)
    resp_direct = client.get("/search/export-index")
    assert resp_direct.status_code == 200
    assert resp_direct.json()["total_documents"] == data["total_documents"]


def test_api_export_index_with_query_parameters(
    search_service: SearchIndexService,
    in_memory_sqlite_repo: SqliteLineageAdapter,
    sample_gold_asset: InternalAssetRecord,
    sample_bronze_asset: InternalAssetRecord,
) -> None:
    """Verifica filtros de consulta en la API REST."""
    in_memory_sqlite_repo.record_asset(sample_gold_asset)
    in_memory_sqlite_repo.record_asset(sample_bronze_asset)
    set_search_index_service(search_service)

    client = TestClient(app)

    # Filtro por storage_layer=GOLD
    resp_gold = client.get("/api/v1/search/export-index?storage_layer=GOLD")
    assert resp_gold.status_code == 200
    data_gold = resp_gold.json()
    assert all(doc["origin"] == "INTERNAL_GOLD" for doc in data_gold["documents"])

    # Filtro limit=1
    resp_limit = client.get("/api/v1/search/export-index?limit=1")
    assert resp_limit.status_code == 200
    assert len(resp_limit.json()["documents"]) == 1
