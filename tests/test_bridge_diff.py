"""Pruebas unitarias para HUI-18: Motor de Conciliación y Diff con DuckDB."""

from pathlib import Path
import pytest

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.bridge.diff_engine import DuckDBDiffEngine
from prospector_interno.domain.exceptions import (
    ExternalMapNotFoundError,
)
from prospector_interno.domain.models import (
    BronzeArtifact,
    ResourceChangeCategory,
)


@pytest.fixture
def diff_engine(bronze_storage: LocalBronzeStorage) -> DuckDBDiffEngine:
    """Instancia del motor de diff configurada con almacenamiento local."""
    return DuckDBDiffEngine(storage=bronze_storage)


def test_compute_diff_missing_file_raises_error(
    diff_engine: DuckDBDiffEngine,
) -> None:
    """Verifica que un archivo inexistente lanza ExternalMapNotFoundError."""
    with pytest.raises(ExternalMapNotFoundError):
        diff_engine.compute_diff(
            source_id="inexistente",
            external_map_path=Path("ruta/falsa/no_existe.json"),
        )


def test_compute_diff_with_real_bbv_external_map(
    diff_engine: DuckDBDiffEngine,
    bbv_map_path: Path,
) -> None:
    """Verifica HUI-18 procesando el mapa del Prospector Externo para BBV."""
    report = diff_engine.compute_diff(
        source_id="bbv",
        external_map_path=bbv_map_path,
    )

    assert report.source_id == "bbv"
    assert report.total_external_resources == 235
    assert report.total_new == 235  # En almacenamiento vacío todos son nuevos
    assert report.total_confirmed == 0
    assert len(report.items) == 235


def test_compute_diff_classifies_confirmed_when_in_storage(
    diff_engine: DuckDBDiffEngine,
    bronze_storage: LocalBronzeStorage,
    finrural_map_path: Path,
) -> None:
    """Verifica que recursos ya presentes en Capa Bronze se clasifican CONFIRMED."""
    # Precargar un recurso específico en Bronze coincidente con el periodo '2026-01'
    artifact = BronzeArtifact(
        artifact_id="bronze_test_mock_finrural",
        dataset_code="DS_BO_FINRURAL",
        domain="finrural",
        file_path=Path("fake/path.dat"),
        sha256_hash="hash_mock_sha256_01",
        size_bytes=1024,
    )
    bronze_storage.register_existing_artifact(artifact, period="2026-01")

    report = diff_engine.compute_diff(
        source_id="finrural",
        external_map_path=finrural_map_path,
    )

    assert report.source_id == "finrural"
    assert report.total_external_resources == 130
    assert report.total_confirmed >= 1

    # Verificar que el item con periodo '2026-01' está marcado como CONFIRMED
    confirmed_item = next(
        (i for i in report.items if i.period_label == "2026-01"),
        None,
    )
    assert confirmed_item is not None
    assert confirmed_item.category == ResourceChangeCategory.CONFIRMED


def test_compute_diff_with_supervisor_canonical_bcb_csv(
    diff_engine: DuckDBDiffEngine,
    bcb_canonical_csv_path: Path,
) -> None:
    """Verifica que DuckDB procese el CSV canónico entregado por el supervisor."""
    report = diff_engine.compute_diff(
        source_id="bcb",
        external_map_path=bcb_canonical_csv_path,
    )

    assert report.source_id == "bcb"
    assert report.total_external_resources == 164
    assert len(report.items) == 164
    assert all("bcb.gob.bo" in item.url for item in report.items)


def test_compute_diff_via_rest_client(
    bronze_storage: LocalBronzeStorage,
) -> None:
    """Verifica que DuckDBDiffEngine concilie vía REST sin ruta de archivo."""
    from unittest.mock import create_autospec
    from prospector_interno.domain.models import DiscoveredResourceItem
    from prospector_interno.domain.ports import ExternalCatalogClientPort

    mock_client = create_autospec(ExternalCatalogClientPort)
    mock_client.fetch_resources.return_value = [
        DiscoveredResourceItem(
            resource_key="res_01",
            url="https://www.bbv.com.bo/doc1.pdf",
            source_id="bbv",
            title="Doc 1",
            file_extension=".pdf",
            period_label="2026-02",
        ),
        DiscoveredResourceItem(
            resource_key="res_02",
            url="https://www.bbv.com.bo/doc2.pdf",
            source_id="bbv",
            title="Doc 2",
            file_extension=".pdf",
            period_label="2026-03",
        ),
    ]
    mock_client.get_latest_run_id.return_value = "run_mock_remote_99"

    engine = DuckDBDiffEngine(
        storage=bronze_storage,
        external_client=mock_client,
    )
    report = engine.compute_diff(source_id="bbv")

    assert report.source_id == "bbv"
    assert report.run_id == "run_mock_remote_99"
    assert report.total_external_resources == 2
    assert report.total_new == 2
    mock_client.fetch_resources.assert_called_once_with(
        source_id="bbv", base_url=None
    )


def test_compute_diff_with_bcb_datax_projection(
    diff_engine: DuckDBDiffEngine,
    bcb_projection_path: Path,
) -> None:
    """Verifica que DuckDBDiffEngine procese datax_projection.json del BCB (527 recursos)."""
    assert bcb_projection_path.exists(), "El mapa de proyección del BCB debe existir"

    report = diff_engine.compute_diff(
        source_id="bcb",
        external_map_path=bcb_projection_path,
    )

    assert report.source_id == "bcb"
    assert report.run_id == "run_20260919_052909_fa1124"
    assert report.total_external_resources == 527
    assert len(report.items) == 527
    assert all("bcb.gob.bo" in item.url for item in report.items)
    assert all(item.resource_key for item in report.items)


def test_compute_diff_auto_discovers_bcb_projection(
    bronze_storage: LocalBronzeStorage,
    bcb_projection_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifica que compute_diff auto-descubra mapas_externo_last/bcb cuando no se pasa path."""
    from unittest.mock import create_autospec
    from prospector_interno.domain.ports import ExternalCatalogClientPort

    assert bcb_projection_path.exists()
    monkeypatch.chdir(tmp_path)

    # Mock client que no devuelve recursos para forzar fallback local
    mock_client = create_autospec(ExternalCatalogClientPort)
    mock_client.fetch_resources.return_value = []
    mock_client.get_latest_run_id.return_value = None

    engine = DuckDBDiffEngine(
        storage=bronze_storage,
        external_client=mock_client,
    )
    report = engine.compute_diff(source_id="bcb")

    assert report.source_id == "bcb"
    assert report.total_external_resources == 527
    assert len(report.items) == 527

