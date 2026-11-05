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
) -> None:
    """Verifica HUI-18 procesando el mapa real del Prospector Externo para BBV."""
    bbv_map = Path("../prospector_externo/output/bbv/mapa_bbv.json")
    if not bbv_map.exists():
        bbv_map = Path("prospector_externo/output/bbv/mapa_bbv.json")

    report = diff_engine.compute_diff(
        source_id="bbv",
        external_map_path=bbv_map,
    )

    assert report.source_id == "bbv"
    assert report.total_external_resources == 235
    assert report.total_new == 235  # En almacenamiento vacío todos son nuevos
    assert report.total_confirmed == 0
    assert len(report.items) == 235


def test_compute_diff_classifies_confirmed_when_in_storage(
    diff_engine: DuckDBDiffEngine,
    bronze_storage: LocalBronzeStorage,
) -> None:
    """Verifica que recursos ya presentes en Capa Bronze se clasifican CONFIRMED."""
    finrural_map = Path("../prospector_externo/output/finrural/mapa_finrural.json")
    if not finrural_map.exists():
        finrural_map = Path("prospector_externo/output/finrural/mapa_finrural.json")

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
        external_map_path=finrural_map,
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
) -> None:
    """Verifica que DuckDB procese el CSV canónico entregado por el supervisor."""
    base_dir = Path("/home/douke017/Datax/archivos_prospector_interno")
    candidate_paths = [
        Path("../archivos_prospector_interno/bcb_mapa_interno_canonico_v1.csv"),
        Path("archivos_prospector_interno/bcb_mapa_interno_canonico_v1.csv"),
        base_dir / "bcb_mapa_interno_canonico_v1.csv",
    ]
    target_csv = next((p for p in candidate_paths if p.exists()), None)
    assert target_csv is not None

    report = diff_engine.compute_diff(
        source_id="bcb",
        external_map_path=target_csv,
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
) -> None:
    """Verifica que DuckDBDiffEngine procese datax_projection.json del BCB (527 recursos)."""
    bcb_projection = Path("mapas_externo_last/bcb/datax_projection.json")
    if not bcb_projection.exists():
        bcb_projection = Path("../mapas_externo_last/bcb/datax_projection.json")

    assert bcb_projection.exists(), "El mapa de proyección del BCB debe existir"

    report = diff_engine.compute_diff(
        source_id="bcb",
        external_map_path=bcb_projection,
    )

    assert report.source_id == "bcb"
    assert report.run_id == "run_20260919_052909_fa1124"
    assert report.total_external_resources == 527
    assert len(report.items) == 527
    assert all("bcb.gob.bo" in item.url for item in report.items)
    assert all(item.resource_key for item in report.items)


def test_compute_diff_auto_discovers_bcb_projection(
    bronze_storage: LocalBronzeStorage,
) -> None:
    """Verifica que compute_diff auto-descubra mapas_externo_last/bcb cuando no se pasa path."""
    from unittest.mock import create_autospec
    from prospector_interno.domain.ports import ExternalCatalogClientPort

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

