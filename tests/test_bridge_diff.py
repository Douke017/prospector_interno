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
