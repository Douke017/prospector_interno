"""Pruebas de integración para Pre-Flight Gatekeeper sobre datos reales.

Valida los 4 filtros del Gatekeeper (Derivación, Cache Hit Bronze, Tracking
de Ciclo de Vida y Locks en Vuelo) utilizando el catálogo canónico y el
historial de descargas extraído de la base de datos de producción replicada.
"""

from pathlib import Path
import time
import pytest

pytestmark = pytest.mark.legacy_pg

from prospector_interno.adapters.legacy.backup_seeder import PostgresLegacySeeder
from prospector_interno.adapters.memory_state import InMemoryGatekeeperState
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.models import (
    IngestionRequest,
    PreFlightVerdict,
)


SNAPSHOT_PATH = (
    Path(__file__).parent.parent / "data" / "snapshot" / "platform_prod_snapshot.db"
)
TOP_5_SOURCES = ["BCB", "ASFI", "INE", "BBV", "APS"]
TOP_5_FILES = [
    "D_BO_000000484",  # BCB
    "D_BO_000000058",  # ASFI
    "D_BO_000000016",  # INE
    "D_BO_000000481",  # BBV
    "D_BO_000000447",  # APS
]


@pytest.fixture
def test_environment(tmp_path: Path):
    """Inicializa el ecosistema completo del Gatekeeper conectado al snapshot."""
    seeder = PostgresLegacySeeder(sqlite_snapshot_path=SNAPSHOT_PATH)

    # 1. Catálogo Canónico SQLite
    catalog_adapter = SqliteLineageAdapter(db_path=tmp_path / "catalog.db")
    seeder.seed_catalog(
        catalog=catalog_adapter,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    # 2. Historial de Descargas SQLite
    tracking_adapter = SqliteDownloadTrackingAdapter(
        db_path=tmp_path / "tracking.db"
    )
    seeder.seed_download_history(
        tracker=tracking_adapter,
        source_codes=TOP_5_SOURCES,
        file_codes=TOP_5_FILES,
    )

    # 3. Componentes del Gatekeeper
    storage = LocalBronzeStorage(base_path=tmp_path / "bronze")
    state_registry = InMemoryGatekeeperState()
    download_filter = DownloadStateFilter(tracking_port=tracking_adapter)

    gatekeeper = PreFlightGatekeeper(
        storage=storage,
        state_registry=state_registry,
        download_state_filter=download_filter,
    )

    return {
        "gatekeeper": gatekeeper,
        "seeder": seeder,
        "catalog": catalog_adapter,
        "tracking": tracking_adapter,
        "storage": storage,
    }


def test_gatekeeper_detects_historical_bcb_download_in_under_20ms(
    test_environment: dict,
) -> None:
    """Verifica que un archivo histórico de BCB se resuelve como CACHE_HIT en <20ms."""
    gatekeeper: PreFlightGatekeeper = test_environment["gatekeeper"]
    seeder: PostgresLegacySeeder = test_environment["seeder"]

    # Extraer descargas reales de BCB
    bcb_downloads = seeder.extract_downloads(
        source_codes=["BCB"],
        file_codes=["D_BO_000000484"],
    )
    assert len(bcb_downloads) > 0
    sample_dl = bcb_downloads[0]

    request = IngestionRequest(
        request_id="req-gatekeeper-bcb-001",
        domain="BO_BCB",
        dataset_code="DS_BO_BCB_D_BO_000000484",
        period="2026-10-05",
        target_url=sample_dl.canonical_url,
    )

    start_time = time.perf_counter()
    eval_result = gatekeeper.evaluate(request)
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Cumplimiento estricto del DoD: evaluación <20 ms
    assert elapsed_ms < 20.0, f"Evaluación tardó {elapsed_ms:.2f} ms (debe ser < 20 ms)"
    assert eval_result.verdict == PreFlightVerdict.CACHE_HIT_BRONZE
    assert eval_result.lock_acquired is False
    assert "ya descargado con éxito" in eval_result.reason


def test_gatekeeper_aps_control_gap_permits_download(
    test_environment: dict,
) -> None:
    """Verifica que APS (brecha de control: 0 descargas) se autoriza para descarga."""
    gatekeeper: PreFlightGatekeeper = test_environment["gatekeeper"]
    catalog: SqliteLineageAdapter = test_environment["catalog"]

    aps_ds = catalog.get_dataset_by_legacy_code("D_BO_000000447")
    assert aps_ds is not None

    request = IngestionRequest(
        request_id="req-gatekeeper-aps-001",
        domain="BO_APS",
        dataset_code=aps_ds.dataset_code,
        period="2026-09",
        target_url="https://www.aps.gob.bo/media/boletin_mensual_2026_09.pdf",
    )

    eval_result = gatekeeper.evaluate(request)

    assert eval_result.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert eval_result.lock_acquired is True
    assert eval_result.cached_artifact is None


def test_gatekeeper_subscribes_concurrent_request_to_active_download(
    test_environment: dict,
) -> None:
    """Verifica que una petición concurrente a una tarea en DOWNLOADING se suscribe."""
    gatekeeper: PreFlightGatekeeper = test_environment["gatekeeper"]
    tracking: SqliteDownloadTrackingAdapter = test_environment["tracking"]

    # Simular una descarga activa en curso
    active_url = "https://www.bbv.com.bo/Media/Default/Home/Resumen_EnCurso.pdf"
    tracking.record_download(
        DownloadRecord(
            download_id="dl_active_bbv_test",
            source_id="BO_BBV",
            canonical_url=active_url,
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="task-worker-42",
        )
    )

    concurrent_req = IngestionRequest(
        request_id="req-concurrent-analyst-99",
        domain="BO_BBV",
        dataset_code="DS_BO_BBV_D_BO_000000481",
        period="2026-10-09",
        target_url=active_url,
    )

    eval_result = gatekeeper.evaluate(concurrent_req)

    assert eval_result.verdict == PreFlightVerdict.IN_PROGRESS
    assert eval_result.lock_acquired is False
    assert eval_result.in_flight_task_id == "task-worker-42"
    assert eval_result.subscribers_count >= 1

    # Verificar que el suscriptor quedó persistido en SQLite
    updated = tracking.get_download("dl_active_bbv_test")
    assert updated is not None
    assert "req-concurrent-analyst-99" in updated.subscribers


def test_gatekeeper_force_refresh_bypasses_historical_cache(
    test_environment: dict,
) -> None:
    """Verifica que force_refresh=True omite la descarga histórica previa."""
    gatekeeper: PreFlightGatekeeper = test_environment["gatekeeper"]
    seeder: PostgresLegacySeeder = test_environment["seeder"]

    ine_downloads = seeder.extract_downloads(
        source_codes=["INE"],
        file_codes=["D_BO_000000016"],
    )
    assert len(ine_downloads) > 0
    sample_dl = ine_downloads[0]

    forced_req = IngestionRequest(
        request_id="req-ine-forced-001",
        domain="BO_INE",
        dataset_code="DS_BO_INE_D_BO_000000016",
        period="2026-09",
        target_url=sample_dl.canonical_url,
        force_refresh=True,
    )

    eval_result = gatekeeper.evaluate(forced_req)

    assert eval_result.verdict == PreFlightVerdict.PROCEED_DOWNLOAD
    assert eval_result.lock_acquired is True
