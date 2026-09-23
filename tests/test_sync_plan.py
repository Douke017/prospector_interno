import io
from typing import BinaryIO
import pytest

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.bridge.sync_orchestrator import (
    SyncOrchestrator,
)
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.models import (
    ReconciliationDiffReport,
    ReconciliationDiffReportItem,
    ResourceChangeCategory,
    SyncPriority,
)
from prospector_interno.domain.ports import HttpDownloaderPort


class MockHttpDownloader(HttpDownloaderPort):
    """Cliente de descarga simulado en memoria para pruebas de integración."""

    def __init__(self, payload: bytes = b"CONTENIDO_BINARIO_SIMULADO") -> None:
        self._payload = payload

    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        return io.BytesIO(self._payload), 200, "application/pdf"


@pytest.fixture
def mock_downloader() -> MockHttpDownloader:
    return MockHttpDownloader()


@pytest.fixture
def sync_orchestrator(
    gatekeeper: PreFlightGatekeeper,
    bronze_storage: LocalBronzeStorage,
    mock_downloader: MockHttpDownloader,
) -> SyncOrchestrator:
    return SyncOrchestrator(
        gatekeeper=gatekeeper,
        storage=bronze_storage,
        downloader=mock_downloader,
    )


def test_create_sync_plan_prioritizes_recent_periods(
    sync_orchestrator: SyncOrchestrator,
) -> None:
    """Verifica que el SyncPlan asigna prioridad CRITICAL a periodos recientes."""
    diff_report = ReconciliationDiffReport(
        source_id="finrural",
        run_id="run_test_001",
        total_external_resources=3,
        total_confirmed=0,
        total_new=3,
        total_modified=0,
        items=[
            ReconciliationDiffReportItem(
                resource_key="key_2020",
                url="https://finrural.org.bo/doc_2020.pdf",
                source_id="finrural",
                category=ResourceChangeCategory.NEW,
                file_extension=".pdf",
                period_label="2020-01",
                detail="Antiguo",
            ),
            ReconciliationDiffReportItem(
                resource_key="key_2026",
                url="https://finrural.org.bo/doc_2026.pdf",
                source_id="finrural",
                category=ResourceChangeCategory.NEW,
                file_extension=".pdf",
                period_label="2026-06",
                detail="Reciente",
            ),
        ],
    )

    plan = sync_orchestrator.create_plan(diff_report)

    assert plan.total_tasks == 2
    assert plan.is_executed is False

    # El primer item debe ser el de 2026 por tener mayor prioridad
    assert plan.items[0].resource_key == "key_2026"
    assert plan.items[0].priority == SyncPriority.CRITICAL
    assert plan.items[1].resource_key == "key_2020"
    assert plan.items[1].priority == SyncPriority.NORMAL


def test_execute_sync_plan_downloads_to_bronze(
    sync_orchestrator: SyncOrchestrator,
    bronze_storage: LocalBronzeStorage,
) -> None:
    """Verifica la ejecución del plan depositando artefactos reales en Bronze."""
    diff_report = ReconciliationDiffReport(
        source_id="finrural",
        run_id="run_test_002",
        total_external_resources=1,
        total_confirmed=0,
        total_new=1,
        total_modified=0,
        items=[
            ReconciliationDiffReportItem(
                resource_key="key_exec_01",
                url="https://finrural.org.bo/archivo.pdf",
                source_id="finrural",
                category=ResourceChangeCategory.NEW,
                file_extension=".pdf",
                period_label="2026-05",
                detail="Nuevo",
            ),
        ],
    )

    plan = sync_orchestrator.create_plan(diff_report)
    executed_plan = sync_orchestrator.execute_plan(plan)

    assert executed_plan.is_executed is True
    assert executed_plan.completed_tasks == 1
    assert executed_plan.failed_tasks == 0
    assert executed_plan.items[0].status == "COMPLETED"

    artifact = executed_plan.items[0].downloaded_artifact
    assert artifact is not None
    assert artifact.file_path.exists()
    assert artifact.file_path.name.endswith(".dat")
