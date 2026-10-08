from datetime import datetime, timezone

from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
    DownloadStateEvaluationRequest,
)
from prospector_interno.domain.gatekeeper.download_state_filter import (
    DownloadStateFilter,
)
from prospector_interno.domain.models import (
    BronzeArtifact,
    IngestionRequest,
    PreFlightVerdict,
)
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    DownloadTrackingPort,
    GatekeeperStatePort,
)


class MockDownloadTrackingAdapter(DownloadTrackingPort):
    """Implementación simulada en memoria de DownloadTrackingPort."""

    def __init__(self) -> None:
        self.records: dict[str, DownloadRecord] = {}

    def record_download(self, record: DownloadRecord) -> None:
        self.records[record.download_id] = record

    def update_status(
        self,
        download_id: str,
        status: DownloadLifecycleStatus,
        bytes_downloaded: int | None = None,
        total_bytes: int | None = None,
        local_path: str | None = None,
        sha256_checksum: str | None = None,
        error_code: str | None = None,
    ) -> None:
        if download_id in self.records:
            r = self.records[download_id]
            now_dt = datetime.now(timezone.utc)
            is_done = status in (
                DownloadLifecycleStatus.COMPLETED,
                DownloadLifecycleStatus.FAILED,
            )
            completed_time = now_dt if is_done else r.completed_at
            self.records[download_id] = DownloadRecord(
                download_id=r.download_id,
                source_id=r.source_id,
                canonical_url=r.canonical_url,
                local_path=local_path or r.local_path,
                status=status,
                sha256_checksum=sha256_checksum or r.sha256_checksum,
                bytes_downloaded=bytes_downloaded
                if bytes_downloaded is not None
                else r.bytes_downloaded,
                total_bytes=total_bytes if total_bytes is not None else r.total_bytes,
                consecutive_failures=r.consecutive_failures,
                last_error_code=error_code or r.last_error_code,
                task_id=r.task_id,
                subscribers=list(r.subscribers),
                started_at=r.started_at,
                completed_at=completed_time,
            )

    def get_download(self, download_id: str) -> DownloadRecord | None:
        return self.records.get(download_id)

    def find_by_url(self, canonical_url: str) -> DownloadRecord | None:
        for r in self.records.values():
            if r.canonical_url == canonical_url:
                return r
        return None

    def subscribe_request(self, download_id: str, request_id: str) -> bool:
        if download_id in self.records:
            r = self.records[download_id]
            if request_id not in r.subscribers:
                new_subs = list(r.subscribers) + [request_id]
                self.records[download_id] = DownloadRecord(
                    download_id=r.download_id,
                    source_id=r.source_id,
                    canonical_url=r.canonical_url,
                    local_path=r.local_path,
                    status=r.status,
                    sha256_checksum=r.sha256_checksum,
                    bytes_downloaded=r.bytes_downloaded,
                    total_bytes=r.total_bytes,
                    consecutive_failures=r.consecutive_failures,
                    last_error_code=r.last_error_code,
                    task_id=r.task_id,
                    subscribers=new_subs,
                    started_at=r.started_at,
                    completed_at=r.completed_at,
                )
            return True
        return False

    def list_active_downloads(self) -> list[DownloadRecord]:
        return [
            r
            for r in self.records.values()
            if r.status
            in (DownloadLifecycleStatus.DOWNLOADING, DownloadLifecycleStatus.PENDING)
        ]


class MockBronzeStorage(BronzeStoragePort):
    """Mock simplificado de BronzeStoragePort."""

    def __init__(self) -> None:
        self.artifacts: list[BronzeArtifact] = []

    def store_stream(self, **kwargs) -> BronzeArtifact:
        raise NotImplementedError

    def find_by_url(self, target_url: str) -> BronzeArtifact | None:
        return None

    def find_by_period(
        self, domain: str, dataset_code: str, period: str
    ) -> BronzeArtifact | None:
        return None

    def find_by_checksum(self, sha256_hash: str) -> BronzeArtifact | None:
        return None

    def find_by_resource_key(self, resource_key: str) -> BronzeArtifact | None:
        return None


class MockGatekeeperState(GatekeeperStatePort):
    """Mock de GatekeeperStatePort para locks."""

    def __init__(self) -> None:
        self.locks: dict[str, str] = {}

    def try_acquire_lock(self, idempotency_key: str, task_id: str) -> bool:
        if idempotency_key in self.locks:
            return False
        self.locks[idempotency_key] = task_id
        return True

    def release_lock(self, idempotency_key: str, task_id: str) -> None:
        self.locks.pop(idempotency_key, None)

    def get_in_flight_task(self, idempotency_key: str) -> str | None:
        return self.locks.get(idempotency_key)


def test_criterio_1_archivo_descargando_retorna_in_progress_y_task_id() -> None:
    """Criterio 1: Archivo en estado DOWNLOADING retorna IN_PROGRESS y su task_id."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_123",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/reports/2024.xlsx",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="task_worker_01",
        )
    )

    filter_engine = DownloadStateFilter(tracking_port=tracking)
    req = DownloadStateEvaluationRequest(
        canonical_url="https://bcb.gob.bo/reports/2024.xlsx",
        request_id="req_analista_007",
    )
    result = filter_engine.evaluate(req)

    assert result.is_in_progress is True
    assert result.verdict_status == "IN_PROGRESS"
    assert result.task_id == "task_worker_01"
    assert result.status == DownloadLifecycleStatus.DOWNLOADING
    assert result.subscribers_count == 1
    assert "task_worker_01" in result.explanation


def test_suscripcion_concurrente_multiples_peticiones() -> None:
    """DoD: Suscripción de peticiones concurrentes a la misma tarea activa."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_asfi_01",
            source_id="BO_ASFI",
            canonical_url="https://asfi.gob.bo/bol/2024.zip",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="worker_asfi_task",
        )
    )
    filter_engine = DownloadStateFilter(tracking_port=tracking)

    # 1. Petición analista 1
    res1 = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://asfi.gob.bo/bol/2024.zip",
            request_id="analista_1",
        )
    )
    assert res1.is_in_progress is True
    assert res1.subscribers_count == 1

    # 2. Petición analista 2 a la misma tarea
    res2 = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://asfi.gob.bo/bol/2024.zip",
            request_id="analista_2",
        )
    )
    assert res2.is_in_progress is True
    assert res2.subscribers_count == 2

    # Verificar en el adaptador de persistencia
    record = tracking.get_download("dl_asfi_01")
    assert record is not None
    assert record.subscribers == ["analista_1", "analista_2"]


def test_archivo_en_cola_pending_retorna_in_progress() -> None:
    """Verifica que una descarga en cola PENDING también reporte IN_PROGRESS."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_queue_01",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/queued.pdf",
            status=DownloadLifecycleStatus.PENDING,
            task_id="task_queue_99",
        )
    )
    filter_engine = DownloadStateFilter(tracking_port=tracking)

    result = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://bcb.gob.bo/queued.pdf",
            request_id="req_queue",
        )
    )
    assert result.is_in_progress is True
    assert result.verdict_status == "IN_PROGRESS"
    assert result.task_id == "task_queue_99"


def test_archivo_completado_retorna_completed() -> None:
    """Verifica que un archivo previamente completado no marque in_progress."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_comp_01",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/done.xlsx",
            status=DownloadLifecycleStatus.COMPLETED,
            local_path="data/bronze/BO_BCB/2024/done.xlsx",
            sha256_checksum="a" * 64,
        )
    )
    filter_engine = DownloadStateFilter(tracking_port=tracking)

    result = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://bcb.gob.bo/done.xlsx",
            request_id="req_done",
        )
    )
    assert result.is_in_progress is False
    assert result.verdict_status == "COMPLETED"


def test_archivo_fallido_retorna_failed() -> None:
    """Verifica que una descarga fallida anterior requiera reintento."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_fail_01",
            source_id="BO_INE",
            canonical_url="https://ine.gob.bo/error.csv",
            status=DownloadLifecycleStatus.FAILED,
            last_error_code="HTTP_503_SERVICE_UNAVAILABLE",
        )
    )
    filter_engine = DownloadStateFilter(tracking_port=tracking)

    result = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://ine.gob.bo/error.csv",
            request_id="req_retry",
        )
    )
    assert result.is_in_progress is False
    assert result.verdict_status == "FAILED"


def test_archivo_no_registrado_retorna_not_found() -> None:
    """Verifica respuesta cuando el archivo no está en cola ni descargando."""
    tracking = MockDownloadTrackingAdapter()
    filter_engine = DownloadStateFilter(tracking_port=tracking)

    result = filter_engine.evaluate(
        DownloadStateEvaluationRequest(
            canonical_url="https://desconocido.gob.bo/nuevo.xlsx",
            request_id="req_nuevo",
        )
    )
    assert result.is_in_progress is False
    assert result.verdict_status == "NOT_FOUND"


def test_gatekeeper_integra_filtro_estado_descarga_in_progress() -> None:
    """Verifica que PreFlightGatekeeper devuelva PreFlightVerdict.IN_PROGRESS."""
    tracking = MockDownloadTrackingAdapter()
    tracking.record_download(
        DownloadRecord(
            download_id="dl_gate_01",
            source_id="BO_BCB",
            canonical_url="https://bcb.gob.bo/inflacion_2024.xlsx",
            status=DownloadLifecycleStatus.DOWNLOADING,
            task_id="worker_active_07",
        )
    )
    filter_engine = DownloadStateFilter(tracking_port=tracking)
    gatekeeper = PreFlightGatekeeper(
        storage=MockBronzeStorage(),
        state_registry=MockGatekeeperState(),
        download_state_filter=filter_engine,
    )

    req = IngestionRequest(
        request_id="analista_req_01",
        dataset_code="DS_INFLACION",
        domain="bcb.gob.bo",
        target_url="https://bcb.gob.bo/inflacion_2024.xlsx",
        period="2024-05",
    )
    evaluation = gatekeeper.evaluate(req)

    assert evaluation.verdict == PreFlightVerdict.IN_PROGRESS
    assert evaluation.in_flight_task_id == "worker_active_07"
    assert evaluation.lock_acquired is False
    assert evaluation.subscribers_count == 1
    assert "worker_active_07" in evaluation.reason
