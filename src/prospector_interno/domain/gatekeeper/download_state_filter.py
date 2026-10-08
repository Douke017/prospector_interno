"""Filtro de Estado de Ciclo de Ingesta (HUI-03).

Filtro 3 del Pre-Flight Gatekeeper (Sección 7.1 de la arquitectura):
Verifica si el archivo fuente ya se encuentra en cola o en proceso de
transferencia física por el PoliteMassiveDownloader para prevenir descargas
duplicadas y suscribir peticiones concurrentes a la misma tarea activa.
"""

from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadStateEvaluationRequest,
    DownloadStateEvaluationResult,
)
from prospector_interno.domain.ports import DownloadTrackingPort


class DownloadStateFilter:
    """Filtro 3 del Gatekeeper: Consulta el estado en tiempo real del archivo."""

    def __init__(self, tracking_port: DownloadTrackingPort) -> None:
        self._tracking_port = tracking_port

    def evaluate(
        self,
        request: DownloadStateEvaluationRequest,
    ) -> DownloadStateEvaluationResult:
        """Determina si la solicitud ya está en proceso de adquisición.

        Criterios de Decisión:
        - Si está en DOWNLOADING o PENDING:
          Suscribe el request_id del cliente a la tarea activa y devuelve
          status='IN_PROGRESS' con el task_id.
        - Si está COMPLETED:
          Reporta finalización (permite canalizar a Capa Bronze/Silver).
        - Si no está registrado:
          Reporta 'READY_TO_DOWNLOAD' / 'NOT_FOUND'.
        """
        record = self._tracking_port.find_by_url(request.canonical_url)

        if record is None:
            return DownloadStateEvaluationResult(
                is_in_progress=False,
                status=None,
                task_id=None,
                download_id=None,
                subscribers_count=0,
                verdict_status="NOT_FOUND",
                explanation=(
                    "El archivo fuente no se encuentra en cola ni en descarga "
                    "activa. Autorizado para programación de ingesta."
                ),
            )

        if record.status in (
            DownloadLifecycleStatus.DOWNLOADING,
            DownloadLifecycleStatus.PENDING,
        ):
            # Suscribir petición concurrente a la tarea activa (DoD)
            self._tracking_port.subscribe_request(
                download_id=record.download_id,
                request_id=request.request_id,
            )
            # Reconsultar o calcular conteo de suscriptores actualizado
            updated = self._tracking_port.get_download(record.download_id)
            if updated:
                subs_count = len(updated.subscribers)
            else:
                subs_count = len(record.subscribers) + 1

            status_str = (
                "en curso de descarga"
                if record.status == DownloadLifecycleStatus.DOWNLOADING
                else "en cola de espera"
            )
            return DownloadStateEvaluationResult(
                is_in_progress=True,
                status=record.status,
                task_id=record.task_id,
                download_id=record.download_id,
                subscribers_count=subs_count,
                verdict_status="IN_PROGRESS",
                explanation=(
                    f"Descarga física {status_str} bajo la tarea '{record.task_id}'. "
                    f"Petición '{request.request_id}' suscrita a la tarea activa "
                    f"({subs_count} suscriptor(es))."
                ),
            )

        if record.status == DownloadLifecycleStatus.COMPLETED:
            chk = record.sha256_checksum[:12] if record.sha256_checksum else "N/A"
            return DownloadStateEvaluationResult(
                is_in_progress=False,
                status=record.status,
                task_id=record.task_id,
                download_id=record.download_id,
                subscribers_count=len(record.subscribers),
                verdict_status="COMPLETED",
                explanation=(
                    f"Archivo fuente ya descargado con éxito en '{record.local_path}' "
                    f"(checksum: {chk})."
                ),
            )

        if record.status == DownloadLifecycleStatus.FAILED:
            return DownloadStateEvaluationResult(
                is_in_progress=False,
                status=record.status,
                task_id=record.task_id,
                download_id=record.download_id,
                subscribers_count=len(record.subscribers),
                verdict_status="FAILED",
                explanation=(
                    f"Descarga previa fallida (error: {record.last_error_code}). "
                    "Se requiere reintento o saneamiento de conexión."
                ),
            )

        # THROTTLED u otro estado
        return DownloadStateEvaluationResult(
            is_in_progress=True,
            status=record.status,
            task_id=record.task_id,
            download_id=record.download_id,
            subscribers_count=len(record.subscribers),
            verdict_status="THROTTLED",
            explanation=(
                f"Descarga retenida temporalmente por disyuntor o tasa: "
                f"{record.last_error_code}."
            ),
        )
