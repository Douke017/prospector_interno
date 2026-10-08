"""Guardia de Programación en Ventana Horaria Valle (HUI-10: Off-Peak Execution).

Regula la ejecución de lotes masivos de descarga (> 100 archivos) para operar
exclusivamente durante la ventana valle (23:00 a 06:00 hora de Bolivia, UTC-4),
evitando competir con el tráfico público de portales financieros durante horas
laborales y previniendo bloqueos preventivos de red.
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from prospector_interno.domain.contracts.downloader_spec import (
    BatchExecutionVerdict,
    OffPeakEvaluationRequest,
    OffPeakEvaluationResult,
)
from prospector_interno.domain.ports import OffPeakWindowGuardPort


class OffPeakWindowGuard(OffPeakWindowGuardPort):
    """Guardia horaria basada en el huso horario oficial de Bolivia (UTC-4)."""

    DEFAULT_TIMEZONE_NAME: str = "America/La_Paz"
    DEFAULT_START_HOUR: int = 23  # 23:00
    DEFAULT_END_HOUR: int = 6     # 06:00
    DEFAULT_BATCH_THRESHOLD: int = 100

    def __init__(
        self,
        timezone_name: str = DEFAULT_TIMEZONE_NAME,
        start_hour: int = DEFAULT_START_HOUR,
        end_hour: int = DEFAULT_END_HOUR,
        batch_threshold: int = DEFAULT_BATCH_THRESHOLD,
    ) -> None:
        self._start_hour = start_hour
        self._end_hour = end_hour
        self._batch_threshold = batch_threshold

        try:
            self._tz = ZoneInfo(timezone_name)
        except Exception:
            # Fallback seguro a UTC-4 si la base de datos no está instalada
            self._tz = timezone(timedelta(hours=-4))

    def _normalize_datetime(self, at_time: datetime | None = None) -> datetime:
        """Normaliza un timestamp asegurando su localización en zona valle."""
        if at_time is None:
            return datetime.now(self._tz)
        if at_time.tzinfo is None:
            return at_time.replace(tzinfo=self._tz)
        return at_time.astimezone(self._tz)

    def is_in_offpeak_window(self, at_time: datetime | None = None) -> bool:
        """Determina si la hora dada está en ventana valle (23:00 - 06:00 UTC-4)."""
        dt_local = self._normalize_datetime(at_time)
        hour = dt_local.hour
        return hour >= self._start_hour or hour < self._end_hour

    def calculate_seconds_to_next_window(
        self,
        at_time: datetime | None = None,
    ) -> float:
        """Calcula los segundos restantes hasta la próxima ventana valle."""
        dt_local = self._normalize_datetime(at_time)
        if self.is_in_offpeak_window(dt_local):
            return 0.0

        # Si estamos fuera de la ventana, abre a las 23:00 de hoy
        next_open = dt_local.replace(
            hour=self._start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        diff = (next_open - dt_local).total_seconds()

        return max(0.0, diff)

    def get_next_window_datetime(
        self,
        at_time: datetime | None = None,
    ) -> datetime:
        """Retorna el timestamp exacto en que abrirá la próxima ventana valle."""
        dt_local = self._normalize_datetime(at_time)
        if self.is_in_offpeak_window(dt_local):
            return dt_local

        return dt_local.replace(
            hour=self._start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )

    def evaluate_batch(
        self,
        request: OffPeakEvaluationRequest,
    ) -> OffPeakEvaluationResult:
        """Evalúa si un lote de descargas masivas puede proceder o debe pausarse."""
        dt_local = self._normalize_datetime(request.current_time)
        current_hour = dt_local.hour
        is_offpeak = self.is_in_offpeak_window(dt_local)

        # Regla 1: Flag manual de forzado (--force-now)
        if request.force_now:
            return OffPeakEvaluationResult(
                verdict=BatchExecutionVerdict.FORCED_OVERRIDE,
                is_permitted=True,
                reason=(
                    f"Ejecución autorizada de inmediato mediante anulación manual "
                    f"force_now para {request.batch_size} archivos."
                ),
                current_hour_bolivia=current_hour,
                next_window_opens_at=None,
                seconds_until_next_window=0.0,
                threshold_batch_size=self._batch_threshold,
            )

        # Regla 2: Lote pequeño dentro del umbral permitido durante horas diurnas
        if request.batch_size <= self._batch_threshold:
            return OffPeakEvaluationResult(
                verdict=BatchExecutionVerdict.PERMITTED_IMMEDIATE,
                is_permitted=True,
                reason=(
                    f"Lote de {request.batch_size} archivos <= umbral "
                    f"({self._batch_threshold}). Admitido de inmediato."
                ),
                current_hour_bolivia=current_hour,
                next_window_opens_at=None,
                seconds_until_next_window=0.0,
                threshold_batch_size=self._batch_threshold,
            )

        # Regla 3: Lote masivo (> 100) evaluado contra la ventana horaria valle
        if is_offpeak:
            return OffPeakEvaluationResult(
                verdict=BatchExecutionVerdict.PERMITTED_IMMEDIATE,
                is_permitted=True,
                reason=(
                    f"Lote masivo ({request.batch_size} archivos) admitido dentro "
                    f"de la ventana valle autorizada (23:00 - 06:00 hora de Bolivia)."
                ),
                current_hour_bolivia=current_hour,
                next_window_opens_at=None,
                seconds_until_next_window=0.0,
                threshold_batch_size=self._batch_threshold,
            )

        # Regla 4: Lote masivo fuera de ventana valle -> PAUSED_WAITING_OFFPEAK
        seconds_left = self.calculate_seconds_to_next_window(dt_local)
        next_open_dt = self.get_next_window_datetime(dt_local)

        reason = (
            f"Lote masivo ({request.batch_size} archivos > "
            f"{self._batch_threshold}) encolado en horario laboral "
            f"({current_hour:02d}:{dt_local.minute:02d}). "
            f"Estado PAUSED_WAITING_OFFPEAK hasta las {self._start_hour:02d}:00."
        )

        return OffPeakEvaluationResult(
            verdict=BatchExecutionVerdict.PAUSED_WAITING_OFFPEAK,
            is_permitted=False,
            reason=reason,
            current_hour_bolivia=current_hour,
            next_window_opens_at=next_open_dt,
            seconds_until_next_window=seconds_left,
            threshold_batch_size=self._batch_threshold,
        )
