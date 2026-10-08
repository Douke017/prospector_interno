"""Pruebas unitarias para el Programador de Ventana Horaria Valle (HUI-10).

Verifica que los lotes masivos (> 100 archivos) queden en estado
PAUSED_WAITING_OFFPEAK fuera de la ventana valle (23:00 a 06:00 hora de Bolivia),
permitiendo la ejecución de lotes pequeños o descargas urgentes con --force-now.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock

from prospector_interno.adapters.downloader.window_guard import (
    OffPeakWindowGuard,
)
from prospector_interno.application.bridge.sync_orchestrator import (
    SyncOrchestrator,
)
from prospector_interno.domain.contracts.downloader_spec import (
    BatchExecutionVerdict,
    OffPeakEvaluationRequest,
)
from prospector_interno.domain.models import (
    PreFlightVerdict,
    SyncPlan,
    SyncPlanItem,
    SyncPriority,
)


def test_batch_at_14_hours_paused_waiting_offpeak() -> None:
    """Criterio 1: Lote masivo a las 14:00 queda en PAUSED_WAITING_OFFPEAK."""
    guard = OffPeakWindowGuard()

    tz = ZoneInfo("America/La_Paz")

    # Simular 14:00 hora de Bolivia (horario laboral pico)
    simulated_now = datetime(2026, 10, 8, 14, 0, 0, tzinfo=tz)

    request = OffPeakEvaluationRequest(
        batch_size=1500,
        force_now=False,
        current_time=simulated_now,
        source_id="finrural",
    )

    result = guard.evaluate_batch(request)

    assert result.verdict == BatchExecutionVerdict.PAUSED_WAITING_OFFPEAK
    assert result.is_permitted is False
    assert result.current_hour_bolivia == 14
    assert result.seconds_until_next_window == 9 * 3600  # 9 horas exactas
    assert result.next_window_opens_at == datetime(2026, 10, 8, 23, 0, 0, tzinfo=tz)
    assert "PAUSED_WAITING_OFFPEAK" in result.reason


def test_batch_during_offpeak_hours_is_permitted() -> None:
    """Lote de 1.500 archivos durante la ventana valle (23:00 a 06:00) es admitido."""
    guard = OffPeakWindowGuard()
    tz = ZoneInfo("America/La_Paz")

    # Caso 1: 23:30 (inicio de ventana)
    time_2330 = datetime(2026, 10, 8, 23, 30, 0, tzinfo=tz)
    res_2330 = guard.evaluate_batch(
        OffPeakEvaluationRequest(batch_size=1500, current_time=time_2330)
    )
    assert res_2330.verdict == BatchExecutionVerdict.PERMITTED_IMMEDIATE
    assert res_2330.is_permitted is True
    assert res_2330.seconds_until_next_window == 0.0

    # Caso 2: 03:00 de la madrugada (plena ventana)
    time_0300 = datetime(2026, 10, 9, 3, 0, 0, tzinfo=tz)
    res_0300 = guard.evaluate_batch(
        OffPeakEvaluationRequest(batch_size=1500, current_time=time_0300)
    )
    assert res_0300.verdict == BatchExecutionVerdict.PERMITTED_IMMEDIATE
    assert res_0300.is_permitted is True

    # Caso 3: 05:59 (último minuto antes de cierre)
    time_0559 = datetime(2026, 10, 9, 5, 59, 0, tzinfo=tz)
    res_0559 = guard.evaluate_batch(
        OffPeakEvaluationRequest(batch_size=1500, current_time=time_0559)
    )
    assert res_0559.verdict == BatchExecutionVerdict.PERMITTED_IMMEDIATE
    assert res_0559.is_permitted is True


def test_small_batch_under_threshold_is_permitted_anytime() -> None:
    """Lotes pequeños (<= 100) no saturan portales y se admiten en horario diurno."""
    guard = OffPeakWindowGuard()
    tz = ZoneInfo("America/La_Paz")
    daytime = datetime(2026, 10, 8, 11, 30, 0, tzinfo=tz)

    # Lote de 10 archivos (admitido)
    res_10 = guard.evaluate_batch(
        OffPeakEvaluationRequest(batch_size=10, current_time=daytime)
    )
    assert res_10.verdict == BatchExecutionVerdict.PERMITTED_IMMEDIATE
    assert res_10.is_permitted is True

    # Lote límite de 100 archivos (admitido)
    res_100 = guard.evaluate_batch(
        OffPeakEvaluationRequest(batch_size=100, current_time=daytime)
    )
    assert res_100.verdict == BatchExecutionVerdict.PERMITTED_IMMEDIATE
    assert res_100.is_permitted is True


def test_force_now_override_permits_massive_batch_during_daytime() -> None:
    """Flag --force-now anula la restricción para descargas urgentes del analista."""
    guard = OffPeakWindowGuard()
    tz = ZoneInfo("America/La_Paz")
    daytime = datetime(2026, 10, 8, 15, 0, 0, tzinfo=tz)

    res_forced = guard.evaluate_batch(
        OffPeakEvaluationRequest(
            batch_size=2000,
            force_now=True,
            current_time=daytime,
        )
    )
    assert res_forced.verdict == BatchExecutionVerdict.FORCED_OVERRIDE
    assert res_forced.is_permitted is True
    assert "force_now" in res_forced.reason


def test_sync_orchestrator_pauses_massive_plan_when_outside_window() -> None:
    """Verifica que SyncOrchestrator pause un plan masivo si la guardia lo rechaza."""
    tz = ZoneInfo("America/La_Paz")
    daytime = datetime(2026, 10, 8, 16, 0, 0, tzinfo=tz)

    mock_gatekeeper = MagicMock()
    mock_storage = MagicMock()
    mock_downloader = MagicMock()

    # Configurar guardia simulando horario diurno
    guard = OffPeakWindowGuard()
    # Interceptar _normalize_datetime para simular la hora
    guard._normalize_datetime = lambda at_time=None: daytime  # type: ignore

    orchestrator = SyncOrchestrator(
        gatekeeper=mock_gatekeeper,
        storage=mock_storage,
        downloader=mock_downloader,
        window_guard=guard,
    )

    # Crear plan con 150 items
    items = [
        SyncPlanItem(
            item_id=f"item_{i:03d}",
            resource_key=f"res_{i}",
            url=f"https://finrural.org.bo/doc_{i}.pdf",
            source_id="finrural",
            domain="finrural.org.bo",
            period="2025-05",
            priority=SyncPriority.NORMAL,
            status="PENDING",
        )
        for i in range(150)
    ]
    plan = SyncPlan(
        plan_id="plan_masivo_test",
        source_id="finrural",
        created_at=datetime.now(),
        total_tasks=150,
        items=items,
        completed_tasks=0,
        failed_tasks=0,
        is_executed=False,
    )

    # Ejecución sin force_now -> Debe pausarse sin tocar el storage ni downloader
    result_plan = orchestrator.execute_plan(plan, force_now=False)

    assert result_plan.is_executed is False
    assert result_plan.completed_tasks == 0
    assert result_plan.failed_tasks == 0
    assert all(item.status == "PAUSED_WAITING_OFFPEAK" for item in result_plan.items)
    mock_downloader.download_to_stream.assert_not_called()
    mock_storage.store_stream.assert_not_called()

    # Ahora ejecución con force_now -> Debe proceder
    mock_evaluation = MagicMock()
    mock_evaluation.verdict = PreFlightVerdict.PROCEED_DOWNLOAD
    mock_evaluation.lock_acquired = False
    mock_gatekeeper.evaluate.return_value = mock_evaluation
    mock_downloader.download_to_stream.return_value = (
        MagicMock(),
        200,
        "application/pdf",
    )
    mock_storage.store_stream.return_value = MagicMock()

    forced_result = orchestrator.execute_plan(plan, force_now=True)

    assert forced_result.is_executed is True
    assert forced_result.completed_tasks == 150
    assert mock_downloader.download_to_stream.call_count == 150
