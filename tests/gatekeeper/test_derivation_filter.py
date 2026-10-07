"""Pruebas unitarias para el motor de reutilización y derivación (HUI-02)."""

from pathlib import Path

import pytest

from prospector_interno.domain.contracts.derivation_spec import (
    DerivationEvaluationRequest,
    DerivationGranularity,
    DerivationStatus,
)
from prospector_interno.domain.gatekeeper.derivation_filter import (
    AssetDerivationFilter,
)
from prospector_interno.domain.gatekeeper.derivation_rules import (
    parse_period_granularity,
)
from prospector_interno.domain.ports import GoldPartitionPort


class MockGoldPartitionChecker(GoldPartitionPort):
    """Implementación simulada en memoria de GoldPartitionPort para tests."""

    def __init__(self, available: set[tuple[int, int]] | None = None) -> None:
        self._available = available or set()

    def list_available_partitions(
        self,
        datamart: str = "mart_cartera",
    ) -> list[tuple[int, int]]:
        return sorted(list(self._available))

    def has_partition(
        self,
        datamart: str,
        gestion: int,
        mes: int,
    ) -> bool:
        return (gestion, mes) in self._available


def test_parse_period_granularity_annual() -> None:
    """Verifica detección de periodos anuales y sus 12 meses requeridos."""
    for raw in ["2024", "2024-ANNUAL", "2024-ANUAL", "GESTION-2024", "GESTION_2024"]:
        bd = parse_period_granularity(raw)
        assert bd.granularity == DerivationGranularity.ANNUAL
        assert bd.gestion == 2024
        assert bd.required_months == list(range(1, 13))
        assert bd.required_period_keys == [f"2024-{m:02d}" for m in range(1, 13)]


def test_parse_period_granularity_semesters() -> None:
    """Verifica descomposición de semestres S1 y S2."""
    s1 = parse_period_granularity("2024-S1")
    assert s1.granularity == DerivationGranularity.SEMESTER
    assert s1.required_months == [1, 2, 3, 4, 5, 6]

    s2 = parse_period_granularity("2024-S2")
    assert s2.granularity == DerivationGranularity.SEMESTER
    assert s2.required_months == [7, 8, 9, 10, 11, 12]


def test_parse_period_granularity_quarters() -> None:
    """Verifica descomposición de los 4 trimestres del año."""
    expected_quarters = {
        "2025-Q1": [1, 2, 3],
        "2025-Q2": [4, 5, 6],
        "2025-Q3": [7, 8, 9],
        "2025-Q4": [10, 11, 12],
    }
    for q_str, months in expected_quarters.items():
        bd = parse_period_granularity(q_str)
        assert bd.granularity == DerivationGranularity.QUARTER
        assert bd.required_months == months


def test_parse_period_granularity_monthly() -> None:
    """Verifica detección de periodos mensuales individuales."""
    bd = parse_period_granularity("2024-08")
    assert bd.granularity == DerivationGranularity.MONTHLY
    assert bd.required_months == [8]


def test_parse_period_granularity_invalid_raises_value_error() -> None:
    """Valida rechazo de periodos inválidos o años fuera de rango."""
    with pytest.raises(ValueError):
        parse_period_granularity("periodo_invalido")

    with pytest.raises(ValueError):
        parse_period_granularity("1800-01")

    with pytest.raises(ValueError):
        parse_period_granularity("2024-15")


def test_derivation_filter_criterio_1_full_coverage_derivable() -> None:
    """Criterio 1: 12 particiones en Gold -> DERIVABLE con query precompilada."""
    all_12_months = {(2024, m) for m in range(1, 13)}
    checker = MockGoldPartitionChecker(available=all_12_months)
    filter_engine = AssetDerivationFilter(partition_checker=checker)

    req = DerivationEvaluationRequest(
        dataset_code="mart_cartera",
        period="2024",
    )
    result = filter_engine.evaluate(req)

    assert result.status == DerivationStatus.DERIVABLE
    assert result.is_derivable is True
    assert result.coverage_ratio == 1.0
    assert result.available_periods == [f"2024-{m:02d}" for m in range(1, 13)]
    assert result.missing_periods == []
    assert result.precompiled_sql is not None

    # Validar estructura del SQL precompilado
    sql = result.precompiled_sql
    assert "gestion = 2024" in sql
    assert "mes IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)" in sql
    assert "SUM(cartera_bruta)" in sql
    assert "ratio_mora_acumulado" in sql
    assert "GROUP BY institucion_id, institucion_nombre" in sql


def test_derivation_filter_criterio_2_partial_coverage_missing_periods() -> None:
    """Criterio 2: 10 de 12 particiones -> MISSING_PERIODS indicando los 2 faltantes."""
    only_10_months = {(2024, m) for m in range(1, 11)}  # Faltan 11 y 12
    checker = MockGoldPartitionChecker(available=only_10_months)
    filter_engine = AssetDerivationFilter(partition_checker=checker)

    req = DerivationEvaluationRequest(
        dataset_code="mart_cartera",
        period="2024",
    )
    result = filter_engine.evaluate(req)

    assert result.status == DerivationStatus.MISSING_PERIODS
    assert result.is_derivable is False
    assert result.coverage_ratio == pytest.approx(10.0 / 12.0, rel=1e-3)
    assert result.missing_periods == ["2024-11", "2024-12"]
    assert "2024-11, 2024-12" in result.explanation
    assert "Requiere ingesta de 2 periodo(s) faltante(s)" in result.explanation


def test_derivation_filter_quarter_derivation() -> None:
    """Verifica derivación exitosa y fallida para un trimestre Q2 (meses 4, 5, 6)."""
    # 1. Éxito: 3 de 3 meses disponibles
    q2_available = {(2025, 4), (2025, 5), (2025, 6)}
    checker_full = MockGoldPartitionChecker(available=q2_available)
    engine_full = AssetDerivationFilter(partition_checker=checker_full)

    res_full = engine_full.evaluate(
        DerivationEvaluationRequest(dataset_code="mart_cartera", period="2025-Q2")
    )
    assert res_full.is_derivable is True
    assert res_full.status == DerivationStatus.DERIVABLE
    assert res_full.coverage_ratio == 1.0

    # 2. Falla: falta mes 5
    q2_partial = {(2025, 4), (2025, 6)}
    checker_partial = MockGoldPartitionChecker(available=q2_partial)
    engine_partial = AssetDerivationFilter(partition_checker=checker_partial)

    res_partial = engine_partial.evaluate(
        DerivationEvaluationRequest(dataset_code="mart_cartera", period="2025-Q2")
    )
    assert res_partial.is_derivable is False
    assert res_partial.status == DerivationStatus.MISSING_PERIODS
    assert res_partial.missing_periods == ["2025-05"]


def test_derivation_filter_with_institution_id() -> None:
    """Verifica que el SQL precompilado incluya el filtro de institución."""
    checker = MockGoldPartitionChecker(available={(2024, 1), (2024, 2), (2024, 3)})
    engine = AssetDerivationFilter(partition_checker=checker)

    req = DerivationEvaluationRequest(
        dataset_code="mart_cartera",
        period="2024-Q1",
        institution_id="CRECER",
    )
    result = engine.evaluate(req)
    assert result.is_derivable is True
    assert result.precompiled_sql is not None
    assert "UPPER(institucion_id) = 'CRECER'" in result.precompiled_sql


def test_duckdb_gold_partition_adapter(tmp_path: Path) -> None:
    """Verifica DuckDBGoldPartitionAdapter leyendo particiones en disco."""
    from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
        DuckDBGoldPartitionAdapter,
    )

    gold_dir = tmp_path / "gold"
    # Crear particiones simuladas
    p1 = gold_dir / "mart_cartera" / "gestion=2024" / "mes=1"
    p2 = gold_dir / "mart_cartera" / "gestion=2024" / "mes=2"
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)
    (p1 / "part.parquet").write_bytes(b"dummy parquet")
    (p2 / "part.parquet").write_bytes(b"dummy parquet")

    adapter = DuckDBGoldPartitionAdapter(base_gold_path=gold_dir)

    partitions = adapter.list_available_partitions("mart_cartera")
    assert (2024, 1) in partitions
    assert (2024, 2) in partitions
    assert adapter.has_partition("mart_cartera", 2024, 1) is True
    assert adapter.has_partition("mart_cartera", 2024, 3) is False
    assert adapter.list_available_partitions("inexistente") == []


def test_derivation_filter_unknown_granularity() -> None:
    """Verifica que un periodo inválido retorne status NOT_DERIVABLE."""
    checker = MockGoldPartitionChecker()
    engine = AssetDerivationFilter(partition_checker=checker)

    req = DerivationEvaluationRequest(
        dataset_code="mart_cartera",
        period="FECHA_INVALIDA_XYZ",
    )
    result = engine.evaluate(req)
    assert result.status == DerivationStatus.NOT_DERIVABLE
    assert result.is_derivable is False
    assert "No fue posible determinar la granularidad" in result.explanation
