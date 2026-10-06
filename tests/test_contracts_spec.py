"""Pruebas unitarias de contratos y especificaciones bajo SDD."""

from decimal import Decimal
import pytest
from pydantic import ValidationError

from prospector_interno.domain.contracts import (
    AccountCategory,
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
    GoldMartCarteraRecord,
    SilverFinancialRecord,
    StatementType,
)


def test_silver_financial_record_valid() -> None:
    """Verifica que un registro válido cumpla la especificación Silver."""
    record = SilverFinancialRecord(
        source_id="finrural",
        institution_id="CRECER",
        institution_name="Crecer IFD",
        period="2026-01",
        gestion=2026,
        mes=1,
        statement_type=StatementType.SITUACION_PATRIMONIAL,
        account_category=AccountCategory.CARTERA,
        account_name="CARTERA_BRUTA",
        amount_bob=Decimal("3010769544.00"),
        source_file_sha256="abc123sha256fake",
    )
    assert record.institution_id == "CRECER"
    assert record.amount_bob == Decimal("3010769544.00")
    assert record.mes == 1


def test_silver_financial_record_rejects_extra_fields() -> None:
    """Verifica que campos fuera de contrato sean estrictamente rechazados."""
    with pytest.raises(ValidationError):
        SilverFinancialRecord(
            source_id="finrural",
            institution_id="CRECER",
            institution_name="Crecer IFD",
            period="2026-01",
            gestion=2026,
            mes=1,
            statement_type=StatementType.SITUACION_PATRIMONIAL,
            account_category=AccountCategory.CARTERA,
            account_name="CARTERA_BRUTA",
            amount_bob=Decimal("100.00"),
            source_file_sha256="fake_sha",
            campo_no_autorizado="error",  # Extra no permitido
        )


def test_gold_mart_cartera_record_valid() -> None:
    """Verifica que el modelo dimensional cumpla el contrato de Capa Gold."""
    record = GoldMartCarteraRecord(
        gestion=2026,
        mes=1,
        institucion_id="CRECER",
        institucion_nombre="Crecer IFD",
        cartera_bruta=Decimal("3010769544.00"),
        cartera_vigente=Decimal("2849456353.00"),
        cartera_vencida=Decimal("19480526.00"),
        cartera_en_ejecucion=Decimal("11746159.00"),
        cartera_en_mora=Decimal("31226685.00"),
        prevision_cartera=Decimal("177151212.00"),
        total_activo=Decimal("3614696244.00"),
        total_pasivo=Decimal("3087015720.00"),
        total_patrimonio=Decimal("527680524.00"),
        ratio_mora=Decimal("0.0104"),
        ratio_cobertura=Decimal("5.6731"),
        source_file_sha256="sha256_mock_file",
    )
    assert record.gestion == 2026
    assert record.mes == 1
    assert record.cartera_en_mora == Decimal("31226685.00")


def test_gold_mart_cartera_record_bounds_check() -> None:
    """Verifica que valores ilegales de mes o año sean rechazados."""
    with pytest.raises(ValidationError):
        GoldMartCarteraRecord(
            gestion=1900,  # Año fuera de rango
            mes=13,  # Mes inválido
            institucion_id="CRECER",
            institucion_nombre="Crecer IFD",
            cartera_bruta=Decimal("100"),
            cartera_vigente=Decimal("100"),
            cartera_vencida=Decimal("0"),
            cartera_en_ejecucion=Decimal("0"),
            cartera_en_mora=Decimal("0"),
            prevision_cartera=Decimal("10"),
            total_activo=Decimal("200"),
            total_pasivo=Decimal("100"),
            total_patrimonio=Decimal("100"),
            ratio_mora=Decimal("0"),
            ratio_cobertura=Decimal("0"),
            source_file_sha256="sha",
        )


def test_analytics_query_specs() -> None:
    """Verifica la serialización y tipos del contrato analítico."""
    req = AnalyticsQueryRequest(
        datamart="mart_cartera",
        institutions=["CRECER"],
        gestiones=[2026],
        meses=[1],
        metrics=["cartera_bruta", "ratio_mora"],
    )
    assert req.datamart == "mart_cartera"
    assert req.institutions == ["CRECER"]

    res = AnalyticsQueryResponse(
        datamart="mart_cartera",
        total_records=1,
        execution_time_ms=12.4,
        columns=["institucion_id", "cartera_bruta"],
        records=[{"institucion_id": "CRECER", "cartera_bruta": 3010769544.0}],
    )
    assert res.total_records == 1
    assert len(res.records) == 1
