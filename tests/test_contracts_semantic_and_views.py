"""Pruebas de especificación para contratos semánticos y vistas (SDD)."""

import pytest
from pydantic import ValidationError

from prospector_interno.domain.contracts.semantic_spec import (
    FinancialMetricDefinition,
    MetricDataType,
    SemanticDictionaryResponse,
)
from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewMetadata,
    PredefinedViewQueryRequest,
    PredefinedViewResult,
)


def test_financial_metric_definition_valid() -> None:
    """Valida la creación correcta de una definición semántica de métrica."""
    metric = FinancialMetricDefinition(
        metric_code="ratio_mora",
        canonical_name="Ratio de Mora Cartera",
        category="CARTERA",
        data_type=MetricDataType.PERCENTAGE,
        unit="Porcentaje (%)",
        formula_latex=r"\frac{\text{mora}}{\text{bruta}} \times 100",
        description="Porcentaje de cartera con atraso superior a 30 días.",
        required_fields=["cartera_en_mora", "cartera_bruta"],
    )
    assert metric.metric_code == "ratio_mora"
    assert metric.data_type == MetricDataType.PERCENTAGE
    assert "cartera_en_mora" in metric.required_fields


def test_financial_metric_definition_rejects_extra_fields() -> None:
    """Verifica que el contrato semántico prohíba atributos no reconocidos."""
    with pytest.raises(ValidationError):
        FinancialMetricDefinition(
            metric_code="ratio_mora",
            canonical_name="Ratio de Mora",
            category="CARTERA",
            unit="%",
            formula_latex="formula",
            description="desc",
            campo_desconocido="hack",
        )


def test_semantic_dictionary_response_model() -> None:
    """Valida la respuesta consolidada del diccionario semántico."""
    metric = FinancialMetricDefinition(
        metric_code="cartera_bruta",
        canonical_name="Cartera Bruta Total",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\sum \text{prestamos}",
        description="Saldo total de préstamos antes de previsiones.",
    )
    response = SemanticDictionaryResponse(
        total_metrics=1,
        categories=["CARTERA"],
        metrics=[metric],
    )
    assert response.total_metrics == 1
    assert response.categories == ["CARTERA"]
    assert len(response.metrics) == 1


def test_predefined_view_metadata_valid() -> None:
    """Valida la especificación de metadatos de vistas analíticas."""
    meta = PredefinedViewMetadata(
        view_id="evolucion-mora-temporal",
        title="Evolución de Mora",
        description="Serie temporal mensual de mora agregada.",
        category="RIESGO",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=["gestion", "mes", "cartera_en_mora", "ratio_mora"],
    )
    assert meta.view_id == "evolucion-mora-temporal"
    assert meta.category == "RIESGO"


def test_predefined_view_metadata_rejects_extra_fields() -> None:
    """Verifica que PredefinedViewMetadata rechace campos no especificados."""
    with pytest.raises(ValidationError):
        PredefinedViewMetadata(
            view_id="vista-test",
            title="Vista Test",
            description="Desc",
            category="RIESGO",
            inyeccion_insegura=True,
        )


def test_predefined_view_query_request_bounds() -> None:
    """Valida los límites numéricos de paginación/limit en consultas."""
    req_default = PredefinedViewQueryRequest()
    assert req_default.limit == 100

    # Límite superior excedido (> 1000)
    with pytest.raises(ValidationError):
        PredefinedViewQueryRequest(limit=2000)

    # Límite inferior inválido (< 1)
    with pytest.raises(ValidationError):
        PredefinedViewQueryRequest(limit=0)


def test_predefined_view_result_valid() -> None:
    """Valida el contenedor de resultados de ejecución de vista."""
    result = PredefinedViewResult(
        view_id="ranking-solvencia",
        title="Ranking Solvencia",
        execution_time_ms=3.45,
        total_records=1,
        columns=["institucion_id", "ratio_cobertura"],
        records=[{"institucion_id": "CRECER", "ratio_cobertura": 1.45}],
    )
    assert result.total_records == 1
    assert result.execution_time_ms == 3.45
    assert result.records[0]["institucion_id"] == "CRECER"
