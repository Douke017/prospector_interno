"""Servicio del Diccionario Semántico de Métricas Financieras (HUI-16)."""

from prospector_interno.domain.contracts.semantic_spec import (
    FinancialMetricDefinition,
    MetricDataType,
    SemanticDictionaryResponse,
)

CANONICAL_METRICS: list[FinancialMetricDefinition] = [
    FinancialMetricDefinition(
        metric_code="cartera_bruta",
        canonical_name="Cartera Bruta Total",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\sum \text{prestamos}",
        description=(
            "Saldo total de capital de préstamos otorgados antes de previsiones."
        ),
        required_fields=["cartera_bruta"],
    ),
    FinancialMetricDefinition(
        metric_code="cartera_vigente",
        canonical_name="Cartera Vigente",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{cartera\_al\_dia}",
        description="Saldo de créditos sin ningún atraso en sus pagos programados.",
        required_fields=["cartera_vigente"],
    ),
    FinancialMetricDefinition(
        metric_code="cartera_vencida",
        canonical_name="Cartera Vencida",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{creditos\_en\_mora\_corta}",
        description=(
            "Saldo de créditos en mora a corto plazo (atraso entre 31 y 90 días)."
        ),
        required_fields=["cartera_vencida"],
    ),
    FinancialMetricDefinition(
        metric_code="cartera_en_ejecucion",
        canonical_name="Cartera en Ejecución",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{cobranza\_judicial}",
        description="Saldo de créditos en proceso de cobro judicial y ejecución.",
        required_fields=["cartera_en_ejecucion"],
    ),
    FinancialMetricDefinition(
        metric_code="cartera_en_mora",
        canonical_name="Cartera en Mora Total",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{cartera\_vencida} + \text{cartera\_en\_ejecucion}",
        description=(
            "Saldo total de créditos vencidos y en ejecución con atraso superior "
            "a 30 días calendario."
        ),
        required_fields=["cartera_vencida", "cartera_en_ejecucion"],
    ),
    FinancialMetricDefinition(
        metric_code="prevision_cartera",
        canonical_name="Previsión para Cartera Incobrable",
        category="CARTERA",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"|\text{prevision\_especifica}| + |\text{prevision\_generica}|",
        description=(
            "Previsión contable constituida para absorber contingencias y pérdidas "
            "crediticias estimadas."
        ),
        required_fields=["prevision_cartera"],
    ),
    FinancialMetricDefinition(
        metric_code="total_activo",
        canonical_name="Total del Activo",
        category="BALANCE",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{recursos\_economicos\_totales}",
        description=(
            "Total de activos institucionales reportados en el balance general."
        ),
        required_fields=["total_activo"],
    ),
    FinancialMetricDefinition(
        metric_code="total_pasivo",
        canonical_name="Total del Pasivo",
        category="BALANCE",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{obligaciones\_totales}",
        description=(
            "Total de pasivos y obligaciones financieras contraídas con el público "
            "y entidades bancarias."
        ),
        required_fields=["total_pasivo"],
    ),
    FinancialMetricDefinition(
        metric_code="total_patrimonio",
        canonical_name="Total del Patrimonio",
        category="BALANCE",
        data_type=MetricDataType.DECIMAL,
        unit="Bolivianos (BOB)",
        formula_latex=r"\text{activo} - \text{pasivo}",
        description=(
            "Capital social, reservas patrimoniales y resultados acumulados "
            "de la entidad."
        ),
        required_fields=["total_patrimonio"],
    ),
    FinancialMetricDefinition(
        metric_code="ratio_mora",
        canonical_name="Ratio de Morosidad de Cartera",
        category="CARTERA",
        data_type=MetricDataType.PERCENTAGE,
        unit="Porcentaje (%)",
        formula_latex=(
            r"\frac{\text{cartera\_en\_mora}}{\text{cartera\_bruta}} \times 100"
        ),
        description=(
            "Porcentaje de la cartera crediticia en mora respecto al total de "
            "la cartera bruta colocada."
        ),
        required_fields=["cartera_en_mora", "cartera_bruta"],
    ),
    FinancialMetricDefinition(
        metric_code="ratio_cobertura",
        canonical_name="Ratio de Cobertura de Previsiones",
        category="SOLVENCIA",
        data_type=MetricDataType.PERCENTAGE,
        unit="Porcentaje (%)",
        formula_latex=(
            r"\frac{\text{prevision\_cartera}}"
            r"{\text{cartera\_en\_mora}} \times 100"
        ),
        description=(
            "Proporción en que las previsiones acumuladas cubren el saldo total "
            "de cartera en mora."
        ),
        required_fields=["prevision_cartera", "cartera_en_mora"],
    ),
    FinancialMetricDefinition(
        metric_code="roa",
        canonical_name="Retorno sobre Activos (ROA)",
        category="RENTABILIDAD",
        data_type=MetricDataType.PERCENTAGE,
        unit="Porcentaje (%)",
        formula_latex=(
            r"\frac{\text{utilidad\_neta}}{\text{activo\_promedio}} \times 100"
        ),
        description=(
            "Medida de rentabilidad de los activos institucionales generada en "
            "el período (anualizado)."
        ),
        required_fields=["utilidad_neta", "total_activo"],
    ),
    FinancialMetricDefinition(
        metric_code="roe",
        canonical_name="Retorno sobre Patrimonio (ROE)",
        category="RENTABILIDAD",
        data_type=MetricDataType.PERCENTAGE,
        unit="Porcentaje (%)",
        formula_latex=(
            r"\frac{\text{utilidad\_neta}}{\text{patrimonio\_promedio}} \times 100"
        ),
        description=(
            "Medida de rendimiento financiero sobre los fondos propios de la entidad."
        ),
        required_fields=["utilidad_neta", "total_patrimonio"],
    ),
]


class SemanticDictionaryService:
    """Servicio de catálogo y consulta de definiciones semánticas oficiales."""

    def __init__(
        self,
        metrics: list[FinancialMetricDefinition] | None = None,
    ) -> None:
        self._metrics_list = metrics or list(CANONICAL_METRICS)
        self._by_code: dict[str, FinancialMetricDefinition] = {
            m.metric_code.lower(): m for m in self._metrics_list
        }

    def list_metrics(
        self,
        category: str | None = None,
    ) -> SemanticDictionaryResponse:
        """Retorna las métricas registradas, opcionalmente filtradas por categoría."""
        if category:
            cat_upper = category.strip().upper()
            filtered = [m for m in self._metrics_list if m.category == cat_upper]
        else:
            filtered = list(self._metrics_list)

        categories = sorted(list({m.category for m in self._metrics_list}))
        return SemanticDictionaryResponse(
            total_metrics=len(filtered),
            categories=categories,
            metrics=filtered,
        )

    def get_metric(self, metric_code: str) -> FinancialMetricDefinition | None:
        """Busca una métrica por su código canónico único."""
        return self._by_code.get(metric_code.strip().lower())
