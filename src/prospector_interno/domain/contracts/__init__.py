"""Contratos formales y especificaciones de datos bajo SDD."""

from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
)
from prospector_interno.domain.contracts.semantic_spec import (
    FinancialMetricDefinition,
    MetricDataType,
    SemanticDictionaryResponse,
)
from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    SilverFinancialRecord,
    StatementType,
)
from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewMetadata,
    PredefinedViewQueryRequest,
    PredefinedViewResult,
)

__all__ = [
    "AccountCategory",
    "AnalyticsQueryRequest",
    "AnalyticsQueryResponse",
    "FinancialMetricDefinition",
    "GoldMartCarteraRecord",
    "MetricDataType",
    "PredefinedViewMetadata",
    "PredefinedViewQueryRequest",
    "PredefinedViewResult",
    "SemanticDictionaryResponse",
    "SilverFinancialRecord",
    "StatementType",
]
