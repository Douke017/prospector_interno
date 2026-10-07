"""Contratos formales y especificaciones de datos bajo SDD."""

from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from prospector_interno.domain.contracts.derivation_spec import (
    DerivationEvaluationRequest,
    DerivationEvaluationResult,
    DerivationGranularity,
    DerivationStatus,
    PeriodBreakdown,
)
from prospector_interno.domain.contracts.downloader_spec import (
    CircuitState,
    DomainCircuitStatus,
    ResumableDownloadResult,
)

from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
)
from prospector_interno.domain.contracts.lineage_spec import (
    AssetLineageGraphResponse,
    DataLineageRecord,
    InternalAssetRecord,
    LineageIntegrityCheckResult,
    ProvenanceStep,
    StorageLayer,
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
    "AssetLineageGraphResponse",
    "CircuitState",
    "DataLineageRecord",
    "DerivationEvaluationRequest",
    "DerivationEvaluationResult",
    "DerivationGranularity",
    "DerivationStatus",
    "DomainCircuitStatus",

    "FinancialMetricDefinition",
    "GoldMartCarteraRecord",
    "InternalAssetRecord",
    "LineageIntegrityCheckResult",
    "MetricDataType",
    "PeriodBreakdown",
    "PredefinedViewMetadata",

    "PredefinedViewQueryRequest",
    "PredefinedViewResult",
    "ProvenanceStep",
    "ResumableDownloadResult",
    "SemanticDictionaryResponse",
    "SilverFinancialRecord",
    "StatementType",
    "StorageLayer",
]
