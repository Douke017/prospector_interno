"""Filtro de Derivación y Reutilización sobre Tablas Consolidadas (HUI-02).

Evalúa si una petición macro-temporal (anual, semestral o trimestral) puede
construirse agregando particiones mensuales existentes en la Capa Gold sin
disparar descargas de archivos físicos ni pipelines de parseo redundantes.
"""

from pathlib import Path

from prospector_interno.domain.contracts.derivation_spec import (
    DerivationEvaluationRequest,
    DerivationEvaluationResult,
    DerivationGranularity,
    DerivationStatus,
)
from prospector_interno.domain.gatekeeper.derivation_rules import (
    compile_derivation_sql,
    parse_period_granularity,
)
from prospector_interno.domain.ports import GoldPartitionPort


class AssetDerivationFilter:
    """Filtro 2 del Gatekeeper: Evaluación de derivación temporal sobre Capa Gold."""

    def __init__(
        self,
        partition_checker: GoldPartitionPort,
        base_gold_path: Path | None = None,
        bronze_storage: object | None = None,
    ) -> None:
        self._checker = partition_checker
        self._gold_path = base_gold_path or Path("data/gold")
        self._bronze_storage = bronze_storage

    def _has_master_archive_coverage(self, dataset_code: str, year: int) -> bool:
        """Verifica si existe un archivo maestro anual/acumulado en custodia Bronze."""
        if self._bronze_storage is None or not hasattr(self._bronze_storage, "find_by_period"):
            return False
        for candidate_period in (str(year), f"{year}-ANNUAL", f"{year}-MASTER"):
            art = self._bronze_storage.find_by_period(
                domain="",
                dataset_code=dataset_code,
                period=candidate_period,
            )
            if art is not None:
                return True
        return False

    def evaluate(
        self,
        request: DerivationEvaluationRequest,
    ) -> DerivationEvaluationResult:
        """Determina si la solicitud es derivable (Gold + maestros Bronze) y genera SQL."""
        try:
            breakdown = parse_period_granularity(request.period)
        except ValueError as err:
            return DerivationEvaluationResult(
                status=DerivationStatus.NOT_DERIVABLE,
                is_derivable=False,
                target_period=request.period,
                granularity=DerivationGranularity.MONTHLY,
                required_periods=[],
                available_periods=[],
                missing_periods=[],
                coverage_ratio=0.0,
                precompiled_sql=None,
                explanation=f"No fue posible determinar la granularidad: {err}",
            )

        available_periods: list[str] = []
        missing_periods: list[str] = []
        master_years_cache: dict[int, bool] = {}

        for key in breakdown.required_period_keys:
            yr_s, mo_s = key.split("-")
            yr_i, mo_i = int(yr_s), int(mo_s)
            has_gold = self._checker.has_partition(
                datamart=request.dataset_code,
                gestion=yr_i,
                mes=mo_i,
            )
            if not has_gold:
                if yr_i not in master_years_cache:
                    master_years_cache[yr_i] = self._has_master_archive_coverage(
                        request.dataset_code, yr_i
                    )
                has_gold = master_years_cache[yr_i]

            if has_gold:
                available_periods.append(key)
            else:
                missing_periods.append(key)

        total_required = len(breakdown.required_period_keys)
        coverage_ratio = round(len(available_periods) / max(1, total_required), 4)

        if coverage_ratio == 1.0:
            status = DerivationStatus.DERIVABLE
            is_derivable = True
            parquet_glob = (
                (self._gold_path / request.dataset_code / "**/*.parquet")
                .resolve()
                .as_posix()
            )
            precompiled_sql = compile_derivation_sql(
                datamart=request.dataset_code,
                breakdown=breakdown,
                parquet_glob=parquet_glob,
                institution_id=request.institution_id,
            )
            explanation = (
                f"Derivación total aprobada: 100% de cobertura disponible para "
                f"'{request.period}' ({len(available_periods)}/{total_required} "
                "periodos mensuales consolidados en Capa Gold)."
            )
        else:
            status = DerivationStatus.MISSING_PERIODS
            is_derivable = False
            precompiled_sql = None
            pct = coverage_ratio * 100
            missing_str = ", ".join(missing_periods)
            explanation = (
                f"Derivación rechazada por cobertura incompleta "
                f"({len(available_periods)}/{total_required} disponibles, "
                f"{pct:.1f}%). Requiere ingesta de {len(missing_periods)} "
                f"periodo(s) faltante(s): {missing_str}."
            )

        return DerivationEvaluationResult(

            status=status,
            is_derivable=is_derivable,
            target_period=request.period,
            granularity=breakdown.granularity,
            required_periods=breakdown.required_period_keys,
            available_periods=available_periods,
            missing_periods=missing_periods,
            coverage_ratio=coverage_ratio,
            precompiled_sql=precompiled_sql,
            explanation=explanation,
        )
