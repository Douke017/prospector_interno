"""Reglas de dominio y descomposición de granularidad temporal (HUI-02).

Descompone peticiones macro-temporales en sus meses atómicos requeridos y
compila la consulta analítica optimizada sobre la Capa Gold.
"""

import re

from prospector_interno.domain.contracts.derivation_spec import (
    DerivationGranularity,
    PeriodBreakdown,
)


def parse_period_granularity(raw_period: str) -> PeriodBreakdown:
    """Analiza una cadena de periodo y determina su granularidad y meses componentes."""
    clean = raw_period.strip().upper()

    # 1. Anual / Gestión completa (ej: '2024', '2024-ANNUAL', 'GESTION-2024')
    annual_pattern = r"^(?:GESTION[-_])?(\d{4})(?:[-_]?(?:ANNUAL|ANUAL))?$"
    annual_match = re.match(annual_pattern, clean)

    if annual_match:
        year = int(annual_match.group(1))
        _validate_year(year)
        months = list(range(1, 13))
        return PeriodBreakdown(
            raw_period=raw_period,
            granularity=DerivationGranularity.ANNUAL,
            gestion=year,
            required_months=months,
            required_period_keys=[f"{year}-{m:02d}" for m in months],
        )

    # 2. Semestral (ej: '2024-S1', '2024-S2', '2024-SEM1', '2024-SEM2')
    semester_match = re.match(r"^(\d{4})[-_]?(?:S|SEM)([12])$", clean)
    if semester_match:
        year = int(semester_match.group(1))
        sem = int(semester_match.group(2))
        _validate_year(year)
        months = list(range(1, 7)) if sem == 1 else list(range(7, 13))
        return PeriodBreakdown(
            raw_period=raw_period,
            granularity=DerivationGranularity.SEMESTER,
            gestion=year,
            required_months=months,
            required_period_keys=[f"{year}-{m:02d}" for m in months],
        )

    # 3. Trimestral (ej: '2024-Q1', '2024-Q2', '2024-T1', '2024-T4')
    quarter_match = re.match(r"^(\d{4})[-_]?(?:Q|T)([1-4])$", clean)
    if quarter_match:
        year = int(quarter_match.group(1))
        q = int(quarter_match.group(2))
        _validate_year(year)
        start_month = (q - 1) * 3 + 1
        months = [start_month, start_month + 1, start_month + 2]
        return PeriodBreakdown(
            raw_period=raw_period,
            granularity=DerivationGranularity.QUARTER,
            gestion=year,
            required_months=months,
            required_period_keys=[f"{year}-{m:02d}" for m in months],
        )

    # 4. Mensual individual (ej: '2024-05', '2024-5')
    monthly_match = re.match(r"^(\d{4})[-_](\d{1,2})$", clean)
    if monthly_match:
        year = int(monthly_match.group(1))
        m = int(monthly_match.group(2))
        _validate_year(year)
        if not (1 <= m <= 12):
            raise ValueError(f"Mes {m} fuera de rango permitido (1-12).")
        return PeriodBreakdown(
            raw_period=raw_period,
            granularity=DerivationGranularity.MONTHLY,
            gestion=year,
            required_months=[m],
            required_period_keys=[f"{year}-{m:02d}"],
        )

    raise ValueError(
        f"Formato de periodo no reconocido: '{raw_period}'. "
        "Formatos válidos: 'YYYY', 'YYYY-Q[1-4]', 'YYYY-S[1-2]', 'YYYY-MM'."
    )


def compile_derivation_sql(
    datamart: str,
    breakdown: PeriodBreakdown,
    parquet_glob: str,
    institution_id: str | None = None,
) -> str:
    """Genera la consulta DuckDB vectorizada para agregar los meses sobre Gold."""
    months_str = ", ".join(str(m) for m in breakdown.required_months)
    inst_clause = (
        f" AND UPPER(institucion_id) = '{institution_id.strip().upper()}'"
        if institution_id
        else ""
    )

    return (
        "SELECT "
        "institucion_id, "
        "institucion_nombre, "
        f"{breakdown.gestion} AS gestion, "
        f"'{breakdown.raw_period}' AS periodo_derivado, "
        "SUM(cartera_bruta) AS cartera_bruta_acumulada, "
        "SUM(cartera_vigente) AS cartera_vigente_acumulada, "
        "SUM(cartera_vencida) AS cartera_vencida_acumulada, "
        "SUM(cartera_en_ejecucion) AS cartera_en_ejecucion_acumulada, "
        "SUM(cartera_en_mora) AS cartera_en_mora_acumulada, "
        "SUM(prevision_cartera) AS prevision_cartera_acumulada, "
        "ROUND(AVG(total_activo), 2) AS total_activo_promedio, "
        "ROUND(AVG(total_patrimonio), 2) AS total_patrimonio_promedio, "
        "ROUND(SUM(cartera_en_mora) / NULLIF(SUM(cartera_bruta), 0), 4) "
        "AS ratio_mora_acumulado, "
        "ROUND(SUM(prevision_cartera) / NULLIF(SUM(cartera_en_mora), 0), 4) "
        "AS ratio_cobertura_acumulado, "
        "COUNT(DISTINCT mes) AS meses_agregados "
        f"FROM read_parquet('{parquet_glob}') "
        f"WHERE gestion = {breakdown.gestion} AND mes IN ({months_str})"
        f"{inst_clause} "
        "GROUP BY institucion_id, institucion_nombre "
        "ORDER BY cartera_bruta_acumulada DESC;"
    )


def _validate_year(year: int) -> None:
    if not (1990 <= year <= 2100):
        raise ValueError(f"Año {year} fuera del rango histórico (1990-2100).")
