"""Servicio de ejecución y catálogo de Vistas Analíticas Predefinidas."""

from __future__ import annotations

import math
from pathlib import Path
import time
from typing import Any
import duckdb

from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewMetadata,
    PredefinedViewQueryRequest,
    PredefinedViewResult,
)
from prospector_interno.domain.exceptions import ProspectorError


class ViewNotFoundError(ProspectorError):
    """Excepción lanzada cuando una vista analítica no está registrada."""


VIEWS_CATALOG: list[PredefinedViewMetadata] = [
    PredefinedViewMetadata(
        view_id="evolucion-mora-temporal",
        title="Evolución Temporal de Cartera y Mora",
        description=(
            "Serie cronológica de cartera bruta, cartera en mora y ratio de "
            "morosidad agrupada por gestión y mes."
        ),
        category="RIESGO",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "gestion",
            "mes",
            "institucion_id",
            "cartera_bruta",
            "cartera_en_mora",
            "ratio_mora",
        ],
    ),
    PredefinedViewMetadata(
        view_id="ranking-solvencia-cobertura",
        title="Ranking de Solvencia y Cobertura de Previsiones",
        description=(
            "Clasificación jerárquica de entidades financieras ordenadas de mayor "
            "a menor ratio de cobertura de previsiones sobre cartera vencida."
        ),
        category="SOLVENCIA",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "gestion",
            "mes",
            "total_patrimonio",
            "prevision_cartera",
            "cartera_en_mora",
            "ratio_cobertura",
        ],
    ),
    PredefinedViewMetadata(
        view_id="comparativo-entidades-resumen",
        title="Resumen Comparativo de Entidades Financieras",
        description=(
            "Fotografía comparativa transversal de activos, pasivos, patrimonio "
            "y cartera colocada por cada entidad financiera."
        ),
        category="BENCHMARK",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "gestion",
            "mes",
            "total_activo",
            "total_pasivo",
            "total_patrimonio",
            "cartera_bruta",
            "ratio_mora",
        ],
    ),
]

EXTENDED_VIEWS_CATALOG: list[PredefinedViewMetadata] = [
    *VIEWS_CATALOG,
    PredefinedViewMetadata(
        view_id="evolucion-monetaria-bcb",
        title="Evolución Monetaria, Reservas Internacionales e Inflación (BCB)",
        description=(
            "Serie cronológica del Banco Central de Bolivia con base monetaria, "
            "reservas internacionales netas, inflación mensual, bolivianización y UFV."
        ),
        category="MONETARIO",
        supported_filters=["gestion", "mes", "limit"],
        output_columns=[
            "gestion",
            "mes",
            "institucion_id",
            "base_monetaria_bs",
            "emision_monetaria_bs",
            "reservas_internacionales_usd",
            "inflacion_mensual_pct",
            "bolivianizacion_depositos_pct",
            "ufv_cierre",
        ],
    ),
    PredefinedViewMetadata(
        view_id="estados-financieros-bbv",
        title="Liquidez, Inversiones Bursátiles y Solvencia Patrimonial (BBV)",
        description=(
            "Evolución mensual de activos corrientes, inversiones bursátiles, "
            "patrimonio y razones de liquidez y endeudamiento de la BBV S.A."
        ),
        category="BURSATIL",
        supported_filters=["gestion", "mes", "limit"],
        output_columns=[
            "gestion",
            "mes",
            "institucion_id",
            "disponible_bs",
            "inversiones_bursatiles_bs",
            "total_activo",
            "total_pasivo",
            "total_patrimonio",
            "ratio_liquidez",
            "ratio_endeudamiento",
        ],
    ),
    PredefinedViewMetadata(
        view_id="solvencia-prudencial-asfi",
        title="Tablero Prudencial de Solvencia y Apalancamiento (ASFI)",
        description=(
            "Evaluación supervisada de coeficiente de solvencia patrimonial, "
            "peso de cartera sobre activo y apalancamiento financiero."
        ),
        category="SUPERVISION",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "gestion",
            "mes",
            "total_activo",
            "total_patrimonio",
            "coeficiente_solvencia",
            "peso_cartera_activo",
            "ratio_mora",
            "apalancamiento_veces",
        ],
    ),
]

VIEW_TO_DATAMART: dict[str, str] = {
    "evolucion-mora-temporal": "mart_cartera",
    "ranking-solvencia-cobertura": "mart_cartera",
    "comparativo-entidades-resumen": "mart_cartera",
    "evolucion-monetaria-bcb": "mart_tasas_bcb",
    "estados-financieros-bbv": "mart_estadisticas_bbv",
    "solvencia-prudencial-asfi": "mart_indicadores_asfi",
}


class PredefinedViewsService:
    """Motor de consulta y orquestación de vistas analíticas predefinidas."""

    def __init__(self, base_gold_path: Path | None = None) -> None:
        self._gold_path = base_gold_path or Path("data/gold")
        self._views_map = {v.view_id: v for v in EXTENDED_VIEWS_CATALOG}

    def list_views(
        self,
        datamart: str | None = None,
    ) -> list[PredefinedViewMetadata]:
        """Retorna vistas predefinidas (por defecto las 3 estándar o filtradas por repositorio)."""
        if datamart is None:
            return list(VIEWS_CATALOG)
        dm = datamart.strip().lower()
        if dm == "all":
            return list(EXTENDED_VIEWS_CATALOG)
        return [
            v
            for v in EXTENDED_VIEWS_CATALOG
            if VIEW_TO_DATAMART.get(v.view_id) == dm
        ]

    def get_view(self, view_id: str) -> PredefinedViewMetadata | None:
        """Obtiene la metadata de una vista predefinida por su identificador."""
        return self._views_map.get(view_id.strip().lower())

    def _resolve_parquet_glob(self, datamart: str = "mart_cartera") -> str | None:
        mart_dir = self._gold_path / datamart
        if not mart_dir.exists():
            return None
        files = list(mart_dir.glob("**/*.parquet"))
        if not files:
            return None
        return (mart_dir / "**/*.parquet").resolve().as_posix()

    def execute_view(
        self,
        view_id: str,
        request: PredefinedViewQueryRequest | None = None,
    ) -> PredefinedViewResult:
        """Ejecuta una vista analítica aplicando filtros opcionales de forma segura."""
        t_start = time.perf_counter()
        clean_id = view_id.strip().lower()
        view_meta = self.get_view(clean_id)
        if not view_meta:
            raise ViewNotFoundError(
                f"La vista analítica '{view_id}' no existe en el catálogo."
            )

        req = request or PredefinedViewQueryRequest()
        target_dm = VIEW_TO_DATAMART.get(clean_id, "mart_cartera")
        glob_path = self._resolve_parquet_glob(target_dm)

        if not glob_path:
            return PredefinedViewResult(
                view_id=clean_id,
                title=view_meta.title,
                execution_time_ms=0.0,
                total_records=0,
                columns=view_meta.output_columns,
                records=[],
            )

        where_clauses = ["1=1"]
        params: list[Any] = []

        if req.gestion is not None:
            where_clauses.append("gestion = ?")
            params.append(int(req.gestion))

        if req.mes is not None:
            where_clauses.append("mes = ?")
            params.append(int(req.mes))

        if req.institutions:
            ph = ", ".join(["?"] * len(req.institutions))
            where_clauses.append(f"institucion_id IN ({ph})")
            params.extend(req.institutions)

        where_sql = " AND ".join(where_clauses)
        cols_sql = ", ".join(view_meta.output_columns)

        if clean_id in ("evolucion-mora-temporal", "evolucion-monetaria-bcb", "estados-financieros-bbv"):
            order_sql = "ORDER BY gestion DESC, mes DESC, institucion_id ASC"
            if clean_id == "evolucion-mora-temporal":
                order_sql = "ORDER BY gestion ASC, mes ASC, institucion_id ASC"
        elif clean_id == "ranking-solvencia-cobertura":
            order_sql = "ORDER BY ratio_cobertura DESC, total_patrimonio DESC"
        elif clean_id == "solvencia-prudencial-asfi":
            order_sql = "ORDER BY coeficiente_solvencia DESC, total_patrimonio DESC"
        else:
            order_sql = "ORDER BY cartera_bruta DESC, institucion_id ASC"

        limit_val = int(req.limit)
        sql = f"""
            SELECT {cols_sql}
            FROM read_parquet('{glob_path}')
            WHERE {where_sql}
            {order_sql}
            LIMIT {limit_val}
        """

        con = duckdb.connect(":memory:")
        try:
            con.execute("SET max_memory = '4GB'")
            cursor = con.execute(sql, params)
            raw_cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
        finally:
            con.close()

        records: list[dict[str, Any]] = []
        for row in rows:
            rec_dict = {}
            for col_name, val in zip(raw_cols, row):
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    rec_dict[col_name] = None
                else:
                    rec_dict[col_name] = val
            records.append(rec_dict)

        t_end = time.perf_counter()
        elapsed_ms = round((t_end - t_start) * 1000, 2)

        return PredefinedViewResult(
            view_id=clean_id,
            title=view_meta.title,
            execution_time_ms=elapsed_ms,
            total_records=len(records),
            columns=raw_cols,
            records=records,
        )
