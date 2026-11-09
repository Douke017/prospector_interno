"""Adaptador analítico vectorizado sobre los 4 Repositorios Consolidados Gold."""

from __future__ import annotations

import math
from pathlib import Path
import time
from typing import Any
import duckdb

from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from prospector_interno.domain.ports import GoldPartitionPort


DATAMART_CATALOG: dict[str, dict[str, Any]] = {
    "mart_cartera": {
        "datamart": "mart_cartera",
        "title": "Repositorio de Cartera y Mora (FINRURAL / IFDs)",
        "source_code": "FINRURAL",
        "description": (
            "Estados de situación patrimonial, colocaciones de cartera bruta, "
            "mora, previsiones y ratios de cobertura de entidades financieras de desarrollo."
        ),
        "default_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "cartera_bruta",
            "cartera_vigente",
            "cartera_en_mora",
            "ratio_mora",
            "total_activo",
        ],
        "allowed_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "institucion_nombre",
            "cartera_bruta",
            "cartera_vigente",
            "cartera_vencida",
            "cartera_en_ejecucion",
            "cartera_en_mora",
            "prevision_cartera",
            "total_activo",
            "total_pasivo",
            "total_patrimonio",
            "ratio_mora",
            "ratio_cobertura",
            "source_file_sha256",
        ],
    },
    "mart_tasas_bcb": {
        "datamart": "mart_tasas_bcb",
        "title": "Repositorio Monetario, Liquidez e Inflación (BCB)",
        "source_code": "BCB",
        "description": (
            "Series mensuales consolidadas del Banco Central de Bolivia: base y emisión "
            "monetaria, reservas internacionales, depósitos, bolivianización, IPC y UFV."
        ),
        "default_columns": [
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
        "allowed_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "institucion_nombre",
            "base_monetaria_bs",
            "emision_monetaria_bs",
            "reservas_bancarias_bs",
            "depositos_sistema_bs",
            "cartera_sistema_bs",
            "reservas_internacionales_usd",
            "inflacion_mensual_pct",
            "bolivianizacion_depositos_pct",
            "ufv_cierre",
            "source_file_sha256",
        ],
    },
    "mart_estadisticas_bbv": {
        "datamart": "mart_estadisticas_bbv",
        "title": "Repositorio Bursátil y Patrimonial (BBV)",
        "source_code": "BBV",
        "description": (
            "Balances generales consolidados de la Bolsa Boliviana de Valores S.A.: "
            "disponibilidades, inversiones bursátiles, liquidez corriente y solvencia."
        ),
        "default_columns": [
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
        "allowed_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "institucion_nombre",
            "disponible_bs",
            "inversiones_bursatiles_bs",
            "activo_corriente_bs",
            "inversiones_permanentes_bs",
            "total_activo",
            "pasivo_corriente_bs",
            "total_pasivo",
            "total_patrimonio",
            "ratio_liquidez",
            "ratio_endeudamiento",
            "source_file_sha256",
        ],
    },
    "mart_indicadores_asfi": {
        "datamart": "mart_indicadores_asfi",
        "title": "Repositorio de Indicadores Prudenciales (ASFI)",
        "source_code": "ASFI",
        "description": (
            "Indicadores prudenciales consolidados bajo normativa de supervisión financiera: "
            "coeficiente de solvencia, apalancamiento, peso de cartera y cobertura."
        ),
        "default_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "institucion_nombre",
            "total_activo",
            "total_patrimonio",
            "coeficiente_solvencia",
            "peso_cartera_activo",
            "ratio_mora",
            "apalancamiento_veces",
        ],
        "allowed_columns": [
            "gestion",
            "mes",
            "institucion_id",
            "institucion_nombre",
            "total_activo",
            "total_pasivo",
            "total_patrimonio",
            "cartera_bruta",
            "prevision_cartera",
            "coeficiente_solvencia",
            "peso_cartera_activo",
            "ratio_mora",
            "ratio_cobertura",
            "apalancamiento_veces",
            "source_file_sha256",
        ],
    },
}

ALLOWED_COLUMNS: set[str] = {
    col
    for cfg in DATAMART_CATALOG.values()
    for col in cfg["allowed_columns"]
}


class DuckDBAnalyticsAdapter:
    """Motor de consulta analítica vectorizada sobre los 4 repositorios Gold."""

    def __init__(
        self,
        base_gold_path: Path | None = None,
        max_memory: str = "4GB",
    ) -> None:
        self._gold_path = base_gold_path or Path("data/gold")
        self._max_memory = max_memory

    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute(f"SET max_memory = '{self._max_memory}'")
        return con

    def get_max_memory_setting(self) -> str:
        con = self._connect()
        try:
            return con.execute(
                "SELECT current_setting('max_memory')"
            ).fetchone()[0]
        finally:
            con.close()

    def _resolve_parquet_glob(self, datamart: str) -> str | None:
        mart_dir = self._gold_path / datamart
        if not mart_dir.exists():
            return None
        parquet_files = list(mart_dir.glob("**/*.parquet"))
        if not parquet_files:
            return None
        return (mart_dir / "**/*.parquet").resolve().as_posix()

    def has_data(
        self,
        datamart: str = "mart_cartera",
        gestion: int | None = None,
        mes: int | None = None,
    ) -> bool:
        glob_path = self._resolve_parquet_glob(datamart)
        if not glob_path:
            return False
        if gestion is None and mes is None:
            return True

        con = self._connect()
        clauses = []
        if gestion is not None:
            clauses.append(f"gestion = {int(gestion)}")
        if mes is not None:
            clauses.append(f"mes = {int(mes)}")

        where_str = " AND ".join(clauses)
        query = f"SELECT count(*) FROM read_parquet('{glob_path}') WHERE {where_str}"
        try:
            count = con.execute(query).fetchone()[0]
            return count > 0
        except Exception:
            return False
        finally:
            con.close()

    def get_dimensions(
        self,
        datamart: str = "mart_cartera",
    ) -> dict[str, list[Any]]:
        glob_path = self._resolve_parquet_glob(datamart)
        if not glob_path:
            return {"instituciones": [], "gestiones": [], "meses": []}

        con = self._connect()
        try:
            insts = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT institucion_id FROM read_parquet('{glob_path}') ORDER BY 1"
                ).fetchall()
            ]
            gestiones = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT gestion FROM read_parquet('{glob_path}') ORDER BY 1"
                ).fetchall()
            ]
            meses = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT mes FROM read_parquet('{glob_path}') ORDER BY 1"
                ).fetchall()
            ]
            return {
                "instituciones": insts,
                "gestiones": gestiones,
                "meses": meses,
            }
        except Exception:
            return {"instituciones": [], "gestiones": [], "meses": []}
        finally:
            con.close()

    def get_datamarts_summary(self) -> list[dict[str, Any]]:
        """Retorna estado físico en vivo de los 4 repositorios consolidados Gold."""
        summaries: list[dict[str, Any]] = []
        con = self._connect()
        try:
            for dm_key, cfg in DATAMART_CATALOG.items():
                mart_dir = self._gold_path / dm_key
                pq_files = list(mart_dir.glob("**/*.parquet")) if mart_dir.exists() else []
                size_bytes = sum(f.stat().st_size for f in pq_files) if pq_files else 0
                row_count = 0
                partitions_count = 0
                inst_count = 0
                min_period = None
                max_period = None

                if pq_files:
                    glob_path = (mart_dir / "**/*.parquet").resolve().as_posix()
                    try:
                        agg = con.execute(
                            f"""
                            SELECT
                                COUNT(*) AS rows_cnt,
                                COUNT(DISTINCT (CAST(gestion AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0'))) AS part_cnt,
                                COUNT(DISTINCT institucion_id) AS inst_cnt,
                                MIN(CAST(gestion AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0')) AS min_p,
                                MAX(CAST(gestion AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0')) AS max_p
                            FROM read_parquet('{glob_path}')
                            """
                        ).fetchone()
                        if agg:
                            row_count = int(agg[0] or 0)
                            partitions_count = int(agg[1] or 0)
                            inst_count = int(agg[2] or 0)
                            min_period = agg[3]
                            max_period = agg[4]
                    except Exception:
                        pass

                summaries.append(
                    {
                        "datamart": dm_key,
                        "title": cfg["title"],
                        "source_code": cfg["source_code"],
                        "description": cfg["description"],
                        "is_materialized": row_count > 0,
                        "total_rows": row_count,
                        "partitions_count": partitions_count,
                        "institutions_count": inst_count,
                        "size_bytes": size_bytes,
                        "size_kb": round(size_bytes / 1024, 2),
                        "earliest_period": min_period,
                        "latest_period": max_period,
                        "available_columns": cfg["allowed_columns"],
                        "default_columns": cfg["default_columns"],
                    }
                )
        finally:
            con.close()
        return summaries

    def execute_query(
        self,
        request: AnalyticsQueryRequest,
    ) -> AnalyticsQueryResponse:
        t_start = time.perf_counter()
        dm_key = request.datamart.strip().lower()
        glob_path = self._resolve_parquet_glob(dm_key)

        if not glob_path:
            return AnalyticsQueryResponse(
                datamart=request.datamart,
                total_records=0,
                execution_time_ms=0.0,
                columns=[],
                records=[],
            )

        dm_cfg = DATAMART_CATALOG.get(dm_key, DATAMART_CATALOG["mart_cartera"])
        dm_allowed = set(dm_cfg["allowed_columns"])
        default_cols = list(dm_cfg["default_columns"])

        if request.metrics:
            selected_cols = [
                c for c in request.metrics if c.lower() in dm_allowed
            ]
        else:
            selected_cols = default_cols

        if not selected_cols:
            selected_cols = default_cols

        cols_sql = ", ".join(selected_cols)

        where_clauses = ["1=1"]
        params: list[Any] = []

        if request.institutions:
            placeholders = ", ".join(["?"] * len(request.institutions))
            where_clauses.append(f"institucion_id IN ({placeholders})")
            params.extend(request.institutions)

        if request.gestiones:
            int_gestiones = [int(g) for g in request.gestiones]
            placeholders = ", ".join(["?"] * len(int_gestiones))
            where_clauses.append(f"gestion IN ({placeholders})")
            params.extend(int_gestiones)

        if request.meses:
            int_meses = [int(m) for m in request.meses]
            placeholders = ", ".join(["?"] * len(int_meses))
            where_clauses.append(f"mes IN ({placeholders})")
            params.extend(int_meses)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT {cols_sql}
            FROM read_parquet('{glob_path}')
            WHERE {where_sql}
            ORDER BY gestion DESC, mes DESC, institucion_id ASC
        """

        con = self._connect()
        try:
            cursor = con.execute(sql, params)
            raw_cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()

            records: list[dict[str, Any]] = []
            for row in rows:
                record_dict = {}
                for col_name, val in zip(raw_cols, row):
                    if isinstance(val, float) and (
                        math.isnan(val) or math.isinf(val)
                    ):
                        record_dict[col_name] = None
                    else:
                        record_dict[col_name] = val
                records.append(record_dict)
        finally:
            con.close()

        t_end = time.perf_counter()
        duration_ms = round((t_end - t_start) * 1000, 2)

        return AnalyticsQueryResponse(
            datamart=request.datamart,
            total_records=len(records),
            execution_time_ms=duration_ms,
            columns=raw_cols,
            records=records,
        )


class DuckDBGoldPartitionAdapter(GoldPartitionPort):
    """Adaptador de inspección de particiones Parquet en Capa Gold."""

    def __init__(self, base_gold_path: Path | None = None) -> None:
        self._analytics = DuckDBAnalyticsAdapter(base_gold_path)
        self._gold_path = base_gold_path or Path("data/gold")

    def list_available_partitions(
        self,
        datamart: str = "mart_cartera",
    ) -> list[tuple[int, int]]:
        mart_dir = self._gold_path / datamart
        if not mart_dir.exists():
            return []
        partitions: set[tuple[int, int]] = set()
        for p in mart_dir.glob("gestion=*/mes=*/*.parquet"):
            try:
                g_part = [pt for pt in p.parts if pt.startswith("gestion=")][0]
                m_part = [pt for pt in p.parts if pt.startswith("mes=")][0]
                g = int(g_part.split("=")[1])
                m = int(m_part.split("=")[1])
                partitions.add((g, m))
            except Exception:
                continue
        return sorted(list(partitions))

    def has_partition(
        self,
        datamart: str,
        gestion: int,
        mes: int,
    ) -> bool:
        part_dir = (
            self._gold_path / datamart / f"gestion={gestion}" / f"mes={mes}"
        )
        if part_dir.exists() and any(part_dir.glob("*.parquet")):
            return True
        return self._analytics.has_data(
            datamart=datamart,
            gestion=gestion,
            mes=mes,
        )
