"""Adaptador DuckDB para consultas vectorizadas sobre la Capa Gold (HUI-14)."""

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


ALLOWED_COLUMNS: set[str] = {
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
}


class DuckDBAnalyticsAdapter:
    """Motor de consulta SQL vectorizado sobre Datamarts en Capa Gold."""

    def __init__(
        self,
        base_gold_path: Path | None = None,
        max_memory: str = "4GB",
    ) -> None:
        self._gold_path = base_gold_path or Path("data/gold")
        self._max_memory = max_memory

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Crea una conexión in-memory aplicando el presupuesto de RAM (HUI-23)."""
        con = duckdb.connect(":memory:")
        con.execute(f"SET max_memory = '{self._max_memory}'")
        return con

    def get_max_memory_setting(self) -> str:
        """Consulta el valor configurado de max_memory en DuckDB."""
        con = self._connect()
        try:
            return con.execute(
                "SELECT current_setting('max_memory')"
            ).fetchone()[0]
        finally:
            con.close()

    def _resolve_parquet_glob(self, datamart: str) -> str | None:
        """Resuelve el patrón glob de archivos Parquet para el datamart."""
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
        """Verifica si existen particiones de datos en Capa Gold."""
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
        """Retorna las dimensiones disponibles (instituciones, gestiones, meses)."""
        glob_path = self._resolve_parquet_glob(datamart)
        if not glob_path:
            return {"instituciones": [], "gestiones": [], "meses": []}

        con = self._connect()
        try:
            sql_inst = (
                f"SELECT DISTINCT institucion_id "
                f"FROM read_parquet('{glob_path}') ORDER BY 1"
            )
            insts = [r[0] for r in con.execute(sql_inst).fetchall()]

            sql_gest = (
                f"SELECT DISTINCT gestion "
                f"FROM read_parquet('{glob_path}') ORDER BY 1"
            )
            gestiones = [r[0] for r in con.execute(sql_gest).fetchall()]

            sql_mes = (
                f"SELECT DISTINCT mes "
                f"FROM read_parquet('{glob_path}') ORDER BY 1"
            )
            meses = [r[0] for r in con.execute(sql_mes).fetchall()]
            return {
                "instituciones": insts,
                "gestiones": gestiones,
                "meses": meses,
            }
        except Exception:
            return {"instituciones": [], "gestiones": [], "meses": []}
        finally:
            con.close()

    def execute_query(
        self,
        request: AnalyticsQueryRequest,
    ) -> AnalyticsQueryResponse:
        """Ejecuta una consulta analítica parametrizada sobre la Capa Gold."""
        t_start = time.perf_counter()
        glob_path = self._resolve_parquet_glob(request.datamart)

        if not glob_path:
            return AnalyticsQueryResponse(
                datamart=request.datamart,
                total_records=0,
                execution_time_ms=0.0,
                columns=[],
                records=[],
            )

        # Selección segura de métricas
        if request.metrics:
            selected_cols = [
                c for c in request.metrics if c.lower() in ALLOWED_COLUMNS
            ]
        else:
            selected_cols = [
                "gestion",
                "mes",
                "institucion_id",
                "cartera_bruta",
                "cartera_vigente",
                "cartera_en_mora",
                "ratio_mora",
                "total_activo",
            ]

        if not selected_cols:
            selected_cols = ["gestion", "mes", "institucion_id", "cartera_bruta"]

        cols_sql = ", ".join(selected_cols)

        # Construcción dinámica de filtros WHERE
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
    """Adaptador de inspección de particiones Parquet en Capa Gold (HUI-02)."""

    def __init__(self, base_gold_path: Path | None = None) -> None:
        self._analytics = DuckDBAnalyticsAdapter(base_gold_path)
        self._gold_path = base_gold_path or Path("data/gold")

    def list_available_partitions(
        self,
        datamart: str = "mart_cartera",
    ) -> list[tuple[int, int]]:
        """Retorna tuplas (gestion, mes) existentes en el datamart Parquet."""
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
        """Verifica si una partición específica (gestion, mes) existe en Gold."""
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
