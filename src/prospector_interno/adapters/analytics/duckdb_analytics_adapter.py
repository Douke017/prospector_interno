"""Adaptador de consulta sobre los 5 Repositorios Consolidados de las 52+ Fuentes DATAX."""

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


PRIMARY_DATAMART_CATALOG: dict[str, dict[str, Any]] = {
    "catalogo_series_files": {
        "datamart": "catalogo_series_files",
        "title": "Catálogo Maestro de Series y Archivos Fuente (D_BO_* / A_*)",
        "source_code": "52+ FUENTES",
        "description": (
            "Inventario unificado de las 1.232 series documentales (586 series manuales "
            "D_BO_* y 646 series automatizadas A_*) de todas las fuentes oficiales, con "
            "frecuencia, ruta en servidor, descargas en bruto y reportes asociados."
        ),
        "default_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_serie",
            "nombre_serie",
            "tipo_serie",
            "formato_archivo",
            "frecuencia_publicacion",
            "estado_serie",
            "actualizado_hasta",
            "total_descargas_bruto",
            "total_reportes_derivados",
            "url_principal",
        ],
        "allowed_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_serie",
            "nombre_serie",
            "tipo_serie",
            "formato_archivo",
            "frecuencia_publicacion",
            "estado_serie",
            "actualizado_hasta",
            "url_principal",
            "ruta_servidor_spim",
            "total_descargas_bruto",
            "total_reportes_derivados",
            "gestion",
            "mes",
            "source_file_sha256",
        ],
    },
    "historial_descargas_bruto": {
        "datamart": "historial_descargas_bruto",
        "title": "Repositorio de Archivos Descargados en Bruto (Download & DAGs)",
        "source_code": "52+ FUENTES",
        "description": (
            "Registro consolidado de los archivos descargados en bruto desde los portales "
            "web e integraciones automatizadas (Airflow DAGs), preservando ruta física en "
            "servidor, URL de origen, formato y firma criptográfica."
        ),
        "default_columns": [
            "institucion_id",
            "institucion_nombre",
            "id_descarga",
            "codigo_serie",
            "nombre_serie",
            "fecha_datos",
            "fecha_descarga",
            "estado_descarga",
            "formato_archivo",
            "ruta_archivo_bruto",
            "url_descarga",
        ],
        "allowed_columns": [
            "institucion_id",
            "institucion_nombre",
            "id_descarga",
            "codigo_serie",
            "nombre_serie",
            "fecha_datos",
            "fecha_descarga",
            "estado_descarga",
            "formato_archivo",
            "ruta_archivo_bruto",
            "url_descarga",
            "gestion",
            "mes",
            "source_file_sha256",
        ],
    },
    "reportes_conversiones": {
        "datamart": "reportes_conversiones",
        "title": "Repositorio de Reportes Extraídos y Conversiones (D_BO_*_xx)",
        "source_code": "52+ FUENTES",
        "description": (
            "Consolidado de los 346 reportes tabulares extraídos desde los archivos en bruto, "
            "historial de 20.802 conversiones, diagnósticos de cambios de estructura, "
            "tablas destino de almacenamiento y bases de datos alimentadas."
        ),
        "default_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_reporte",
            "codigo_serie_madre",
            "nombre_reporte",
            "frecuencia_reporte",
            "extension_salida",
            "tabla_almacenamiento",
            "convertido_hasta",
            "migrado_hasta",
            "total_conversiones",
            "incidencias_estructura",
            "bases_alimentadas",
        ],
        "allowed_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_reporte",
            "codigo_serie_madre",
            "nombre_reporte",
            "frecuencia_reporte",
            "extension_salida",
            "tabla_almacenamiento",
            "ruta_conversion",
            "convertido_hasta",
            "migrado_hasta",
            "total_conversiones",
            "incidencias_estructura",
            "bases_alimentadas",
            "gestion",
            "mes",
            "source_file_sha256",
        ],
    },
    "bases_datos_cubos": {
        "datamart": "bases_datos_cubos",
        "title": "Repositorio de Bases de Datos Consolidadas (DB_BO_* / S_BO*) y Cubos",
        "source_code": "52+ FUENTES",
        "description": (
            "Inventario consolidado de las 694 bases de datos estructuradas, 820 vínculos "
            "con reportes insumo, 692 cubos SPIM y 63.923 actualizaciones históricas "
            "disponibles para consulta directa y autoservicio."
        ),
        "default_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_base",
            "nombre_base",
            "frecuencia_datos",
            "datos_desde",
            "actualizado_hasta",
            "proxima_actualizacion",
            "estado_cubo",
            "reportes_insumo",
            "total_actualizaciones",
            "ultima_url_origen",
        ],
        "allowed_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_base",
            "nombre_base",
            "frecuencia_datos",
            "frecuencia_actualizacion",
            "datos_desde",
            "actualizado_hasta",
            "proxima_actualizacion",
            "estado_cubo",
            "reportes_insumo",
            "total_actualizaciones",
            "ultima_url_origen",
            "gestion",
            "mes",
            "source_file_sha256",
        ],
    },
    "mapa_web_52_fuentes": {
        "datamart": "mapa_web_52_fuentes",
        "title": "Mapa Unificado de las 52 Fuentes (Prospector Externo ↔ Interno)",
        "source_code": "52 PORTALES",
        "description": (
            "Cruce completo en tiempo real entre todos los mapas y recursos descubiertos "
            "por el Prospector Externo en los 52 portales y su estado de descarga en bruto "
            "y consolidación en el servidor interno."
        ),
        "default_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_recurso",
            "titulo_documento",
            "url_canonica",
            "en_custodia_bruto",
            "consolidado_en_base",
            "estado_inventario",
            "ruta_almacenamiento",
            "actualizado_en",
        ],
        "allowed_columns": [
            "institucion_id",
            "institucion_nombre",
            "codigo_recurso",
            "titulo_documento",
            "url_canonica",
            "en_custodia_bruto",
            "consolidado_en_base",
            "estado_inventario",
            "ruta_almacenamiento",
            "actualizado_en",
            "gestion",
            "mes",
            "source_file_sha256",
        ],
    },
}

LEGACY_DATAMART_CATALOG: dict[str, dict[str, Any]] = {
    "mart_cartera": {
        "datamart": "mart_cartera",
        "title": "Repositorio de Cartera (Compatibilidad Suite Unitaria)",
        "source_code": "FINRURAL",
        "description": "Esquema dimensional para pruebas unitarias aisladas.",
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
        "title": "Repositorio BCB (Compatibilidad)",
        "source_code": "BCB",
        "description": "Esquema de compatibilidad.",
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
        "title": "Repositorio BBV (Compatibilidad)",
        "source_code": "BBV",
        "description": "Esquema de compatibilidad.",
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
        "title": "Repositorio ASFI (Compatibilidad)",
        "source_code": "ASFI",
        "description": "Esquema de compatibilidad.",
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

DATAMART_CATALOG: dict[str, dict[str, Any]] = {
    **PRIMARY_DATAMART_CATALOG,
    **LEGACY_DATAMART_CATALOG,
}

ALLOWED_COLUMNS: set[str] = {
    col
    for cfg in DATAMART_CATALOG.values()
    for col in cfg["allowed_columns"]
}


class DuckDBAnalyticsAdapter:
    """Motor de consulta vectorizada sobre los 5 repositorios multi-fuente de DATAX."""

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
        datamart: str = "catalogo_series_files",
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
        datamart: str = "catalogo_series_files",
    ) -> dict[str, list[Any]]:
        glob_path = self._resolve_parquet_glob(datamart)
        if not glob_path:
            return {"instituciones": [], "institutions": [], "gestiones": [], "meses": []}

        con = self._connect()
        try:
            insts = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT institucion_id FROM read_parquet('{glob_path}') WHERE institucion_id IS NOT NULL ORDER BY 1"
                ).fetchall()
            ]
            gestiones = [
                int(r[0])
                for r in con.execute(
                    f"SELECT DISTINCT gestion FROM read_parquet('{glob_path}') WHERE gestion IS NOT NULL ORDER BY 1 DESC"
                ).fetchall()
            ]
            meses = [
                int(r[0])
                for r in con.execute(
                    f"SELECT DISTINCT mes FROM read_parquet('{glob_path}') WHERE mes IS NOT NULL ORDER BY 1 ASC"
                ).fetchall()
            ]
            return {
                "instituciones": insts,
                "institutions": insts,
                "gestiones": gestiones,
                "meses": meses,
            }
        except Exception:
            return {"instituciones": [], "institutions": [], "gestiones": [], "meses": []}
        finally:
            con.close()

    def get_datamarts_summary(self) -> list[dict[str, Any]]:
        """Retorna estado físico en vivo de los 5 repositorios multi-fuente de DATAX."""
        summaries: list[dict[str, Any]] = []
        con = self._connect()
        try:
            for dm_key, cfg in PRIMARY_DATAMART_CATALOG.items():
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
                        "min_period": min_period,
                        "max_period": max_period,
                        "earliest_period": min_period,
                        "latest_period": max_period,
                        "columns": cfg["default_columns"],
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

        dm_cfg = DATAMART_CATALOG.get(dm_key, PRIMARY_DATAMART_CATALOG["catalogo_series_files"])
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
            upper_insts = [str(i).strip().upper() for i in request.institutions if str(i).strip()]
            if upper_insts:
                placeholders = ", ".join(["?"] * len(upper_insts))
                where_clauses.append(f"UPPER(institucion_id) IN ({placeholders})")
                params.extend(upper_insts)

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

        search_term = (request.search or "").strip()
        if search_term:
            like_pattern = f"%{search_term}%"
            search_parts = [f"CAST({col} AS VARCHAR) ILIKE ?" for col in selected_cols]
            where_clauses.append(f"({' OR '.join(search_parts)})")
            params.extend([like_pattern] * len(selected_cols))

        where_sql = " AND ".join(where_clauses)
        count_sql = f"SELECT COUNT(*) FROM read_parquet('{glob_path}') WHERE {where_sql}"

        page_clause = ""
        query_params = list(params)
        if request.limit is not None:
            page_clause = " LIMIT ? OFFSET ?"
            query_params.extend([int(request.limit), int(request.offset or 0)])

        sql = f"""
            SELECT {cols_sql}
            FROM read_parquet('{glob_path}')
            WHERE {where_sql}
            ORDER BY gestion DESC, mes DESC, institucion_id ASC
            {page_clause}
        """

        con = self._connect()
        try:
            total_row = con.execute(count_sql, params).fetchone()
            total_matching = int(total_row[0]) if total_row else 0

            cursor = con.execute(sql, query_params)
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
            total_records=total_matching,
            execution_time_ms=duration_ms,
            columns=raw_cols,
            records=records,
        )


class DuckDBGoldPartitionAdapter(GoldPartitionPort):
    """Adaptador de inspección de particiones Parquet en Repositorios Consolidados."""

    def __init__(self, base_gold_path: Path | None = None) -> None:
        self._analytics = DuckDBAnalyticsAdapter(base_gold_path)
        self._gold_path = base_gold_path or Path("data/gold")

    def list_available_partitions(
        self,
        datamart: str = "catalogo_series_files",
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
