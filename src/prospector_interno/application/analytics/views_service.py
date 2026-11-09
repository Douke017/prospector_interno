"""Servicio de ejecución y catálogo de Vistas Preestablecidas de Autoservicio."""

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

MULTI_SOURCE_VIEWS_CATALOG: list[PredefinedViewMetadata] = [
    PredefinedViewMetadata(
        view_id="series-activas-por-fuente",
        title="Series Documentales Vigentes y Volumen de Descargas (D_BO_* / A_*)",
        description=(
            "Inventario priorizado de las series de archivos fuente manuales y "
            "automatizadas de las 52+ fuentes ordenadas por volumen de descargas."
        ),
        category="CATALOGO_SERIES",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "codigo_serie",
            "nombre_serie",
            "tipo_serie",
            "formato_archivo",
            "frecuencia_publicacion",
            "actualizado_hasta",
            "total_descargas_bruto",
            "total_reportes_derivados",
        ],
    ),
    PredefinedViewMetadata(
        view_id="ultimas-descargas-en-bruto",
        title="Últimos Archivos Descargados en Bruto con Huella SHA-256",
        description=(
            "Trazabilidad cronológica de los archivos descargados en bruto desde "
            "los portales institucionales y DAGs de Airflow hacia el servidor."
        ),
        category="DESCARGAS_BRUTO",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
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
            "source_file_sha256",
        ],
    ),
    PredefinedViewMetadata(
        view_id="reportes-conversiones-y-tablas",
        title="Reportes Tabulares Extraídos, Tablas Destino y Alertas de Estructura (D_BO_*_xx)",
        description=(
            "Relación de reportes extraídos desde archivos en bruto, cantidad de "
            "conversiones históricas, incidencias estructurales y bases que alimentan."
        ),
        category="CONVERSIONES",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "codigo_reporte",
            "codigo_serie_madre",
            "nombre_reporte",
            "tabla_almacenamiento",
            "convertido_hasta",
            "migrado_hasta",
            "total_conversiones",
            "incidencias_estructura",
            "bases_alimentadas",
        ],
    ),
    PredefinedViewMetadata(
        view_id="bases-consolidadas-y-cubos-spim",
        title="Bases de Datos Consolidadas (DB_BO_* / S_BO*) y Estado de Cubos",
        description=(
            "Catálogo operativo de las 694 bases de datos consolidadas y cubos SPIM, "
            "sus reportes insumo, fecha de último corte y total de actualizaciones."
        ),
        category="BASES_DATOS",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
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
        ],
    ),
    PredefinedViewMetadata(
        view_id="cobertura-mapa-52-fuentes",
        title="Cobertura Unificada de Mapas de las 52 Fuentes (Externo ↔ Interno)",
        description=(
            "Cruce directo entre los enlaces descubiertos por el Prospector Externo "
            "en los 52 portales y su estado en custodia documental y bases internas."
        ),
        category="MAPA_52_FUENTES",
        supported_filters=["gestion", "mes", "institutions", "limit"],
        output_columns=[
            "institucion_id",
            "institucion_nombre",
            "codigo_recurso",
            "titulo_documento",
            "url_canonica",
            "en_custodia_bruto",
            "consolidado_en_base",
            "estado_inventario",
            "actualizado_en",
        ],
    ),
]

EXTENDED_VIEWS_CATALOG: list[PredefinedViewMetadata] = [
    *MULTI_SOURCE_VIEWS_CATALOG,
    *VIEWS_CATALOG,
]

VIEW_TO_DATAMART: dict[str, str] = {
    "series-activas-por-fuente": "catalogo_series_files",
    "ultimas-descargas-en-bruto": "historial_descargas_bruto",
    "reportes-conversiones-y-tablas": "reportes_conversiones",
    "bases-consolidadas-y-cubos-spim": "bases_datos_cubos",
    "cobertura-mapa-52-fuentes": "mapa_web_52_fuentes",
    "evolucion-mora-temporal": "mart_cartera",
    "ranking-solvencia-cobertura": "mart_cartera",
    "comparativo-entidades-resumen": "mart_cartera",
}


class PredefinedViewsService:
    """Motor de consulta y orquestación de vistas predefinidas de autoservicio."""

    def __init__(self, base_gold_path: Path | None = None) -> None:
        self._gold_path = base_gold_path or Path("data/gold")
        self._views_map = {v.view_id: v for v in EXTENDED_VIEWS_CATALOG}

    def list_views(
        self,
        datamart: str | None = None,
    ) -> list[PredefinedViewMetadata]:
        """Retorna vistas predefinidas filtradas por repositorio o todas las vistas."""
        if datamart is None:
            return list(VIEWS_CATALOG)
        dm = datamart.strip().lower()
        if dm == "all":
            return list(MULTI_SOURCE_VIEWS_CATALOG)
        return [
            v
            for v in EXTENDED_VIEWS_CATALOG
            if VIEW_TO_DATAMART.get(v.view_id) == dm
        ]

    def get_view(self, view_id: str) -> PredefinedViewMetadata | None:
        """Obtiene la metadata de una vista predefinida por su identificador."""
        return self._views_map.get(view_id.strip().lower())

    def _resolve_parquet_glob(self, datamart: str = "catalogo_series_files") -> str | None:
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
        """Ejecuta una vista predefinida aplicando filtros opcionales de forma segura."""
        t_start = time.perf_counter()
        clean_id = view_id.strip().lower()
        view_meta = self.get_view(clean_id)
        if not view_meta:
            raise ViewNotFoundError(
                f"La vista analítica '{view_id}' no existe en el catálogo."
            )

        req = request or PredefinedViewQueryRequest()
        target_dm = VIEW_TO_DATAMART.get(clean_id, "catalogo_series_files")
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
            upper_insts = [str(i).strip().upper() for i in req.institutions if str(i).strip()]
            if upper_insts:
                ph = ", ".join(["?"] * len(upper_insts))
                where_clauses.append(f"UPPER(institucion_id) IN ({ph})")
                params.extend(upper_insts)

        where_sql = " AND ".join(where_clauses)
        cols_sql = ", ".join(view_meta.output_columns)

        if clean_id == "series-activas-por-fuente":
            order_sql = "ORDER BY total_descargas_bruto DESC, institucion_id ASC"
        elif clean_id == "ultimas-descargas-en-bruto":
            order_sql = "ORDER BY gestion DESC, mes DESC, id_descarga DESC"
        elif clean_id == "reportes-conversiones-y-tablas":
            order_sql = "ORDER BY total_conversiones DESC, codigo_reporte ASC"
        elif clean_id == "bases-consolidadas-y-cubos-spim":
            order_sql = "ORDER BY total_actualizaciones DESC, codigo_base ASC"
        elif clean_id == "cobertura-mapa-52-fuentes":
            order_sql = "ORDER BY institucion_id ASC, codigo_recurso ASC"
        elif clean_id == "evolucion-mora-temporal":
            order_sql = "ORDER BY gestion ASC, mes ASC, institucion_id ASC"
        elif clean_id == "ranking-solvencia-cobertura":
            order_sql = "ORDER BY ratio_cobertura DESC, total_patrimonio DESC"
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
