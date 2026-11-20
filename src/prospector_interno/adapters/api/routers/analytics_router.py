"""Router para Autoservicio Analítico, Exportación Excel y Conectores en Vivo."""

from __future__ import annotations

import base64
import csv
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
from typing import Any, Literal

import duckdb
from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo
from pydantic import BaseModel, Field

from prospector_interno.adapters.analytics.duckdb_analytics_adapter import (
    DATAMART_CATALOG,
)
from prospector_interno.adapters.analytics.readonly_sql_engine import (
    ReadOnlyDuckDbSqlEngine,
    UnsafeSqlError,
)
from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.routers.catalog_router import (
    get_datamart_lineage,
)
from prospector_interno.adapters.resilience.bulkhead_manager import (
    get_bulkhead_manager,
)
from prospector_interno.application.analytics.views_service import (
    ViewNotFoundError,
)
from prospector_interno.domain.contracts.analytics_spec import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from prospector_interno.domain.contracts.views_spec import (
    PredefinedViewMetadata,
    PredefinedViewQueryRequest,
    PredefinedViewResult,
)

router = APIRouter(prefix="/analytics", tags=["6. Autoservicio Analítico"])

_analytics_adapter = deps.analytics_adapter
_bronze_storage = deps.bronze_storage
_gold_materializer = deps.gold_materializer
_views_service = deps.views_service
_readonly_sql_engine = deps.readonly_sql_engine


def set_readonly_sql_engine(engine: ReadOnlyDuckDbSqlEngine) -> None:
    """Permite inyectar un motor SQL read-only aislado en pruebas."""
    global _readonly_sql_engine
    _readonly_sql_engine = engine


class CreateApiKeyRequest(BaseModel):
    """Solicitud para emitir una llave API con alcance por capa/dataset y expiración."""

    label: str = Field(
        ...,
        min_length=2,
        max_length=120,
        examples=["Notebook Ciencia de Datos Polars"],
    )
    allowed_datamarts: str = Field(
        default="*",
        description="'*' para todos los repositorios o lista separada por comas.",
    )
    allowed_layers: list[str] = Field(
        default_factory=lambda: ["RAW", "GOLD", "SQL"],
        description="Capas autorizadas: RAW, GOLD, SQL (o '*')",
    )
    expires_at: datetime | None = Field(
        default=None,
        description="Fecha/hora UTC exacta de expiración opcional.",
    )
    expires_in_days: int | None = Field(
        default=None,
        ge=1,
        le=3650,
        description="Alternativa a expires_at: vigencia en días desde hoy.",
    )


class ReadOnlySqlRequest(BaseModel):
    """Solicitud para ejecutar una consulta SQL SELECT de solo lectura sobre DuckDB efímero."""

    sql: str = Field(..., min_length=6, description="Consulta SELECT o WITH de solo lectura")
    limit: int = Field(default=500, ge=1, le=10000)
    format: Literal["json", "parquet", "csv", "jsonl"] = Field(default="json")


def _execute_export_dataset(
    datamart: str,
    gestion: int | None = None,
    mes: int | None = None,
    institution_id: str | None = None,
) -> AnalyticsQueryResponse:
    dm_clean = datamart.strip().lower()
    if dm_clean not in DATAMART_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"El repositorio '{datamart}' no existe.",
        )
    all_cols = list(DATAMART_CATALOG[dm_clean]["allowed_columns"])
    req = AnalyticsQueryRequest(
        datamart=dm_clean,
        institutions=[institution_id] if institution_id else None,
        gestiones=[gestion] if gestion is not None else None,
        meses=[mes] if mes is not None else None,
        metrics=all_cols,
    )
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_analytics(_analytics_adapter.execute_query, req)


def _build_excel_workbook_bytes(
    datamart: str,
    query_res: AnalyticsQueryResponse,
    dictionary_cols: list[dict[str, Any]],
    lineage_rows: list[dict[str, Any]],
) -> bytes:
    """Construye en memoria un libro Excel (.xlsx) profesional de 3 hojas."""
    wb = openpyxl.Workbook()

    header_fill = PatternFill(start_color="0F172A", end_color="0F172A", fill_type="solid")
    header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Hoja 1: Datos_Consolidados
    ws_data = wb.active
    ws_data.title = "Datos_Consolidados"

    col_label_map = {
        c["column_name"]: c["business_label"] for c in dictionary_cols
    }
    col_unit_map = {
        c["column_name"]: str(c.get("unit") or "") for c in dictionary_cols
    }

    columns = query_res.columns or list(DATAMART_CATALOG[datamart]["allowed_columns"])
    headers = [col_label_map.get(c, c) for c in columns]
    ws_data.append(headers)

    for idx in range(1, len(headers) + 1):
        cell = ws_data.cell(row=1, column=idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_align

    pct_cols = {
        idx + 1
        for idx, c in enumerate(columns)
        if col_unit_map.get(c) == "%" or c.endswith("_pct") or c.startswith("ratio_") or "solvencia" in c or "peso_" in c
    }
    money_cols = {
        idx + 1
        for idx, c in enumerate(columns)
        if col_unit_map.get(c) in {"Bs.", "USD"} or c.endswith(("_bs", "_usd")) or c.startswith(("cartera_", "total_", "prevision_"))
    }

    if query_res.records:
        for rec in query_res.records:
            row_vals = [rec.get(c) for c in columns]
            ws_data.append(row_vals)
        max_row = len(query_res.records) + 1
        max_col_letter = get_column_letter(len(columns))
        tab = Table(
            displayName=f"Tabla_{datamart}",
            ref=f"A1:{max_col_letter}{max_row}",
        )
        tab.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium9",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        ws_data.add_table(tab)

        for r_idx in range(2, max_row + 1):
            for c_idx in pct_cols:
                ws_data.cell(row=r_idx, column=c_idx).number_format = "0.00%"
            for c_idx in money_cols:
                ws_data.cell(row=r_idx, column=c_idx).number_format = "#,##0.00"

    ws_data.freeze_panes = "A2"
    for idx, h_text in enumerate(headers, start=1):
        ws_data.column_dimensions[get_column_letter(idx)].width = max(16, min(34, len(h_text) + 6))

    # Hoja 2: Diccionario_Variables
    ws_dict = wb.create_sheet(title="Diccionario_Variables")
    dict_headers = [
        "Columna Técnica",
        "Denominación de Negocio",
        "Tipo de Dato",
        "Unidad",
        "Fórmula / Regla de Cálculo",
        "Descripción Funcional",
    ]
    ws_dict.append(dict_headers)
    for idx in range(1, len(dict_headers) + 1):
        c = ws_dict.cell(row=1, column=idx)
        c.fill = header_fill
        c.font = header_font
        c.alignment = header_align

    for col_def in dictionary_cols:
        ws_dict.append(
            [
                col_def.get("column_name"),
                col_def.get("business_label"),
                col_def.get("data_type"),
                col_def.get("unit"),
                col_def.get("formula") or "Extracción directa normalizada",
                col_def.get("description"),
            ]
        )
    ws_dict.freeze_panes = "A2"
    for idx, width in enumerate([26, 34, 16, 16, 40, 60], start=1):
        ws_dict.column_dimensions[get_column_letter(idx)].width = width

    # Hoja 3: Linaje_Documental
    ws_lin = wb.create_sheet(title="Linaje_Documental")
    lin_headers = [
        "Activo Consolidado",
        "Periodo",
        "Registros",
        "Huella SHA-256 Capa Gold",
        "Huella SHA-256 Archivo Fuente (Bronze)",
        "Archivo Custodiado",
        "Ejecución Origen",
        "URL Institucional",
    ]
    ws_lin.append(lin_headers)
    for idx in range(1, len(lin_headers) + 1):
        c = ws_lin.cell(row=1, column=idx)
        c.fill = header_fill
        c.font = header_font
        c.alignment = header_align

    for l_row in lineage_rows:
        ws_lin.append(
            [
                l_row.get("asset_id"),
                l_row.get("period_start"),
                l_row.get("row_count"),
                l_row.get("gold_sha256"),
                l_row.get("bronze_file_sha256"),
                l_row.get("bronze_source_file"),
                l_row.get("external_run_id"),
                l_row.get("canonical_url") or "Custodia local verificada",
            ]
        )
    ws_lin.freeze_panes = "A2"
    for idx, width in enumerate([30, 14, 12, 36, 36, 38, 26, 48], start=1):
        ws_lin.column_dimensions[get_column_letter(idx)].width = width

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def _build_csv_bytes(query_res: AnalyticsQueryResponse) -> bytes:
    sio = io.StringIO()
    writer = csv.writer(sio)
    if query_res.columns:
        writer.writerow(query_res.columns)
        for rec in query_res.records:
            writer.writerow([rec.get(c) for c in query_res.columns])
    return ("\ufeff" + sio.getvalue()).encode("utf-8")


@router.get(
    "/datamarts",
    summary="Listar estado y métricas en vivo de los 5 repositorios consolidados Gold",
)
def list_consolidated_datamarts() -> list[dict[str, Any]]:
    """Retorna el inventario operativo de los 5 repositorios consolidados multi-fuente."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_analytics(_analytics_adapter.get_datamarts_summary)


@router.post(
    "/query",
    response_model=AnalyticsQueryResponse,
    summary="Consultar repositorio consolidado en Capa Gold",
)
def query_gold_mart(request: AnalyticsQueryRequest) -> AnalyticsQueryResponse:
    """Ejecuta consulta analítica aislada en pool de trabajo."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_analytics(_analytics_adapter.execute_query, request)


@router.get(
    "/dimensions",
    summary="Listar dimensiones disponibles en un repositorio consolidado",
)
def get_gold_dimensions(
    datamart: str = "catalogo_series_files",
) -> dict[str, list[Any]]:
    """Consulta las fuentes, gestiones y meses consolidados."""
    bulkhead = get_bulkhead_manager()
    return bulkhead.execute_analytics(_analytics_adapter.get_dimensions, datamart)


@router.get(
    "/views",
    response_model=list[PredefinedViewMetadata],
    summary="Listar catálogo de vistas analíticas predefinidas",
)
def list_predefined_views(
    datamart: str | None = Query(
        default=None,
        description="Filtro opcional por repositorio o 'all' para todas las vistas.",
    ),
) -> list[PredefinedViewMetadata]:
    """Retorna las vistas analíticas disponibles."""
    return _views_service.list_views(datamart=datamart)


@router.get(
    "/views/{view_id}",
    response_model=PredefinedViewMetadata,
    summary="Consultar metadatos de una vista analítica específica",
)
def get_predefined_view(view_id: str) -> PredefinedViewMetadata:
    """Consulta los metadatos de una vista precalculada."""
    view_meta = _views_service.get_view(view_id)
    if not view_meta:
        raise HTTPException(
            status_code=404,
            detail=f"La vista analítica '{view_id}' no existe en el catálogo.",
        )
    return view_meta


@router.post(
    "/views/{view_id}/execute",
    response_model=PredefinedViewResult,
    summary="Ejecutar vista analítica precalculada",
)
def execute_predefined_view(
    view_id: str,
    request: PredefinedViewQueryRequest | None = None,
) -> PredefinedViewResult:
    """Ejecuta la vista analítica solicitada."""
    try:
        bulkhead = get_bulkhead_manager()
        return bulkhead.execute_analytics(
            _views_service.execute_view, view_id, request=request
        )
    except ViewNotFoundError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err


@router.post(
    "/materialize",
    summary="Consolidar Repositorios Gold desde Capa Bronze",
)
def materialize_gold_from_bronze(
    datamart: str | None = Query(
        default=None,
        description="Opcional: consolidar un repositorio específico o todos si se omite.",
    ),
) -> dict[str, Any]:
    """Ejecuta la normalización y consolidación atómica hacia los 4 repositorios Gold."""
    artifacts = _bronze_storage.get_artifacts()
    return _gold_materializer.consolidate_all_datamarts(
        artifacts,
        target_datamart=datamart,
        url_inventory_repo=deps.url_inventory_repo,
    )


@router.get(
    "/export/xlsx",
    summary="Descargar libro Excel (.xlsx) con datos consolidados, diccionario y linaje",
)
def export_datamart_xlsx(
    request: Request,
    datamart: str = Query(default="catalogo_series_files"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> StreamingResponse:
    """Genera un archivo Excel estructurado de 3 hojas listo para ciencia de datos."""
    dm_clean = datamart.strip().lower()
    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )
    try:
        dict_cols = deps.semantic_dict_repo.list_datamart_columns(datamart_name=dm_clean)
    except Exception:
        dict_cols = []

    try:
        lineage_rows = get_datamart_lineage(datamart_name=dm_clean, limit=200)
    except Exception:
        lineage_rows = []

    xlsx_bytes = _build_excel_workbook_bytes(
        datamart=dm_clean,
        query_res=query_res,
        dictionary_cols=dict_cols,
        lineage_rows=lineage_rows,
    )

    try:
        deps.api_key_repo.record_direct_export(
            datamart_name=dm_clean,
            export_format="XLSX",
            rows_returned=query_res.total_records,
            client_agent=request.headers.get("user-agent"),
        )
    except Exception:
        pass

    suffix = f"_{gestion}" if gestion else ""
    filename = f"DATAX_{dm_clean}{suffix}.xlsx"
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/export/csv",
    summary="Descargar archivo CSV (UTF-8 BOM) de un repositorio consolidado",
)
def export_datamart_csv(
    request: Request,
    datamart: str = Query(default="catalogo_series_files"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> Response:
    """Exporta los registros consolidados en formato CSV compatible con Excel."""
    dm_clean = datamart.strip().lower()
    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )
    csv_bytes = _build_csv_bytes(query_res)

    try:
        deps.api_key_repo.record_direct_export(
            datamart_name=dm_clean,
            export_format="CSV",
            rows_returned=query_res.total_records,
            client_agent=request.headers.get("user-agent"),
        )
    except Exception:
        pass

    filename = f"DATAX_{dm_clean}.csv"
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _build_jsonl_bytes(query_res: AnalyticsQueryResponse) -> bytes:
    lines = [
        json.dumps(
            {c: rec.get(c) for c in (query_res.columns or rec.keys())},
            ensure_ascii=False,
            default=str,
        )
        for rec in query_res.records
    ]
    payload = ("\n".join(lines) + "\n") if lines else ""
    return payload.encode("utf-8")


def _build_parquet_bytes(query_res: AnalyticsQueryResponse) -> bytes:
    with tempfile.TemporaryDirectory() as tmpdir:
        json_in = Path(tmpdir) / "export.json"
        pq_out = Path(tmpdir) / "export.parquet"
        con = duckdb.connect(":memory:")
        try:
            if query_res.records:
                json_in.write_text(
                    json.dumps(query_res.records, ensure_ascii=False, default=str),
                    encoding="utf-8",
                )
                con.execute(
                    f"COPY (SELECT * FROM read_json_auto('{json_in.as_posix()}')) "
                    f"TO '{pq_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            else:
                cols = query_res.columns or ["empty"]
                cols_sql = ", ".join(
                    f'CAST(NULL AS VARCHAR) AS "{c.replace(chr(34), "")}"' for c in cols
                )
                con.execute(
                    f"COPY (SELECT {cols_sql} WHERE FALSE) "
                    f"TO '{pq_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
        finally:
            con.close()
        return pq_out.read_bytes()


@router.get(
    "/export/parquet",
    summary="Descargar repositorio consolidado en formato columnar Apache Parquet ZSTD (Tarea 4.4)",
)
def export_datamart_parquet(
    request: Request,
    datamart: str = Query(default="catalogo_series_files"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> Response:
    """Exporta registros consolidados en formato Apache Parquet ZSTD lossless."""
    dm_clean = datamart.strip().lower()
    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )
    pq_bytes = _build_parquet_bytes(query_res)
    try:
        deps.api_key_repo.record_direct_export(
            datamart_name=dm_clean,
            export_format="PARQUET",
            rows_returned=query_res.total_records,
            client_agent=request.headers.get("user-agent"),
        )
    except Exception:
        pass
    filename = f"DATAX_{dm_clean}.parquet"
    return Response(
        content=pq_bytes,
        media_type="application/vnd.apache.parquet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/export/jsonl",
    summary="Descargar repositorio consolidado en formato JSON Lines / NDJSON (Tarea 4.4)",
)
def export_datamart_jsonl(
    request: Request,
    datamart: str = Query(default="catalogo_series_files"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> Response:
    """Exporta registros consolidados en formato NDJSON listo para streaming en Python/R."""
    dm_clean = datamart.strip().lower()
    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )
    jsonl_bytes = _build_jsonl_bytes(query_res)
    try:
        deps.api_key_repo.record_direct_export(
            datamart_name=dm_clean,
            export_format="JSONL",
            rows_returned=query_res.total_records,
            client_agent=request.headers.get("user-agent"),
        )
    except Exception:
        pass
    filename = f"DATAX_{dm_clean}.jsonl"
    return Response(
        content=jsonl_bytes,
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/sql",
    response_model=None,
    summary="Ejecutar consulta SQL SELECT de solo lectura sobre DuckDB efímero in-memory (Tarea 4.4)",
)
def execute_readonly_sql(
    payload: ReadOnlySqlRequest,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response | dict[str, Any]:
    """Ejecuta SELECT/WITH seguro sobre el catálogo cerrado de datasets Gold y permite exportar."""
    if x_api_key:
        entry, code, reason = deps.api_key_repo.authorize_and_log(
            raw_key=x_api_key,
            datamart_name="*",
            export_format=f"SQL_{payload.format.upper()}",
            rows_returned=0,
            client_agent=request.headers.get("user-agent"),
            required_layer="SQL",
        )
        if not entry:
            raise HTTPException(status_code=code, detail=reason or "No autorizado.")

    bulkhead = get_bulkhead_manager()
    try:
        if payload.format == "json":
            return bulkhead.execute_analytics(
                _readonly_sql_engine.execute_select,
                payload.sql,
                limit=payload.limit,
            )
        raw_bytes, media_type, row_count = bulkhead.execute_analytics(
            _readonly_sql_engine.export_select,
            payload.sql,
            export_format=payload.format,
            limit=payload.limit,
        )
        return Response(
            content=raw_bytes,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="datax_sql_result.{payload.format}"',
                "X-Row-Count": str(row_count),
                "Access-Control-Expose-Headers": "X-Row-Count, Content-Disposition",
            },
        )
    except UnsafeSqlError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err


@router.get(
    "/sql/tables",
    summary="Listar tablas autorizadas disponibles para la consola SQL de solo lectura",
)
def list_readonly_sql_tables() -> dict[str, Any]:
    """Devuelve los datasets Gold disponibles en el catálogo cerrado del motor SQL."""
    return {"tables": _readonly_sql_engine.list_available_tables()}


@router.get(
    "/keys",
    summary="Listar llaves API de autoservicio con alcance de capas y expiración",
)
def list_api_keys() -> list[dict[str, Any]]:
    """Retorna las llaves de autoservicio emitidas y sus métricas de uso."""
    return deps.api_key_repo.list_keys()


@router.post(
    "/keys",
    summary="Emitir nueva llave API con alcance por capa/dataset y vigencia opcional",
)
def create_api_key(payload: CreateApiKeyRequest) -> dict[str, Any]:
    """Genera una llave API lista para Python, R, cURL y Power Query con expiración."""
    exp = payload.expires_at
    if exp is None and payload.expires_in_days is not None:
        exp = datetime.now(timezone.utc) + timedelta(days=payload.expires_in_days)
    return deps.api_key_repo.create_key(
        label=payload.label,
        allowed_datamarts=payload.allowed_datamarts,
        allowed_layers=payload.allowed_layers,
        expires_at=exp,
    )


@router.delete(
    "/keys/{key_id}",
    summary="Revocar llave de conexión de autoservicio",
)
def revoke_api_key(key_id: str) -> dict[str, Any]:
    """Desactiva inmediatamente una llave API emitida."""
    ok = deps.api_key_repo.revoke_key(key_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"La llave de acceso '{key_id}' no fue encontrada.",
        )
    return {"status": "REVOKED", "key_id": key_id}


@router.get(
    "/access-logs",
    summary="Consultar bitácora de extracciones permitidas y denegadas",
)
def list_self_service_access_logs(
    limit: int = Query(default=30, ge=1, le=200),
) -> list[dict[str, Any]]:
    """Retorna el historial reciente de descargas directas y consumo programático."""
    return deps.api_key_repo.list_access_logs(limit=limit)


def _decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        decoded = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("ascii")
        offset = int(decoded)
        return max(0, offset)
    except Exception:
        raise HTTPException(status_code=400, detail="Cursor de paginación inválido.")


def _encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode("ascii")).decode("ascii")


@router.get(
    "/live-feed/{datamart}",
    summary="Conector programático con API Key, paginación por cursor y streaming (Python/R/cURL)",
)
def get_power_query_live_feed(
    datamart: str,
    request: Request,
    api_key: str | None = Query(default=None, description="Llave API de autoservicio"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    format: str = Query(default="csv", pattern="^(csv|json|jsonl|parquet)$"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
    cursor: str | None = Query(default=None, description="Cursor opaco de paginación"),
    page_size: int | None = Query(default=None, ge=1, le=10000),
) -> Response:
    """Entrega datos validando llave API (vigencia y capa), paginación por cursor y streaming."""
    token = (api_key or x_api_key or "").strip()
    dm_clean = datamart.strip().lower()
    if dm_clean not in DATAMART_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"El repositorio '{datamart}' no existe.",
        )

    offset = _decode_cursor(cursor)
    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )

    total_all = query_res.total_records
    if page_size is not None or cursor is not None:
        eff_page_size = page_size or 500
        sliced_records = query_res.records[offset : offset + eff_page_size]
        has_more = (offset + eff_page_size) < total_all
        next_cursor = _encode_cursor(offset + eff_page_size) if has_more else None
        paged_res = query_res.model_copy(
            update={"records": sliced_records, "total_records": len(sliced_records)}
        )
    else:
        has_more = False
        next_cursor = None
        paged_res = query_res

    validated, auth_status, denial_reason = deps.api_key_repo.authorize_and_log(
        raw_key=token,
        datamart_name=dm_clean,
        export_format=f"LIVE_{format.upper()}",
        rows_returned=paged_res.total_records,
        client_agent=request.headers.get("user-agent"),
        required_layer="GOLD",
    )
    if not validated:
        raise HTTPException(
            status_code=auth_status,
            detail=denial_reason
            or "Llave API ausente, inválida, expirada, revocada o sin autorización.",
        )

    extra_headers: dict[str, str] = {
        "X-Total-Records": str(total_all),
        "X-Has-More": "true" if has_more else "false",
        "Access-Control-Expose-Headers": "X-Total-Records, X-Has-More, X-Next-Cursor",
    }
    if next_cursor:
        extra_headers["X-Next-Cursor"] = next_cursor

    fmt_low = format.lower()
    if fmt_low == "json":
        body = json.dumps(
            {
                "datamart": dm_clean,
                "total_records": paged_res.total_records,
                "total_available": total_all,
                "has_more": has_more,
                "next_cursor": next_cursor,
                "columns": paged_res.columns,
                "records": paged_res.records,
            },
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
        return Response(content=body, media_type="application/json; charset=utf-8", headers=extra_headers)

    if fmt_low == "jsonl":
        def _jsonl_stream():
            cols = paged_res.columns
            for rec in paged_res.records:
                yield (
                    json.dumps({c: rec.get(c) for c in (cols or rec.keys())}, ensure_ascii=False, default=str)
                    + "\n"
                ).encode("utf-8")
        return StreamingResponse(_jsonl_stream(), media_type="application/x-ndjson", headers=extra_headers)

    if fmt_low == "parquet":
        pq_bytes = _build_parquet_bytes(paged_res)
        return Response(
            content=pq_bytes,
            media_type="application/vnd.apache.parquet",
            headers={**extra_headers, "Content-Disposition": f'attachment; filename="DATAX_{dm_clean}.parquet"'},
        )

    csv_bytes = _build_csv_bytes(paged_res)
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8", headers=extra_headers)
