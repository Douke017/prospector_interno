"""Router para Autoservicio Analítico, Exportación Excel y Conectores en Vivo."""

from __future__ import annotations

import csv
import io
from typing import Any

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


class CreateApiKeyRequest(BaseModel):
    """Solicitud para emitir una llave de conexión directa Excel / Power BI."""

    label: str = Field(
        ...,
        min_length=2,
        max_length=120,
        examples=["Libro Excel Riesgo Crediticio"],
    )
    allowed_datamarts: str = Field(
        default="*",
        description="'*' para todos los repositorios o lista separada por comas.",
    )


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
    summary="Listar estado y métricas en vivo de los 4 repositorios consolidados Gold",
)
def list_consolidated_datamarts() -> list[dict[str, Any]]:
    """Retorna el inventario operativo de los 4 repositorios analíticos."""
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
    datamart: str = "mart_cartera",
) -> dict[str, list[Any]]:
    """Consulta las instituciones, gestiones y meses consolidados."""
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
    datamart: str = Query(default="mart_cartera"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> StreamingResponse:
    """Genera un archivo Excel estructurado de 3 hojas listo para análisis financiero."""
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
    datamart: str = Query(default="mart_cartera"),
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


@router.get(
    "/keys",
    summary="Listar llaves de conexión directa para Excel Power Query y Power BI",
)
def list_api_keys() -> list[dict[str, Any]]:
    """Retorna las llaves de autoservicio emitidas y sus métricas de uso."""
    return deps.api_key_repo.list_keys()


@router.post(
    "/keys",
    summary="Emitir nueva llave de conexión para Excel Power Query / Power BI",
)
def create_api_key(payload: CreateApiKeyRequest) -> dict[str, Any]:
    """Genera una llave API lista para conectores externos sin requerir inicio de sesión."""
    return deps.api_key_repo.create_key(
        label=payload.label,
        allowed_datamarts=payload.allowed_datamarts,
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
    summary="Consultar bitácora de extracciones de autoservicio",
)
def list_self_service_access_logs(
    limit: int = Query(default=30, ge=1, le=200),
) -> list[dict[str, Any]]:
    """Retorna el historial reciente de descargas directas y conexiones Power Query."""
    return deps.api_key_repo.list_access_logs(limit=limit)


@router.get(
    "/live-feed/{datamart}",
    summary="Conector directo autenticado por llave API para Excel Power Query y Power BI",
)
def get_power_query_live_feed(
    datamart: str,
    request: Request,
    api_key: str | None = Query(default=None, description="Llave API de autoservicio"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    format: str = Query(default="csv", pattern="^(csv|json)$"),
    gestion: int | None = Query(default=None),
    mes: int | None = Query(default=None),
    institution_id: str | None = Query(default=None),
) -> Response:
    """Entrega datos en vivo validando llave API y registrando auditoría de acceso."""
    token = (api_key or x_api_key or "").strip()
    dm_clean = datamart.strip().lower()
    if dm_clean not in DATAMART_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"El repositorio '{datamart}' no existe.",
        )

    query_res = _execute_export_dataset(
        datamart=dm_clean,
        gestion=gestion,
        mes=mes,
        institution_id=institution_id,
    )

    validated = deps.api_key_repo.validate_and_log(
        raw_key=token,
        datamart_name=dm_clean,
        export_format=f"LIVE_{format.upper()}",
        rows_returned=query_res.total_records,
        client_agent=request.headers.get("user-agent"),
    )
    if not validated:
        raise HTTPException(
            status_code=401,
            detail="Llave API ausente, inválida, revocada o sin autorización para este repositorio.",
        )

    if format.lower() == "json":
        import json

        body = json.dumps(
            {
                "datamart": dm_clean,
                "total_records": query_res.total_records,
                "columns": query_res.columns,
                "records": query_res.records,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        return Response(content=body, media_type="application/json; charset=utf-8")

    csv_bytes = _build_csv_bytes(query_res)
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8")
