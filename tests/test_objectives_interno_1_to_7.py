"""Suite integral de verificación End-to-End de los 7 Objetivos Específicos del Prospector Interno."""

from __future__ import annotations

import io
import time

import openpyxl
from starlette.testclient import TestClient

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.main import app

client = TestClient(app)


def test_obj_1_5_6_gatekeeper_duplicity_and_live_download_queue() -> None:
    """Valida Objetivos 1, 5 y 6: validación pre-vuelo, duplicidad y cola de descarga."""
    # 1. Objetivo 6: Encolar transferencia activa en download_tracking
    target_url = "https://www.bcb.gob.bo/webdocs/estadisticas/boletin_en_vuelo_2026_11.xlsx"
    enqueue_resp = client.post(
        "/api/v1/downloads/queue",
        json={
            "source_id": "BCB",
            "canonical_url": target_url,
            "status": "DOWNLOADING",
            "task_id": "worker_bcb_live_01",
        },
    )
    assert enqueue_resp.status_code == 200
    dl_item = enqueue_resp.json()
    assert dl_item["status"] == "DOWNLOADING"
    assert dl_item["task_id"] == "worker_bcb_live_01"

    # Confirmar visibilidad en cola en tiempo real
    status_resp = client.get("/api/v1/downloads/status?include_recent=true")
    assert status_resp.status_code == 200
    status_data = status_resp.json()
    assert status_data["total_active"] >= 1
    assert any(d["canonical_url"] == target_url for d in status_data["downloads"])

    # 2. Objetivo 1 y 6: Solicitud concurrente detecta descarga activa en < 200 ms y se suscribe
    t0 = time.perf_counter()
    eval_in_flight = client.post(
        "/api/v1/gatekeeper/evaluate",
        json={
            "request_id": "analista_riesgo_01",
            "source_id": "bcb",
            "dataset_code": "DS_BO_BCB_SEMANAL",
            "period": "2026-11",
            "target_url": target_url,
        },
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert eval_in_flight.status_code == 200
    assert elapsed_ms < 200.0
    ev_data = eval_in_flight.json()
    assert ev_data["verdict"] == "IN_PROGRESS"
    assert ev_data["lock_acquired"] is False
    assert ev_data["in_flight_task_id"] == "worker_bcb_live_01"
    assert ev_data["subscribers_count"] >= 1

    # 3. Objetivo 5: Detección de duplicidad contra archivo ya custodiado en Capa Bronze
    artifacts = deps.bronze_storage.get_all_artifacts()
    custodied_with_url = next((a for a in artifacts if a.get("url")), None)
    assert custodied_with_url is not None

    eval_cached = client.post(
        "/api/v1/gatekeeper/evaluate",
        json={
            "request_id": "analista_auditoria_02",
            "domain": custodied_with_url["domain"],
            "dataset_code": custodied_with_url["dataset_code"],
            "period": custodied_with_url.get("period") or "2026-01",
            "target_url": custodied_with_url["url"],
            "force_refresh": False,
        },
    )
    assert eval_cached.status_code == 200
    cached_data = eval_cached.json()
    assert cached_data["verdict"] == "CACHE_HIT_BRONZE"
    assert cached_data["lock_acquired"] is False
    assert cached_data["cached_artifact_id"] is not None


def test_obj_2_3_7_four_gold_datamarts_catalog_lineage_and_reuse() -> None:
    """Valida Objetivos 2, 3 y 7: 4 Datamarts Gold, diccionario, relaciones, linaje y reutilización."""
    # 1. Objetivo 2: Consolidar los 4 repositorios Gold desde los archivos reales en Bronze
    mat_resp = client.post("/api/v1/analytics/materialize")
    assert mat_resp.status_code == 200
    mat_data = mat_resp.json()
    assert mat_data["status"] == "COMPLETED"
    assert mat_data["total_gold_records"] > 0

    dm_summary_resp = client.get("/api/v1/analytics/datamarts")
    assert dm_summary_resp.status_code == 200
    dm_list = dm_summary_resp.json()
    assert len(dm_list) == 4
    by_name = {d["datamart"]: d for d in dm_list}
    for required_dm in (
        "mart_cartera",
        "mart_tasas_bcb",
        "mart_estadisticas_bbv",
        "mart_indicadores_asfi",
    ):
        assert required_dm in by_name
        assert by_name[required_dm]["is_materialized"] is True
        assert by_name[required_dm]["total_rows"] > 0

    # 2. Objetivo 3: Diccionario técnico de datos y relaciones entre repositorios
    dict_resp = client.get("/api/v1/dictionary/datamarts")
    assert dict_resp.status_code == 200
    dict_payload = dict_resp.json()
    assert dict_payload["total_columns"] >= 40

    bcb_dict_resp = client.get("/api/v1/dictionary/datamarts/mart_tasas_bcb")
    assert bcb_dict_resp.status_code == 200
    bcb_dict = bcb_dict_resp.json()
    assert bcb_dict["datamart"] == "mart_tasas_bcb"
    assert len(bcb_dict["columns"]) >= 10
    assert len(bcb_dict["relationships"]) >= 1

    rels_resp = client.get("/api/v1/dictionary/relationships")
    assert rels_resp.status_code == 200
    rels = rels_resp.json()
    assert len(rels) >= 4

    # Verificar inventario de activos Gold y verificación criptográfica de linaje SHA-256
    gold_assets_resp = client.get("/api/v1/catalog/assets?storage_layer=GOLD")
    assert gold_assets_resp.status_code == 200
    gold_assets = gold_assets_resp.json()
    assert len(gold_assets) >= 4

    sample_asset_id = gold_assets[0]["asset_id"]
    verify_resp = client.get(f"/api/v1/catalog/assets/{sample_asset_id}/verify")
    assert verify_resp.status_code == 200
    verify_data = verify_resp.json()
    assert verify_data["is_valid"] is True
    assert verify_data["all_upstream_intact"] is True
    assert len(verify_data["upstream_details"]) >= 1

    dm_lineage_resp = client.get("/api/v1/catalog/lineage/datamart/mart_tasas_bcb")
    assert dm_lineage_resp.status_code == 200
    assert len(dm_lineage_resp.json()) >= 1

    # 3. Objetivo 7: Reutilización de insumos consolidados (derivación trimestral sin redescargar)
    dims_bcb = client.get("/api/v1/analytics/dimensions?datamart=mart_tasas_bcb").json()
    assert 2025 in dims_bcb["gestiones"]

    inspect_resp = client.post(
        "/api/v1/gatekeeper/inspect",
        json={
            "dataset_code": "mart_tasas_bcb",
            "period": "2025-Q3",
        },
    )
    assert inspect_resp.status_code == 200
    insp = inspect_resp.json()
    assert insp["is_derivable"] is True
    assert insp["coverage_ratio"] == 1.0
    assert insp["precompiled_sql"] is not None
    assert "mart_tasas_bcb" in insp["precompiled_sql"]

    eval_deriv = client.post(
        "/api/v1/gatekeeper/evaluate",
        json={
            "request_id": "analista_macro_q3",
            "source_id": "bcb",
            "dataset_code": "mart_tasas_bcb",
            "period": "2025-Q3",
        },
    )
    assert eval_deriv.status_code == 200
    ev_deriv_data = eval_deriv.json()
    assert ev_deriv_data["verdict"] == "REUSE_DERIVATION"
    assert ev_deriv_data["derivation_query"] is not None


def test_obj_4_self_service_views_excel_csv_and_api_key_live_feed() -> None:
    """Valida Objetivo 4: vistas preestablecidas, Excel 3 hojas, CSV y conectores API Key."""
    # Asegurar que los 4 datamarts estén consolidados
    if not deps.analytics_adapter.has_data("mart_tasas_bcb"):
        client.post("/api/v1/analytics/materialize")

    # 1. Catálogo extendido de vistas preestablecidas para los 4 repositorios
    all_views_resp = client.get("/api/v1/analytics/views?datamart=all")
    assert all_views_resp.status_code == 200
    all_views = all_views_resp.json()
    assert len(all_views) == 6

    exec_view_resp = client.post(
        "/api/v1/analytics/views/evolucion-monetaria-bcb/execute",
        json={"limit": 12},
    )
    assert exec_view_resp.status_code == 200
    view_res = exec_view_resp.json()
    assert view_res["total_records"] > 0

    # 2. Exportación directa a Excel (.xlsx) estructurado de 3 hojas
    xlsx_resp = client.get("/api/v1/analytics/export/xlsx?datamart=mart_tasas_bcb")
    assert xlsx_resp.status_code == 200
    assert (
        xlsx_resp.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_resp.content))
    assert wb.sheetnames == [
        "Datos_Consolidados",
        "Diccionario_Variables",
        "Linaje_Documental",
    ]
    assert wb["Datos_Consolidados"].max_row > 2
    assert wb["Diccionario_Variables"].max_row > 5
    assert wb["Linaje_Documental"].max_row >= 2

    # 3. Exportación directa CSV con UTF-8 BOM
    csv_resp = client.get("/api/v1/analytics/export/csv?datamart=mart_estadisticas_bbv")
    assert csv_resp.status_code == 200
    assert csv_resp.content.startswith(b"\xef\xbb\xbf")
    csv_lines = csv_resp.content.decode("utf-8-sig").strip().splitlines()
    assert len(csv_lines) > 2

    # 4. Conectores en vivo con API Key (sin login de usuario) para Excel Power Query / Power BI
    unauth_feed = client.get("/api/v1/analytics/live-feed/mart_indicadores_asfi")
    assert unauth_feed.status_code == 401

    key_create = client.post(
        "/api/v1/analytics/keys",
        json={
            "label": "Reporte Ejecutivo Excel Gerencia",
            "allowed_datamarts": "mart_indicadores_asfi,mart_cartera",
        },
    )
    assert key_create.status_code == 200
    created_key = key_create.json()
    raw_token = created_key["api_key"]
    key_id = created_key["key_id"]
    assert raw_token.startswith("dtx_live_")

    # Acceso autorizado a mart_indicadores_asfi (CSV y JSON)
    live_csv = client.get(
        f"/api/v1/analytics/live-feed/mart_indicadores_asfi?api_key={raw_token}&format=csv"
    )
    assert live_csv.status_code == 200
    assert len(live_csv.content.decode("utf-8-sig").splitlines()) > 2

    live_json = client.get(
        "/api/v1/analytics/live-feed/mart_cartera?format=json",
        headers={"X-API-Key": raw_token},
    )
    assert live_json.status_code == 200
    assert live_json.json()["total_records"] > 0

    # Acceso denegado a datamart fuera del alcance de la llave
    forbidden_dm = client.get(
        f"/api/v1/analytics/live-feed/mart_tasas_bcb?api_key={raw_token}"
    )
    assert forbidden_dm.status_code == 401

    # Verificar registro en bitácora de extracciones
    logs_resp = client.get("/api/v1/analytics/access-logs")
    assert logs_resp.status_code == 200
    logs = logs_resp.json()
    assert len(logs) >= 2
    assert any(l["datamart_name"] == "mart_indicadores_asfi" for l in logs)

    # Revocación inmediata de llave
    revoke_resp = client.delete(f"/api/v1/analytics/keys/{key_id}")
    assert revoke_resp.status_code == 200
    after_revoke = client.get(
        f"/api/v1/analytics/live-feed/mart_indicadores_asfi?api_key={raw_token}"
    )
    assert after_revoke.status_code == 401
