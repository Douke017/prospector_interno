"""Pruebas del Bloque 4: Descarga Bruta SHA-256, Perfilado, Parquet Lossless, SQL Read-Only y Reutilización Delta."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import zipfile

import duckdb
from fastapi.testclient import TestClient
import openpyxl
import pytest

from prospector_interno.adapters.analytics.readonly_sql_engine import ReadOnlyDuckDbSqlEngine
from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.main import app
from prospector_interno.adapters.api.routers.analytics_router import (
    set_readonly_sql_engine,
)
from prospector_interno.adapters.api.routers.catalog_router import (
    set_bronze_storage,
    set_profile_services,
)
from prospector_interno.adapters.api.routers.downloads_router import (
    get_worker_pool,
    set_worker_pool,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    set_derivation_filter,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.adapters.storage.postgres_v2_repositories import (
    InMemoryAssetProfileRepository,
    InMemoryDeduplicationRepository,
)
from prospector_interno.adapters.storage.sqlite_download_tracking import (
    SqliteDownloadTrackingAdapter,
)
from prospector_interno.application.catalog.raw_file_profiler import (
    RawFileProfilerService,
)
from prospector_interno.application.download_worker_pool import AsyncDownloadWorkerPool
from prospector_interno.application.pipelines.lossless_parquet_packager import (
    LosslessParquetPackager,
    SchemaDriftError,
)
from prospector_interno.domain.gatekeeper.derivation_filter import AssetDerivationFilter
from prospector_interno.domain.ports import GoldPartitionPort


def test_task_4_1_raw_binary_download_and_tamper_409(tmp_path: Path) -> None:
    bronze = LocalBronzeStorage(base_path=tmp_path / "bronze")
    raw_payload = b"codigo_entidad,fecha_corte,saldo_bruto\nBNI,2026-01-31,12345.67\n"
    expected_sha = hashlib.sha256(raw_payload).hexdigest()
    art = bronze.store_stream(
        domain="www.asfi.gob.bo",
        dataset_code="DS_RAW_ASFI",
        period="2026-01",
        stream=io.BytesIO(raw_payload),
        content_type="text/csv",
        url="https://www.asfi.gob.bo/raw/serie_2026_01.csv",
    )

    set_bronze_storage(bronze)
    client = TestClient(app)

    # 1. Descarga íntegra bit-a-bit con cabecera X-Content-SHA256
    resp = client.get(f"/api/v1/catalog/assets/{art.artifact_id}/download")
    assert resp.status_code == 200
    assert resp.content == raw_payload
    assert resp.headers.get("X-Content-SHA256") == expected_sha
    assert "attachment" in (resp.headers.get("Content-Disposition") or "")

    # 2. Manipulación en disco (1 byte alterado) -> rechazo HTTP 409
    art.file_path.write_bytes(raw_payload + b"X")
    corrupted = client.get(f"/api/v1/catalog/assets/{art.artifact_id}/download")
    assert corrupted.status_code == 409
    assert "SHA-256" in corrupted.json()["detail"]

    # 3. Ausencia física en disco -> rechazo HTTP 409
    art.file_path.unlink()
    missing = client.get(f"/api/v1/catalog/assets/{art.artifact_id}/download")
    assert missing.status_code == 409

    # Restaurar referencia por defecto
    set_bronze_storage(deps.bronze_storage)


def test_task_4_2_raw_file_profiler_all_formats_and_column_search(tmp_path: Path) -> None:
    profile_repo = InMemoryAssetProfileRepository()
    profiler = RawFileProfilerService(profile_repo=profile_repo)

    # 1. CSV con 25 filas y nulos
    csv_path = tmp_path / "series.csv"
    csv_lines = ["codigo_entidad;fecha_corte;monto_raw;activo"]
    for i in range(25):
        val = "" if i % 5 == 0 else str(100.5 + i)
        csv_lines.append(f"ENT_{i:02d};2026-01-{(i % 28) + 1:02d};{val};true")
    csv_path.write_text("\n".join(csv_lines), encoding="utf-8")
    csv_sha_before = hashlib.sha256(csv_path.read_bytes()).hexdigest()

    csv_prof = profiler.profile_file(
        asset_id="raw_csv_01",
        file_path=csv_path,
        source_id="ASFI",
        dataset_id="asfi_cartera_raw",
    )
    assert csv_prof["file_format"] == "csv"
    assert csv_prof["csv_delimiter"] == ";"
    assert csv_prof["row_count"] == 25
    assert len(csv_prof["sample_rows"]) == 20
    assert hashlib.sha256(csv_path.read_bytes()).hexdigest() == csv_sha_before
    monto_col = next(c for c in csv_prof["columns_summary"] if c["column_name"] == "monto_raw")
    assert monto_col["inferred_type"] == "FLOAT"
    assert monto_col["null_pct"] == 20.0

    # 2. XLSX multi-hoja
    xlsx_path = tmp_path / "boletin.xlsx"
    wb = openpyxl.Workbook()
    ws1 = wb.active
    ws1.title = "Balances_Raw"
    ws1.append(["codigo_entidad", "cuenta", "saldo_bs"])
    ws1.append(["FIE", "131", 50000])
    ws1.append(["PRODEM", "131", 62000])
    ws2 = wb.create_sheet("Notas_Metodologicas")
    ws2.append(["nota"])
    ws2.append(["Sin alteraciones"])
    wb.save(xlsx_path)

    xlsx_prof = profiler.profile_file(
        asset_id="raw_xlsx_01",
        file_path=xlsx_path,
        source_id="FINRURAL",
    )
    assert xlsx_prof["file_format"] == "xlsx"
    assert xlsx_prof["sheet_names"] == ["Balances_Raw", "Notas_Metodologicas"]
    assert xlsx_prof["row_count"] == 2
    assert xlsx_prof["column_count"] == 3

    # 3. Parquet
    pq_path = tmp_path / "serie.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 'BCB' AS codigo_entidad, 2026 AS gestion, 3.14 AS tasa) "
            f"TO '{pq_path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    pq_prof = profiler.profile_file(asset_id="raw_pq_01", file_path=pq_path, source_id="BCB")
    assert pq_prof["file_format"] == "parquet"
    assert pq_prof["row_count"] == 1
    assert any(c["column_name"] == "tasa" for c in pq_prof["columns_summary"])

    # 4. JSON
    json_path = tmp_path / "api_dump.json"
    json_path.write_text(
        json.dumps([{"codigo_entidad": "BBV", "volumen_usd": 1200.0}, {"codigo_entidad": "BBV", "volumen_usd": None}]),
        encoding="utf-8",
    )
    json_prof = profiler.profile_file(asset_id="raw_json_01", file_path=json_path, source_id="BBV")
    assert json_prof["file_format"] == "json"
    assert json_prof["row_count"] == 2

    # 5. ZIP con CSV interno
    zip_path = tmp_path / "paquete.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("enero/datos.csv", "codigo_entidad,valor\nINE,42\n")
    zip_prof = profiler.profile_file(asset_id="raw_zip_01", file_path=zip_path, source_id="INE")
    assert zip_prof["file_format"] == "zip"
    assert len(zip_prof["archive_entries"]) == 1
    assert zip_prof["row_count"] == 1

    # 6. XLS (cabecera OLE2 BIFF)
    xls_path = tmp_path / "legacy.xls"
    xls_path.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 128)
    xls_prof = profiler.profile_file(asset_id="raw_xls_01", file_path=xls_path, source_id="INE")
    assert xls_prof["file_format"] == "xls"

    # 7. Búsqueda global por nombre de columna vía API
    set_profile_services(profile_repo, profiler)
    client = TestClient(app)
    try:
        search_resp = client.get("/api/v1/catalog/search?column=codigo_entidad")
        assert search_resp.status_code == 200
        sdata = search_resp.json()
        matched_assets = {m["asset_id"] for m in sdata["column_matches"]}
        assert {"raw_csv_01", "raw_xlsx_01", "raw_pq_01", "raw_json_01", "raw_zip_01"} <= matched_assets

        # Endpoint /profile devuelve perfil persistido
        prof_resp = client.get("/api/v1/catalog/assets/raw_xlsx_01/profile")
        assert prof_resp.status_code == 200
        assert prof_resp.json()["sheet_names"] == ["Balances_Raw", "Notas_Metodologicas"]
    finally:
        set_profile_services(deps.asset_profile_repo, deps.raw_file_profiler)


def test_task_4_3_lossless_parquet_atomic_and_schema_drift_blocking(tmp_path: Path) -> None:
    gold_dir = tmp_path / "gold"
    packager = LosslessParquetPackager(base_gold_path=gold_dir)

    csv_jan = tmp_path / "jan.csv"
    csv_jan.write_text("codigo_entidad,monto_bruto\nBNI,100.25\nFIE,250.75\n", encoding="utf-8")

    res_jan = packager.pack_raw_file(
        raw_file_path=csv_jan,
        dataset_id="raw_cartera_serie",
        source_id="ASFI",
        gestion=2026,
        mes=1,
    )
    assert res_jan.row_count == 2
    assert res_jan.parquet_bytes > 0
    assert Path(res_jan.file_path).is_file()
    jan_bytes_before = Path(res_jan.file_path).read_bytes()

    # Verificar valores exactos lossless sin fórmulas ocultas
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            f"SELECT codigo_entidad, monto_bruto FROM read_parquet('{res_jan.file_path}') ORDER BY codigo_entidad"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("BNI", 100.25), ("FIE", 250.75)]

    # Nuevo mes con esquema incompatible -> dispara SCHEMA_DRIFT bloqueante sin corromper enero
    csv_feb_drift = tmp_path / "feb_drift.csv"
    csv_feb_drift.write_text("codigo_entidad,nueva_columna_extra\nBNI,texto\n", encoding="utf-8")
    with pytest.raises(SchemaDriftError) as exc_info:
        packager.pack_raw_file(
            raw_file_path=csv_feb_drift,
            dataset_id="raw_cartera_serie",
            source_id="ASFI",
            gestion=2026,
            mes=2,
        )
    assert exc_info.value.code == "SCHEMA_DRIFT"
    assert "nueva_columna_extra" in exc_info.value.added_columns
    assert "monto_bruto" in exc_info.value.removed_columns
    # La partición de enero permanece intacta y febrero no dejó archivo corrupto
    assert Path(res_jan.file_path).read_bytes() == jan_bytes_before
    assert not (gold_dir / "raw_cartera_serie" / "gestion=2026" / "mes=02" / "part-000.parquet").exists()


def test_task_4_4_parquet_jsonl_readonly_sql_security_and_key_governance(tmp_path: Path) -> None:
    client = TestClient(app)

    # Preparar un repositorio Gold sintético pequeño para pruebas SQL rápidas y determinísticas
    gold_tmp = tmp_path / "gold_sql"
    part = gold_tmp / "serie_cruda_demo" / "gestion=2026" / "mes=01"
    part.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 'BNI' AS entidad, 100.5 AS valor UNION ALL SELECT 'FIE', 200.0) "
            f"TO '{(part / 'part-000.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()

    engine = ReadOnlyDuckDbSqlEngine(base_gold_path=gold_tmp)
    set_readonly_sql_engine(engine)

    try:
        # 1. Consulta SELECT válida -> 200 + tipos + latencia + filas
        ok_sql = client.post(
            "/api/v1/analytics/sql",
            json={"sql": "SELECT entidad, valor FROM serie_cruda_demo ORDER BY entidad", "limit": 50},
        )
        assert ok_sql.status_code == 200
        sdata = ok_sql.json()
        assert sdata["columns"] == ["entidad", "valor"]
        assert sdata["row_count"] == 2
        assert sdata["elapsed_ms"] >= 0.0

        # Exportación directa desde SQL a Parquet y JSONL
        sql_pq = client.post(
            "/api/v1/analytics/sql",
            json={"sql": "SELECT entidad, valor FROM serie_cruda_demo", "format": "parquet"},
        )
        assert sql_pq.status_code == 200
        assert sql_pq.content[:4] == b"PAR1"

        sql_jsonl = client.post(
            "/api/v1/analytics/sql",
            json={"sql": "SELECT entidad, valor FROM serie_cruda_demo", "format": "jsonl"},
        )
        assert sql_jsonl.status_code == 200
        assert len(sql_jsonl.text.strip().splitlines()) == 2

        # 2. Bloqueo estricto de DDL/DML/COPY/ATTACH/PRAGMA/read_parquet -> HTTP 400
        malicious_queries = [
            "DROP TABLE serie_cruda_demo",
            "DELETE FROM serie_cruda_demo",
            "INSERT INTO serie_cruda_demo VALUES ('X', 1)",
            "COPY serie_cruda_demo TO '/tmp/pwn.csv'",
            "ATTACH '/tmp/evil.db' AS evil",
            "PRAGMA database_list",
            "SELECT * FROM read_parquet('/etc/passwd')",
            "SELECT 1; DROP TABLE serie_cruda_demo",
        ]
        for bad_sql in malicious_queries:
            r = client.post("/api/v1/analytics/sql", json={"sql": bad_sql})
            assert r.status_code == 400, f"Debió bloquear con 400: {bad_sql}"

        # 3. Exportación Parquet y JSONL de datamarts + cursor pagination en live-feed
        if not deps.analytics_adapter.has_data("catalogo_series_files"):
            client.post("/api/v1/analytics/materialize?datamart=catalogo_series_files")

        pq_exp = client.get("/api/v1/analytics/export/parquet?datamart=catalogo_series_files")
        assert pq_exp.status_code == 200
        assert pq_exp.content[:4] == b"PAR1"

        jsonl_exp = client.get("/api/v1/analytics/export/jsonl?datamart=catalogo_series_files")
        assert jsonl_exp.status_code == 200
        first_line = json.loads(jsonl_exp.text.strip().splitlines()[0])
        assert isinstance(first_line, dict)

        # 4. Llave API expirada -> HTTP 401; llave restringida a RAW -> HTTP 403 en GOLD/SQL
        past_iso = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        expired_key = client.post(
            "/api/v1/analytics/keys",
            json={
                "label": "Llave Expirada Test",
                "allowed_datamarts": "*",
                "allowed_layers": ["GOLD", "SQL"],
                "expires_at": past_iso,
            },
        ).json()["api_key"]

        exp_resp = client.get(
            "/api/v1/analytics/live-feed/catalogo_series_files?format=jsonl",
            headers={"X-API-Key": expired_key},
        )
        assert exp_resp.status_code == 401

        raw_only_key = client.post(
            "/api/v1/analytics/keys",
            json={
                "label": "Solo Capa RAW",
                "allowed_datamarts": "catalogo_series_files",
                "allowed_layers": ["RAW"],
                "expires_in_days": 30,
            },
        ).json()["api_key"]

        forbidden_layer_resp = client.get(
            "/api/v1/analytics/live-feed/catalogo_series_files?format=json",
            headers={"X-API-Key": raw_only_key},
        )
        assert forbidden_layer_resp.status_code == 403

        forbidden_sql_resp = client.post(
            "/api/v1/analytics/sql",
            json={"sql": "SELECT * FROM serie_cruda_demo"},
            headers={"X-API-Key": raw_only_key},
        )
        assert forbidden_sql_resp.status_code == 403

        # Llave vigente con alcance GOLD -> paginación por cursor funciona
        valid_key = client.post(
            "/api/v1/analytics/keys",
            json={
                "label": "Script Polars Vigente",
                "allowed_datamarts": "catalogo_series_files",
                "allowed_layers": ["GOLD", "SQL"],
                "expires_in_days": 10,
            },
        ).json()["api_key"]

        page1 = client.get(
            "/api/v1/analytics/live-feed/catalogo_series_files?format=json&page_size=2",
            headers={"X-API-Key": valid_key},
        )
        assert page1.status_code == 200
        p1_data = page1.json()
        assert len(p1_data["records"]) <= 2
        if p1_data["has_more"]:
            assert p1_data["next_cursor"] is not None
            page2 = client.get(
                f"/api/v1/analytics/live-feed/catalogo_series_files?format=json&page_size=2&cursor={p1_data['next_cursor']}",
                headers={"X-API-Key": valid_key},
            )
            assert page2.status_code == 200

        # Verificar que intentos denegados quedaron en access-logs
        logs = client.get("/api/v1/analytics/access-logs?limit=50").json()
        assert any(l.get("status") == "DENIED" for l in logs)
    finally:
        set_readonly_sql_engine(deps.readonly_sql_engine)


class _StubGoldPartitions(GoldPartitionPort):
    def __init__(self, parts: set[tuple[int, int]]) -> None:
        self._parts = parts

    def list_available_partitions(self, datamart: str = "mart_cartera") -> list[tuple[int, int]]:
        return sorted(self._parts)

    def has_partition(self, datamart: str, gestion: int, mes: int) -> bool:
        return (gestion, mes) in self._parts


def test_task_4_5_arbitrary_ranges_master_files_and_delta_enqueue(tmp_path: Path) -> None:
    # 9 meses de 2025 en Gold (faltan oct, nov, dic) + archivo maestro anual 2023 en Bronze
    gold_parts = {(2025, m) for m in range(1, 10)} | {(2024, 11), (2024, 12)}
    bronze = LocalBronzeStorage(base_path=tmp_path / "bronze")
    bronze.store_stream(
        domain="www.bcb.gob.bo",
        dataset_code="mart_cartera",
        period="2023",
        stream=io.BytesIO(b"master_2023_data"),
    )
    deriv_filter = AssetDerivationFilter(
        partition_checker=_StubGoldPartitions(gold_parts),
        base_gold_path=tmp_path / "gold",
        bronze_storage=bronze,
    )
    tracking = SqliteDownloadTrackingAdapter(":memory:")
    pool = AsyncDownloadWorkerPool(
        tracking_port=tracking,
        bronze_storage=bronze,
        dedup_repo=InMemoryDeduplicationRepository(),
    )

    prev_pool = get_worker_pool()
    set_derivation_filter(deriv_filter)
    set_worker_pool(pool)
    client = TestClient(app)

    try:
        # 1. Rango multi-año 100% cubierto ('2024-11..2025-02') -> 0 descargas delta
        full_range = client.post(
            "/api/v1/gatekeeper/inspect",
            json={"dataset_code": "mart_cartera", "period": "2024-11..2025-02"},
        )
        assert full_range.status_code == 200
        fdata = full_range.json()
        assert fdata["is_derivable"] is True
        assert fdata["coverage_ratio"] == 1.0
        assert fdata["required_periods"] == ["2024-11", "2024-12", "2025-01", "2025-02"]
        assert "gestion = 2024" in fdata["precompiled_sql"]
        assert "gestion = 2025" in fdata["precompiled_sql"]

        complete_delta = client.post(
            "/api/v1/gatekeeper/enqueue-missing",
            json={"dataset_code": "mart_cartera", "period": "2024-11..2025-02", "execute_now": False},
        )
        assert complete_delta.status_code == 200
        assert complete_delta.json()["status"] == "ALREADY_COMPLETE"
        assert complete_delta.json()["enqueued_count"] == 0

        # 2. Cobertura 100% por archivo maestro anual en Bronze (2023)
        master_resp = client.post(
            "/api/v1/gatekeeper/inspect",
            json={"dataset_code": "mart_cartera", "period": "2023"},
        )
        assert master_resp.status_code == 200
        assert master_resp.json()["is_derivable"] is True
        assert master_resp.json()["coverage_ratio"] == 1.0

        # 3. Cobertura parcial (9 de 12 meses de 2025) -> encola ÚNICAMENTE los 3 cortes faltantes
        partial_delta = client.post(
            "/api/v1/gatekeeper/enqueue-missing",
            json={
                "dataset_code": "mart_cartera",
                "period": "2025",
                "source_id": "FINRURAL",
                "execute_now": False,
            },
        )
        assert partial_delta.status_code == 200
        pdata = partial_delta.json()
        assert pdata["status"] == "DELTA_ENQUEUED"
        assert pdata["enqueued_count"] == 3
        assert pdata["enqueued_periods"] == ["2025-10", "2025-11", "2025-12"]
        assert len(pdata["available_periods"]) == 9
    finally:
        set_derivation_filter(deps.derivation_filter)
        set_worker_pool(prev_pool)
