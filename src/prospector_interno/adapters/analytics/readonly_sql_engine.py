"""Motor SQL de Solo Lectura sobre DuckDB Efímero In-Memory (Objetivo 4 — Tarea 4.4).

Garantiza aislamiento estricto para autoservicio de científicos de datos:
1. Validación estricta de sentencia única (`SELECT` / `WITH` solamente).
2. Bloqueo de DDL/DML/I/O (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `COPY`, `ATTACH`,
   `PRAGMA`, `INSTALL`, `LOAD`, `SET`, `READ_PARQUET`, `READ_CSV`, `GLOB`, etc.).
3. Conexión efímera `duckdb.connect(":memory:")` por consulta.
4. Carga únicamente las tablas autorizadas del catálogo Gold en memoria y luego
   bloquea el motor con `SET enable_external_access = false` + `SET lock_configuration = true`,
   `SET memory_limit = '512MB'` y `SET threads = 2`.
"""

from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import tempfile
import time
from typing import Any

import duckdb


class UnsafeSqlError(ValueError):
    """Error lanzado ante cualquier intento de SQL no permitido o fuera de solo lectura."""


FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b("
    r"DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|"
    r"COPY|ATTACH|DETACH|PRAGMA|INSTALL|LOAD|SET|RESET|CALL|EXPORT|IMPORT|"
    r"EXEC|EXECUTE|VACUUM|CHECKPOINT|BEGIN|COMMIT|ROLLBACK|GRANT|REVOKE|"
    r"READ_CSV|READ_CSV_AUTO|READ_PARQUET|READ_JSON|READ_JSON_AUTO|"
    r"READ_TEXT|READ_BLOB|PARQUET_SCAN|CSV_SCAN|GLOB|HTTPFS|ICEBERG_SCAN|"
    r"POSTGRES_ATTACH|SQLITE_ATTACH|SYSTEM|SHELL"
    r")\b",
    re.IGNORECASE,
)


def _strip_sql_comments(sql: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    no_line = re.sub(r"--[^\n]*", " ", no_block)
    return no_line.strip()


def validate_readonly_select_sql(raw_sql: str) -> str:
    """Valida que la consulta sea estrictamente un único SELECT/WITH de solo lectura."""
    if not raw_sql or not raw_sql.strip():
        raise UnsafeSqlError("La consulta SQL no puede estar vacía.")

    cleaned = _strip_sql_comments(raw_sql)
    # Permitir un único ';' final, prohibir múltiples sentencias
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise UnsafeSqlError(
            "No se permiten múltiples sentencias SQL separadas por punto y coma."
        )

    upper_head = cleaned.lstrip().upper()
    if not (upper_head.startswith("SELECT ") or upper_head.startswith("SELECT\n") or upper_head.startswith("WITH ") or upper_head.startswith("WITH\n")):
        raise UnsafeSqlError(
            "Solo se permiten consultas de lectura que comiencen con SELECT o WITH."
        )

    # Inspeccionar fuera de literales de cadena entre comillas simples
    unquoted = re.sub(r"'(?:''|[^'])*'", "''", cleaned)
    match = FORBIDDEN_SQL_KEYWORDS.search(unquoted)
    if match:
        raise UnsafeSqlError(
            f"Operación SQL prohibida en consola de solo lectura: '{match.group(1).upper()}'."
        )

    return cleaned


def _serialize_cell(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


class ReadOnlyDuckDbSqlEngine:
    """Ejecutor SQL de solo lectura aislado sobre catálogo cerrado de datasets Gold."""

    def __init__(
        self,
        *,
        base_gold_path: Path,
        memory_limit: str = "512MB",
        threads: int = 2,
    ) -> None:
        self.base_gold_path = Path(base_gold_path)
        self.memory_limit = memory_limit
        self.threads = threads

    def list_available_tables(self) -> list[str]:
        """Retorna los datasets Gold efectivamente materializados como Parquet."""
        if not self.base_gold_path.is_dir():
            return []
        tables: list[str] = []
        for subdir in sorted(self.base_gold_path.iterdir()):
            if not subdir.is_dir():
                continue
            name = subdir.name
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                continue
            if any(subdir.glob("**/*.parquet")):
                tables.append(name)
        return tables

    def _prepare_sandbox_connection(self) -> tuple[duckdb.DuckDBPyConnection, list[str]]:
        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit = '{self.memory_limit}'")
        con.execute(f"SET threads = {self.threads}")
        tables = self.list_available_tables()
        for tbl in tables:
            glob_path = (
                (self.base_gold_path / tbl / "**/*.parquet")
                .resolve()
                .as_posix()
                .replace("'", "''")
            )
            con.execute(
                f'CREATE TABLE "{tbl}" AS '
                f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true)"
            )
        # Defensa en profundidad a nivel de motor C++: desactivar cualquier I/O externo
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        return con, tables

    def execute_select(
        self,
        sql: str,
        *,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Ejecuta una consulta SELECT segura y retorna metadatos y filas serializadas."""
        safe_sql = validate_readonly_select_sql(sql)
        safe_limit = max(1, min(int(limit), 10_000))
        t0 = time.perf_counter()
        con, tables = self._prepare_sandbox_connection()
        try:
            wrapped = f"SELECT * FROM ({safe_sql}) AS _datax_ro_q LIMIT {safe_limit + 1}"
            cursor = con.execute(wrapped)
            desc = cursor.description or []
            columns = [str(col[0]) for col in desc]
            column_types = [str(col[1]) for col in desc]
            fetched = cursor.fetchall()
            truncated = len(fetched) > safe_limit
            rows_raw = fetched[:safe_limit]
            rows = [
                {columns[i]: _serialize_cell(val) for i, val in enumerate(row)}
                for row in rows_raw
            ]
        except duckdb.Error as err:
            raise UnsafeSqlError(f"Error al ejecutar consulta SQL: {err}") from err
        finally:
            con.close()

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        return {
            "columns": columns,
            "column_types": column_types,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "elapsed_ms": elapsed_ms,
            "available_tables": tables,
        }

    def export_select(
        self,
        sql: str,
        *,
        export_format: str,
        limit: int = 50_000,
    ) -> tuple[bytes, str, int]:
        """Exporta el resultado exacto de una consulta SELECT en Parquet, CSV o JSONL."""
        result = self.execute_select(sql, limit=limit)
        columns = result["columns"]
        rows = result["rows"]
        fmt = export_format.strip().lower()

        if fmt == "jsonl":
            body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
            if body:
                body += "\n"
            return body.encode("utf-8"), "application/x-ndjson", len(rows)

        if fmt == "csv":
            sio = io.StringIO()
            writer = csv.DictWriter(sio, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
            return ("\ufeff" + sio.getvalue()).encode("utf-8"), "text/csv; charset=utf-8", len(rows)

        if fmt == "parquet":
            with tempfile.TemporaryDirectory() as tmpdir:
                json_in = Path(tmpdir) / "sql_result.json"
                pq_out = Path(tmpdir) / "sql_result.parquet"
                con = duckdb.connect(":memory:")
                try:
                    if rows:
                        json_in.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                        con.execute(
                            f"COPY (SELECT * FROM read_json_auto('{json_in.as_posix()}')) "
                            f"TO '{pq_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                        )
                    else:
                        cols_sql = ", ".join(
                            f'CAST(NULL AS VARCHAR) AS "{c.replace(chr(34), "")}"'
                            for c in (columns or ["empty"])
                        )
                        con.execute(
                            f"COPY (SELECT {cols_sql} WHERE FALSE) "
                            f"TO '{pq_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                        )
                finally:
                    con.close()
                return pq_out.read_bytes(), "application/vnd.apache.parquet", len(rows)

        raise UnsafeSqlError(f"Formato de exportación no soportado: '{export_format}'.")
