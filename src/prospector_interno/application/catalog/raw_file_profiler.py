"""Perfilador Técnico Automático de Archivos en Bruto (Objetivo 3 — Tarea 4.2).

Extrae metadatos estructurales en streaming/acotado sobre binarios en custodia
sin alterar el archivo original ni aplicar fórmulas analíticas de negocio:
- `.csv` / `.tsv`: encoding, delimitador, encabezados, tipos, % nulos, cardinalidad, 20 filas
- `.xlsx`: nombres de todas las hojas (`openpyxl read_only=True`), columnas, tipos, 20 filas
- `.xls`: detección de cabecera OLE2/BIFF y resumen estructural seguro
- `.parquet`: esquema real vía DuckDB efímero in-memory, % nulos, cardinalidad, 20 filas
- `.json` / `.jsonl`: claves, tipos inferidos, % nulos, cardinalidad, 20 filas
- `.zip`: inventario de archivos internos y perfilado opcional del primer tabular
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
from typing import Any
import zipfile

import duckdb
import openpyxl


DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}(?::\d{2})?)?$")
INT_RE = re.compile(r"^[+-]?\d+$")
FLOAT_RE = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?$")


def _infer_scalar_type(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, datetime):
        return "DATETIME"
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nan", "na", "n/a", "-"}:
        return "NULL"
    if text.lower() in {"true", "false"}:
        return "BOOLEAN"
    if INT_RE.match(text):
        return "INTEGER"
    norm_num = text.replace(",", ".") if text.count(",") == 1 and "." not in text else text
    if FLOAT_RE.match(norm_num):
        return "FLOAT"
    if DATE_ISO_RE.match(text):
        return "DATE"
    return "STRING"


def _merge_column_types(observed: set[str]) -> str:
    non_null = {t for t in observed if t != "NULL"}
    if not non_null:
        return "STRING"
    if len(non_null) == 1:
        return next(iter(non_null))
    if non_null <= {"INTEGER", "FLOAT"}:
        return "FLOAT"
    if non_null <= {"DATE", "DATETIME"}:
        return "DATETIME"
    return "STRING"


def _compute_file_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while chunk := fh.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest().lower()


def _detect_format(file_path: Path, filename_hint: str | None = None) -> str:
    ext = Path(filename_hint or file_path.name).suffix.lower().lstrip(".")
    with open(file_path, "rb") as fh:
        magic = fh.read(512)

    if magic.startswith(b"PAR1"):
        return "parquet"
    if magic.startswith(b"%PDF-"):
        return "pdf"
    if magic.startswith(b"\xd0\xcf\x11\xe0"):
        return "xls"
    if magic.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                names = set(zf.namelist())
                if "xl/workbook.xml" in names or any(n.startswith("xl/") for n in names):
                    return "xlsx"
        except zipfile.BadZipFile:
            pass
        return "zip"

    stripped = magic.lstrip()
    if stripped.startswith((b"{", b"[")) and ext in ("", "dat", "json", "jsonl", "ndjson"):
        return "json"
    if ext in {"csv", "tsv", "xlsx", "xls", "parquet", "json", "jsonl", "zip", "pdf", "txt"}:
        return ext
    return "csv"


class RawFileProfilerService:
    """Perfila archivos en bruto con memoria acotada sin modificar el binario en disco."""

    def __init__(
        self,
        *,
        profile_repo: Any = None,
        max_scan_rows: int = 10_000,
        sample_rows_limit: int = 20,
    ) -> None:
        self.profile_repo = profile_repo
        self.max_scan_rows = max_scan_rows
        self.sample_rows_limit = sample_rows_limit

    def profile_file(
        self,
        *,
        asset_id: str,
        file_path: Path,
        source_id: str = "UNKNOWN",
        dataset_id: str | None = None,
        sha256_hash: str | None = None,
        filename_hint: str | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        """Inspecciona el archivo bruto y opcionalmente persiste perfil e índice de columnas."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Archivo bruto no encontrado en disco: {path}")

        file_size = path.stat().st_size
        computed_sha = sha256_hash or _compute_file_sha256(path)
        fmt = _detect_format(path, filename_hint=filename_hint)

        profile: dict[str, Any] = {
            "asset_id": asset_id,
            "source_id": source_id.upper(),
            "dataset_id": dataset_id,
            "file_format": fmt,
            "file_size_bytes": file_size,
            "sha256_hash": computed_sha,
            "detected_encoding": None,
            "csv_delimiter": None,
            "sheet_names": [],
            "row_count": 0,
            "column_count": 0,
            "columns_summary": [],
            "sample_rows": [],
            "archive_entries": [],
            "profiled_at": datetime.now(timezone.utc).isoformat(),
        }

        if fmt in ("csv", "tsv", "txt"):
            self._profile_csv(path, profile, forced_delimiter="\t" if fmt == "tsv" else None)
        elif fmt == "xlsx":
            self._profile_xlsx(path, profile)
        elif fmt == "xls":
            self._profile_xls(path, profile)
        elif fmt == "parquet":
            self._profile_parquet(path, profile)
        elif fmt in ("json", "jsonl"):
            self._profile_json(path, profile)
        elif fmt == "zip":
            self._profile_zip(path, profile)

        if persist and self.profile_repo is not None:
            self.profile_repo.save_profile(profile)
        return profile

    def _summarize_rows(
        self,
        headers: list[str],
        rows_iter: Any,
        *,
        sheet_name: str = "",
    ) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
        clean_headers = [
            (str(h).strip() if h is not None and str(h).strip() else f"col_{idx + 1}")
            for idx, h in enumerate(headers)
        ]
        n_cols = len(clean_headers)
        null_counts = [0] * n_cols
        type_sets: list[set[str]] = [set() for _ in range(n_cols)]
        distinct_sets: list[set[str]] = [set() for _ in range(n_cols)]
        samples_per_col: list[list[Any]] = [[] for _ in range(n_cols)]
        sample_rows: list[dict[str, Any]] = []
        total_rows = 0
        scanned_rows = 0

        for row in rows_iter:
            total_rows += 1
            if scanned_rows < self.max_scan_rows:
                scanned_rows += 1
                row_dict: dict[str, Any] = {}
                for i in range(n_cols):
                    val = row[i] if i < len(row) else None
                    t_kind = _infer_scalar_type(val)
                    if t_kind == "NULL":
                        null_counts[i] += 1
                    else:
                        type_sets[i].add(t_kind)
                        val_str = str(val)
                        if len(distinct_sets[i]) < 5000:
                            distinct_sets[i].add(val_str)
                        if len(samples_per_col[i]) < 5 and val_str not in samples_per_col[i]:
                            samples_per_col[i].append(val_str)
                    if len(sample_rows) < self.sample_rows_limit:
                        row_dict[clean_headers[i]] = (
                            val.isoformat() if isinstance(val, datetime) else val
                        )
                if len(sample_rows) < self.sample_rows_limit:
                    sample_rows.append(row_dict)

        denom = max(1, scanned_rows)
        cols_summary = [
            {
                "column_name": clean_headers[i],
                "sheet_name": sheet_name,
                "inferred_type": _merge_column_types(type_sets[i]),
                "null_pct": round((null_counts[i] / denom) * 100.0, 2),
                "distinct_count": len(distinct_sets[i]),
                "sample_values": samples_per_col[i],
            }
            for i in range(n_cols)
        ]
        return total_rows, cols_summary, sample_rows

    def _profile_csv(
        self,
        path: Path,
        profile: dict[str, Any],
        *,
        forced_delimiter: str | None = None,
    ) -> None:
        raw_head = path.read_bytes()[:65536]
        encoding = "utf-8-sig"
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                raw_head.decode(enc)
                encoding = enc
                break
            except UnicodeDecodeError:
                continue

        sample_text = raw_head.decode(encoding, errors="replace")
        delimiter = forced_delimiter
        if delimiter is None:
            try:
                dialect = csv.Sniffer().sniff(sample_text, delimiters=",;\t|")
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = ";" if sample_text.count(";") > sample_text.count(",") else ","

        profile["detected_encoding"] = encoding
        profile["csv_delimiter"] = delimiter

        with open(path, "r", encoding=encoding, errors="replace", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            headers = next(reader, [])
            if not headers:
                return
            total_rows, cols_summary, sample_rows = self._summarize_rows(headers, reader)

        profile["row_count"] = total_rows
        profile["column_count"] = len(cols_summary)
        profile["columns_summary"] = cols_summary
        profile["sample_rows"] = sample_rows

    def _profile_xlsx(self, path: Path, profile: dict[str, Any]) -> None:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            sheet_names = list(wb.sheetnames)
            profile["sheet_names"] = sheet_names
            if not sheet_names:
                return
            primary = wb[sheet_names[0]]
            row_iter = primary.iter_rows(values_only=True)
            headers: list[Any] = []
            for candidate_row in row_iter:
                if any(cell is not None and str(cell).strip() for cell in candidate_row):
                    headers = list(candidate_row)
                    break
            if not headers:
                return
            total_rows, cols_summary, sample_rows = self._summarize_rows(
                [str(h) if h is not None else "" for h in headers],
                row_iter,
                sheet_name=sheet_names[0],
            )
            profile["row_count"] = total_rows
            profile["column_count"] = len(cols_summary)
            profile["columns_summary"] = cols_summary
            profile["sample_rows"] = sample_rows
        finally:
            wb.close()

    def _profile_xls(self, path: Path, profile: dict[str, Any]) -> None:
        profile["detected_encoding"] = "binary/ole2-biff8"
        try:
            import xlrd  # type: ignore[import-not-found]

            book = xlrd.open_workbook(str(path), on_demand=True)
            try:
                profile["sheet_names"] = list(book.sheet_names())
                if book.nsheets > 0:
                    sheet = book.sheet_by_index(0)
                    if sheet.nrows > 0:
                        headers = [str(sheet.cell_value(0, c)) for c in range(sheet.ncols)]
                        rows = (
                            [sheet.cell_value(r, c) for c in range(sheet.ncols)]
                            for r in range(1, sheet.nrows)
                        )
                        total_rows, cols_summary, sample_rows = self._summarize_rows(
                            headers, rows, sheet_name=sheet.name
                        )
                        profile["row_count"] = total_rows
                        profile["column_count"] = len(cols_summary)
                        profile["columns_summary"] = cols_summary
                        profile["sample_rows"] = sample_rows
            finally:
                book.release_resources()
        except Exception:
            profile["sheet_names"] = ["Hoja_OLE2_BIFF"]

    def _profile_parquet(self, path: Path, profile: dict[str, Any]) -> None:
        con = duckdb.connect(":memory:")
        try:
            escaped = path.resolve().as_posix().replace("'", "''")
            desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{escaped}')").fetchall()
            count_row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped}')").fetchone()
            total_rows = int(count_row[0]) if count_row else 0
            col_names = [str(r[0]) for r in desc]
            col_types = {str(r[0]): str(r[1]) for r in desc}

            cols_summary: list[dict[str, Any]] = []
            for cname in col_names:
                qcol = '"' + cname.replace('"', '""') + '"'
                stat = con.execute(
                    f"SELECT COUNT(*) FILTER (WHERE {qcol} IS NULL), "
                    f"COUNT(DISTINCT {qcol}) FROM read_parquet('{escaped}')"
                ).fetchone()
                nulls = int(stat[0]) if stat else 0
                distinct = int(stat[1]) if stat else 0
                sample_vals = [
                    str(r[0])
                    for r in con.execute(
                        f"SELECT DISTINCT {qcol} FROM read_parquet('{escaped}') "
                        f"WHERE {qcol} IS NOT NULL LIMIT 5"
                    ).fetchall()
                ]
                cols_summary.append(
                    {
                        "column_name": cname,
                        "sheet_name": "",
                        "inferred_type": col_types.get(cname, "VARCHAR"),
                        "null_pct": round((nulls / max(1, total_rows)) * 100.0, 2),
                        "distinct_count": distinct,
                        "sample_values": sample_vals,
                    }
                )

            sample_tuples = con.execute(
                f"SELECT * FROM read_parquet('{escaped}') LIMIT {self.sample_rows_limit}"
            ).fetchall()
            sample_rows = [
                {
                    col_names[idx]: (
                        val.isoformat() if isinstance(val, datetime) else val
                    )
                    for idx, val in enumerate(row)
                }
                for row in sample_tuples
            ]
            profile["row_count"] = total_rows
            profile["column_count"] = len(cols_summary)
            profile["columns_summary"] = cols_summary
            profile["sample_rows"] = sample_rows
        finally:
            con.close()

    def _profile_json(self, path: Path, profile: dict[str, Any]) -> None:
        profile["detected_encoding"] = "utf-8"
        raw_text = path.read_text(encoding="utf-8", errors="replace")
        records: list[dict[str, Any]] = []
        try:
            parsed = json.loads(raw_text)
            if isinstance(parsed, list):
                records = [r for r in parsed if isinstance(r, dict)]
            elif isinstance(parsed, dict):
                list_val = next((v for v in parsed.values() if isinstance(v, list)), None)
                if list_val and all(isinstance(x, dict) for x in list_val[:5]):
                    records = [x for x in list_val if isinstance(x, dict)]
                else:
                    records = [parsed]
        except json.JSONDecodeError:
            for line in raw_text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        records.append(obj)
                except json.JSONDecodeError:
                    continue

        if not records:
            return
        headers = sorted({k for r in records[:200] for k in r.keys()})
        rows_iter = ([r.get(h) for h in headers] for r in records)
        total_rows, cols_summary, sample_rows = self._summarize_rows(headers, rows_iter)
        profile["row_count"] = total_rows
        profile["column_count"] = len(cols_summary)
        profile["columns_summary"] = cols_summary
        profile["sample_rows"] = sample_rows

    def _profile_zip(self, path: Path, profile: dict[str, Any]) -> None:
        entries: list[dict[str, Any]] = []
        with zipfile.ZipFile(path, "r") as zf:
            for info in zf.infolist()[:200]:
                if info.is_dir():
                    continue
                entries.append(
                    {
                        "filename": info.filename,
                        "size_bytes": info.file_size,
                        "compressed_bytes": info.compress_size,
                        "extension": Path(info.filename).suffix.lower(),
                    }
                )
            profile["archive_entries"] = entries

            # Perfilar el primer archivo CSV interno pequeño si existe
            first_csv = next(
                (e for e in entries if e["extension"] == ".csv" and e["size_bytes"] <= 5_000_000),
                None,
            )
            if first_csv is not None:
                data = zf.read(first_csv["filename"]).decode("utf-8-sig", errors="replace")
                reader = csv.reader(io.StringIO(data))
                headers = next(reader, [])
                if headers:
                    total_rows, cols_summary, sample_rows = self._summarize_rows(
                        headers, reader, sheet_name=first_csv["filename"]
                    )
                    profile["row_count"] = total_rows
                    profile["column_count"] = len(cols_summary)
                    profile["columns_summary"] = cols_summary
                    profile["sample_rows"] = sample_rows
