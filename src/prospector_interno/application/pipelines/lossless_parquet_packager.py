"""Empaquetador Columnar Lossless Parquet estilo Hive con Detección de Schema Drift (Tarea 4.3).

Consolida archivos tabulares en bruto hacia particiones Hive Parquet (`ZSTD`)
de forma atómica (`.tmp` -> `os.replace`) SIN aplicar cálculos de negocio ni
imputaciones ocultas, validando compatibilidad de esquema contra particiones
existentes antes de escribir para bloquear `SCHEMA_DRIFT` sin corromper datos.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any
import uuid

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from prospector_interno.domain.contracts.lineage_spec import (
    DataLineageRecord,
    InternalAssetRecord,
    StorageLayer,
)


class SchemaDriftError(Exception):
    """Error bloqueante lanzado cuando un insumo altera incompatiblemente el esquema Gold."""

    def __init__(
        self,
        *,
        dataset_id: str,
        expected_schema_hash: str,
        observed_schema_hash: str,
        added_columns: list[str],
        removed_columns: list[str],
        type_changes: list[str],
    ) -> None:
        self.code = "SCHEMA_DRIFT"
        self.dataset_id = dataset_id
        self.expected_schema_hash = expected_schema_hash
        self.observed_schema_hash = observed_schema_hash
        self.added_columns = added_columns
        self.removed_columns = removed_columns
        self.type_changes = type_changes
        super().__init__(
            f"SCHEMA_DRIFT bloqueado en '{dataset_id}': "
            f"agregadas={added_columns}, eliminadas={removed_columns}, "
            f"cambios_tipo={type_changes}."
        )


class PartitionPackResult(BaseModel):
    """Estadísticas físicas y criptográficas de una partición Parquet lossless."""

    model_config = ConfigDict(frozen=True)

    asset_id: str
    dataset_id: str
    source_id: str
    gestion: int
    mes: int
    file_path: str
    sha256_hash: str
    source_sha256: str
    row_count: int
    raw_bytes: int
    parquet_bytes: int
    compression_ratio: float
    schema_hash: str
    schema_version: int = 1
    columns: list[dict[str, str]] = Field(default_factory=list)


def _compute_schema_hash(columns: list[tuple[str, str]]) -> str:
    canonical = json.dumps(
        sorted((c.strip().lower(), t.strip().upper()) for c, t in columns),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest().lower()


class LosslessParquetPackager:
    """Consolida archivos crudos a Hive Parquet ZSTD atómico con control estricto de esquema."""

    def __init__(
        self,
        *,
        base_gold_path: Path,
        lineage_adapter: Any = None,
    ) -> None:
        self.base_gold_path = Path(base_gold_path)
        self.lineage_adapter = lineage_adapter

    def _schema_file(self, dataset_id: str) -> Path:
        return self.base_gold_path / dataset_id / "_schema_baseline.json"

    def _load_existing_schema(self, dataset_id: str) -> dict[str, Any] | None:
        schema_file = self._schema_file(dataset_id)
        if schema_file.is_file():
            try:
                return json.loads(schema_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return None

        # Respaldo: leer esquema real desde la primera partición Parquet existente
        dataset_dir = self.base_gold_path / dataset_id
        existing_pqs = sorted(dataset_dir.glob("**/*.parquet")) if dataset_dir.is_dir() else []
        if not existing_pqs:
            return None
        con = duckdb.connect(":memory:")
        try:
            esc = existing_pqs[0].resolve().as_posix().replace("'", "''")
            desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{esc}')").fetchall()
            cols = [(str(r[0]), str(r[1])) for r in desc]
            return {
                "schema_version": 1,
                "schema_hash": _compute_schema_hash(cols),
                "columns": [{"name": c, "type": t} for c, t in cols],
            }
        finally:
            con.close()

    def _save_schema_baseline(
        self,
        dataset_id: str,
        *,
        schema_hash: str,
        columns: list[tuple[str, str]],
        schema_version: int = 1,
    ) -> None:
        schema_file = self._schema_file(dataset_id)
        schema_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = schema_file.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "schema_version": schema_version,
                    "schema_hash": schema_hash,
                    "columns": [{"name": c, "type": t} for c, t in columns],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        os.replace(tmp, schema_file)

    @staticmethod
    def _compare_schemas(
        dataset_id: str,
        existing: dict[str, Any],
        incoming_cols: list[tuple[str, str]],
    ) -> None:
        old_map = {
            str(c["name"]).strip().lower(): str(c["type"]).strip().upper()
            for c in existing.get("columns", [])
        }
        new_map = {c.strip().lower(): t.strip().upper() for c, t in incoming_cols}
        incoming_hash = _compute_schema_hash(incoming_cols)
        expected_hash = str(existing.get("schema_hash") or "")

        added = sorted(set(new_map) - set(old_map))
        removed = sorted(set(old_map) - set(new_map))
        type_changes = [
            f"{col}: {old_map[col]} -> {new_map[col]}"
            for col in sorted(set(old_map) & set(new_map))
            if old_map[col] != new_map[col]
        ]
        if added or removed or type_changes:
            raise SchemaDriftError(
                dataset_id=dataset_id,
                expected_schema_hash=expected_hash,
                observed_schema_hash=incoming_hash,
                added_columns=added,
                removed_columns=removed,
                type_changes=type_changes,
            )

    def pack_raw_file(
        self,
        *,
        raw_file_path: Path,
        dataset_id: str,
        source_id: str,
        gestion: int,
        mes: int,
        file_format: str | None = None,
        external_run_id: str = "raw_pack_v2",
        allow_schema_evolution: bool = False,
    ) -> PartitionPackResult:
        """Empaqueta un archivo CSV/JSON/Parquet crudo a partición Hive Parquet lossless."""
        raw_path = Path(raw_file_path)
        if not raw_path.is_file():
            raise FileNotFoundError(f"Archivo fuente inexistente: {raw_path}")

        ds_clean = dataset_id.strip().lower()
        raw_bytes = raw_path.stat().st_size
        source_sha = _sha256_file(raw_path)
        fmt = (file_format or raw_path.suffix.lstrip(".")).lower()

        part_dir = self.base_gold_path / ds_clean / f"gestion={gestion:04d}" / f"mes={mes:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        final_pq = part_dir / "part-000.parquet"
        tmp_pq = part_dir / f".part-{uuid.uuid4().hex[:8]}.parquet.tmp"

        con = duckdb.connect(":memory:")
        try:
            esc = raw_path.resolve().as_posix().replace("'", "''")
            if fmt in ("csv", "tsv", "txt"):
                con.execute(
                    f"CREATE TEMP TABLE raw_in AS SELECT * FROM read_csv_auto('{esc}', header=true)"
                )
            elif fmt in ("json", "jsonl", "ndjson"):
                con.execute(
                    f"CREATE TEMP TABLE raw_in AS SELECT * FROM read_json_auto('{esc}')"
                )
            elif fmt == "parquet":
                con.execute(
                    f"CREATE TEMP TABLE raw_in AS SELECT * FROM read_parquet('{esc}')"
                )
            else:
                raise ValueError(f"Formato no soportado para empaquetado lossless: {fmt}")

            desc = con.execute("DESCRIBE raw_in").fetchall()
            incoming_cols = [(str(r[0]), str(r[1])) for r in desc]
            schema_hash = _compute_schema_hash(incoming_cols)

            existing_schema = self._load_existing_schema(ds_clean)
            schema_version = 1
            if existing_schema is not None:
                if not allow_schema_evolution:
                    self._compare_schemas(ds_clean, existing_schema, incoming_cols)
                    schema_version = int(existing_schema.get("schema_version") or 1)
                else:
                    schema_version = int(existing_schema.get("schema_version") or 1) + 1

            row_cnt_res = con.execute("SELECT COUNT(*) FROM raw_in").fetchone()
            row_count = int(row_cnt_res[0]) if row_cnt_res else 0

            # Escritura atómica sin alterar valores crudos (ZSTD lossless)
            tmp_esc = tmp_pq.resolve().as_posix().replace("'", "''")
            con.execute(
                f"COPY raw_in TO '{tmp_esc}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            os.replace(tmp_pq, final_pq)
        finally:
            con.close()
            if tmp_pq.exists():
                tmp_pq.unlink(missing_ok=True)

        if existing_schema is None or allow_schema_evolution:
            self._save_schema_baseline(
                ds_clean,
                schema_hash=schema_hash,
                columns=incoming_cols,
                schema_version=schema_version,
            )

        pq_bytes = final_pq.stat().st_size
        pq_sha = _sha256_file(final_pq)
        ratio = round(raw_bytes / max(1, pq_bytes), 3)
        asset_id = f"gold_{ds_clean}_{gestion:04d}_{mes:02d}"

        if self.lineage_adapter is not None:
            now_utc = datetime.now(timezone.utc)
            period_str = f"{gestion:04d}-{mes:02d}-01"
            self.lineage_adapter.record_asset(
                InternalAssetRecord(
                    asset_id=asset_id,
                    source_id=source_id.upper(),
                    dataset_id=ds_clean,
                    period_start=period_str,
                    period_end=period_str,
                    storage_layer=StorageLayer.GOLD,
                    file_path=str(final_pq),
                    content_hash_sha256=pq_sha,
                    row_count=row_count,
                    size_bytes=pq_bytes,
                    created_at=now_utc,
                    last_updated_at=now_utc,
                )
            )
            self.lineage_adapter.record_lineage(
                DataLineageRecord(
                    lineage_id=f"lin_{uuid.uuid4().hex[:12]}",
                    gold_asset_id=asset_id,
                    bronze_source_file=str(raw_path),
                    bronze_file_sha256=source_sha,
                    external_run_id=external_run_id,
                    pipeline_version=f"lossless_parquet_v{schema_version}",
                    transformed_at=now_utc,
                )
            )

        return PartitionPackResult(
            asset_id=asset_id,
            dataset_id=ds_clean,
            source_id=source_id.upper(),
            gestion=gestion,
            mes=mes,
            file_path=str(final_pq),
            sha256_hash=pq_sha,
            source_sha256=source_sha,
            row_count=row_count,
            raw_bytes=raw_bytes,
            parquet_bytes=pq_bytes,
            compression_ratio=ratio,
            schema_hash=schema_hash,
            schema_version=schema_version,
            columns=[{"name": c, "type": t} for c, t in incoming_cols],
        )
