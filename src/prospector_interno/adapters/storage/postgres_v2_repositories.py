"""Repositorios PostgreSQL v2.0 (y contrapartes en memoria para tests aislados).

Implementa la capa de persistencia multi-worker segura definida en la migración
`002_raw_data_prospector_evolution.sql`:
- `PostgresGatekeeperState`: locks distribuidos con TTL en `public.gatekeeper_locks`
- `PostgresDeduplicationRepository` / `InMemoryDeduplicationRepository`:
  alias de trazabilidad (`asset_aliases`) y auditoría (`deduplication_audit`)
- `PostgresAssetProfileRepository` / `InMemoryAssetProfileRepository`:
  perfiles técnicos (`asset_profiles`) e índice de columnas (`asset_columns_catalog`)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import threading
from typing import Any
import uuid

from prospector_interno.adapters.storage.db_connection import (
    DatabaseConfig,
    get_readwrite_connection,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    AssetAliasRecord,
    DeduplicationAuditRecord,
)
from prospector_interno.domain.ports import GatekeeperStatePort


def normalize_url_sha256(canonical_url: str) -> str:
    return hashlib.sha256(canonical_url.strip().encode("utf-8")).hexdigest()


class PostgresGatekeeperState(GatekeeperStatePort):
    """Locks distribuidos con TTL en PostgreSQL 16 (`public.gatekeeper_locks`)."""

    def __init__(
        self,
        config: DatabaseConfig | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._config = config
        self._worker_id = worker_id or f"pid-{os.getpid()}"

    def try_acquire_lock(
        self,
        idempotency_key: str,
        task_id: str,
        ttl_seconds: int = 300,
    ) -> bool:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(1, ttl_seconds))
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.gatekeeper_locks (
                        idempotency_key, task_id, worker_id, acquired_at, expires_at
                    ) VALUES (%s, %s, %s, NOW(), %s)
                    ON CONFLICT (idempotency_key) DO UPDATE SET
                        task_id = EXCLUDED.task_id,
                        worker_id = EXCLUDED.worker_id,
                        acquired_at = NOW(),
                        expires_at = EXCLUDED.expires_at
                    WHERE public.gatekeeper_locks.expires_at <= NOW()
                       OR public.gatekeeper_locks.task_id = EXCLUDED.task_id
                    RETURNING task_id;
                    """,
                    (idempotency_key, task_id, self._worker_id, expires_at),
                )
                row = cur.fetchone()
            conn.commit()
        return row is not None

    def release_lock(self, idempotency_key: str, task_id: str) -> None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM public.gatekeeper_locks
                    WHERE idempotency_key = %s AND task_id = %s;
                    """,
                    (idempotency_key, task_id),
                )
            conn.commit()

    def get_in_flight_task(self, idempotency_key: str) -> str | None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT task_id FROM public.gatekeeper_locks
                    WHERE idempotency_key = %s AND expires_at > NOW();
                    """,
                    (idempotency_key,),
                )
                row = cur.fetchone()
                if row:
                    return str(row["task_id"])
                # Purga perezosa si expiró
                cur.execute(
                    """
                    DELETE FROM public.gatekeeper_locks
                    WHERE idempotency_key = %s AND expires_at <= NOW();
                    """,
                    (idempotency_key,),
                )
            conn.commit()
        return None


class InMemoryDeduplicationRepository:
    """Almacén thread-safe en memoria para alias SHA-256 y bitácora de deduplicación."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._aliases_by_url_hash: dict[str, AssetAliasRecord] = {}
        self._aliases_by_etag: dict[str, AssetAliasRecord] = {}
        self._audits: list[DeduplicationAuditRecord] = []

    def register_alias(
        self,
        *,
        canonical_url: str,
        source_id: str,
        target_asset_id: str,
        target_sha256: str,
        etag: str | None = None,
    ) -> AssetAliasRecord:
        url_hash = normalize_url_sha256(canonical_url)
        rec = AssetAliasRecord(
            alias_id=f"als_{uuid.uuid4().hex[:10]}",
            canonical_url=canonical_url.strip(),
            normalized_url_sha256=url_hash,
            source_id=source_id.strip().upper(),
            target_asset_id=target_asset_id,
            target_sha256=target_sha256.lower(),
            etag=etag,
            created_at=datetime.now(timezone.utc),
        )
        with self._lock:
            self._aliases_by_url_hash[url_hash] = rec
            if etag:
                self._aliases_by_etag[etag.strip()] = rec
        return rec

    def find_alias_by_url(self, canonical_url: str) -> AssetAliasRecord | None:
        url_hash = normalize_url_sha256(canonical_url)
        with self._lock:
            return self._aliases_by_url_hash.get(url_hash)

    def find_alias_by_etag(self, etag: str) -> AssetAliasRecord | None:
        if not etag or not etag.strip():
            return None
        with self._lock:
            return self._aliases_by_etag.get(etag.strip())

    def list_aliases(self, target_sha256: str | None = None) -> list[AssetAliasRecord]:
        with self._lock:
            items = list(self._aliases_by_url_hash.values())
        if target_sha256:
            low = target_sha256.lower()
            items = [a for a in items if a.target_sha256 == low]
        return sorted(items, key=lambda a: a.created_at, reverse=True)

    def record_audit(
        self,
        *,
        source_id: str,
        canonical_url: str,
        criterion: str,
        reused_asset_id: str | None = None,
        reused_sha256: str | None = None,
        bytes_saved: int = 0,
        detail: str | None = None,
        request_id: str | None = None,
    ) -> DeduplicationAuditRecord:
        rec = DeduplicationAuditRecord(
            audit_id=f"dedup_{uuid.uuid4().hex[:10]}",
            request_id=request_id,
            source_id=source_id.strip().upper() or "UNKNOWN",
            canonical_url=canonical_url.strip(),
            criterion=criterion,
            reused_asset_id=reused_asset_id,
            reused_sha256=reused_sha256,
            bytes_saved=max(0, int(bytes_saved)),
            detail=detail,
            detected_at=datetime.now(timezone.utc),
        )
        with self._lock:
            self._audits.append(rec)
            if len(self._audits) > 5000:
                self._audits = self._audits[-5000:]
        return rec

    def list_audit(
        self,
        *,
        limit: int = 100,
        criterion: str | None = None,
        source_id: str | None = None,
    ) -> list[DeduplicationAuditRecord]:
        with self._lock:
            items = list(reversed(self._audits))
        if criterion:
            items = [i for i in items if i.criterion.upper() == criterion.upper()]
        if source_id:
            items = [i for i in items if i.source_id.upper() == source_id.upper()]
        return items[:limit]

    def get_savings_summary(self) -> dict[str, Any]:
        with self._lock:
            items = list(self._audits)
            aliases_cnt = len(self._aliases_by_url_hash)
        by_criterion: dict[str, int] = {}
        total_bytes = 0
        for item in items:
            by_criterion[item.criterion] = by_criterion.get(item.criterion, 0) + 1
            total_bytes += item.bytes_saved
        return {
            "total_intercepted": len(items),
            "total_bytes_saved": total_bytes,
            "total_aliases": aliases_cnt,
            "by_criterion": by_criterion,
        }


class PostgresDeduplicationRepository:
    """Persistencia PostgreSQL de alias SHA-256 y bitácora de deduplicación."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def register_alias(
        self,
        *,
        canonical_url: str,
        source_id: str,
        target_asset_id: str,
        target_sha256: str,
        etag: str | None = None,
    ) -> AssetAliasRecord:
        url_hash = normalize_url_sha256(canonical_url)
        alias_id = f"als_{uuid.uuid4().hex[:10]}"
        now = datetime.now(timezone.utc)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.asset_aliases (
                        alias_id, canonical_url, normalized_url_sha256,
                        source_id, target_asset_id, target_sha256, etag, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (normalized_url_sha256, target_sha256) DO UPDATE SET
                        etag = COALESCE(EXCLUDED.etag, public.asset_aliases.etag),
                        target_asset_id = EXCLUDED.target_asset_id
                    RETURNING *;
                    """,
                    (
                        alias_id,
                        canonical_url.strip(),
                        url_hash,
                        source_id.strip().upper(),
                        target_asset_id,
                        target_sha256.lower(),
                        etag,
                        now,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        assert row is not None
        return self._row_to_alias(row)

    def find_alias_by_url(self, canonical_url: str) -> AssetAliasRecord | None:
        url_hash = normalize_url_sha256(canonical_url)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.asset_aliases
                    WHERE normalized_url_sha256 = %s
                    ORDER BY created_at DESC LIMIT 1;
                    """,
                    (url_hash,),
                )
                row = cur.fetchone()
                return self._row_to_alias(row) if row else None

    def find_alias_by_etag(self, etag: str) -> AssetAliasRecord | None:
        if not etag or not etag.strip():
            return None
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.asset_aliases
                    WHERE etag = %s
                    ORDER BY created_at DESC LIMIT 1;
                    """,
                    (etag.strip(),),
                )
                row = cur.fetchone()
                return self._row_to_alias(row) if row else None

    def list_aliases(self, target_sha256: str | None = None) -> list[AssetAliasRecord]:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                if target_sha256:
                    cur.execute(
                        "SELECT * FROM public.asset_aliases WHERE target_sha256 = %s ORDER BY created_at DESC;",
                        (target_sha256.lower(),),
                    )
                else:
                    cur.execute(
                        "SELECT * FROM public.asset_aliases ORDER BY created_at DESC LIMIT 200;"
                    )
                return [self._row_to_alias(r) for r in cur.fetchall()]

    def record_audit(
        self,
        *,
        source_id: str,
        canonical_url: str,
        criterion: str,
        reused_asset_id: str | None = None,
        reused_sha256: str | None = None,
        bytes_saved: int = 0,
        detail: str | None = None,
        request_id: str | None = None,
    ) -> DeduplicationAuditRecord:
        audit_id = f"dedup_{uuid.uuid4().hex[:10]}"
        now = datetime.now(timezone.utc)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.deduplication_audit (
                        audit_id, request_id, source_id, canonical_url, criterion,
                        reused_asset_id, reused_sha256, bytes_saved, detail, detected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        audit_id,
                        request_id,
                        source_id.strip().upper() or "UNKNOWN",
                        canonical_url.strip(),
                        criterion,
                        reused_asset_id,
                        reused_sha256,
                        max(0, int(bytes_saved)),
                        detail,
                        now,
                    ),
                )
            conn.commit()
        return DeduplicationAuditRecord(
            audit_id=audit_id,
            request_id=request_id,
            source_id=source_id.strip().upper() or "UNKNOWN",
            canonical_url=canonical_url.strip(),
            criterion=criterion,
            reused_asset_id=reused_asset_id,
            reused_sha256=reused_sha256,
            bytes_saved=max(0, int(bytes_saved)),
            detail=detail,
            detected_at=now,
        )

    def list_audit(
        self,
        *,
        limit: int = 100,
        criterion: str | None = None,
        source_id: str | None = None,
    ) -> list[DeduplicationAuditRecord]:
        sql = "SELECT * FROM public.deduplication_audit WHERE 1=1"
        params: list[Any] = []
        if criterion:
            sql += " AND UPPER(criterion) = UPPER(%s)"
            params.append(criterion)
        if source_id:
            sql += " AND UPPER(source_id) = UPPER(%s)"
            params.append(source_id)
        sql += " ORDER BY detected_at DESC LIMIT %s;"
        params.append(limit)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return [self._row_to_audit(r) for r in cur.fetchall()]

    def get_savings_summary(self) -> dict[str, Any]:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT criterion, COUNT(*) AS cnt, COALESCE(SUM(bytes_saved), 0) AS saved
                    FROM public.deduplication_audit
                    GROUP BY criterion;
                    """
                )
                rows = cur.fetchall()
                cur.execute("SELECT COUNT(*) AS cnt FROM public.asset_aliases;")
                alias_row = cur.fetchone() or {"cnt": 0}
        by_criterion: dict[str, int] = {}
        total_intercepted = 0
        total_bytes = 0
        for r in rows:
            cnt = int(r["cnt"])
            by_criterion[str(r["criterion"])] = cnt
            total_intercepted += cnt
            total_bytes += int(r["saved"] or 0)
        return {
            "total_intercepted": total_intercepted,
            "total_bytes_saved": total_bytes,
            "total_aliases": int(alias_row["cnt"]),
            "by_criterion": by_criterion,
        }

    @staticmethod
    def _row_to_alias(row: dict[str, Any]) -> AssetAliasRecord:
        return AssetAliasRecord(
            alias_id=str(row["alias_id"]),
            canonical_url=str(row["canonical_url"]),
            normalized_url_sha256=str(row["normalized_url_sha256"]).strip(),
            source_id=str(row["source_id"]),
            target_asset_id=str(row["target_asset_id"]),
            target_sha256=str(row["target_sha256"]).strip(),
            etag=row.get("etag"),
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_audit(row: dict[str, Any]) -> DeduplicationAuditRecord:
        return DeduplicationAuditRecord(
            audit_id=str(row["audit_id"]),
            request_id=row.get("request_id"),
            source_id=str(row["source_id"]),
            canonical_url=str(row["canonical_url"]),
            criterion=str(row["criterion"]),
            reused_asset_id=row.get("reused_asset_id"),
            reused_sha256=(row.get("reused_sha256") or "").strip() or None,
            bytes_saved=int(row.get("bytes_saved") or 0),
            detail=row.get("detail"),
            detected_at=row["detected_at"],
        )


class InMemoryAssetProfileRepository:
    """Almacén en memoria para perfiles de archivos brutos e índice de columnas."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._profiles: dict[str, dict[str, Any]] = {}
        self._columns: list[dict[str, Any]] = []

    def save_profile(self, profile: dict[str, Any]) -> None:
        asset_id = str(profile["asset_id"])
        with self._lock:
            self._profiles[asset_id] = dict(profile)
            self._columns = [c for c in self._columns if c.get("asset_id") != asset_id]
            for col in profile.get("columns_summary") or []:
                self._columns.append(
                    {
                        "asset_id": asset_id,
                        "source_id": profile.get("source_id", "UNKNOWN"),
                        "dataset_id": profile.get("dataset_id"),
                        "sheet_name": col.get("sheet_name") or "",
                        "column_name": str(col["column_name"]),
                        "inferred_type": str(col.get("inferred_type", "STRING")),
                        "null_pct": float(col.get("null_pct", 0.0)),
                        "distinct_count": int(col.get("distinct_count", 0)),
                        "sample_values": list(col.get("sample_values") or [])[:5],
                    }
                )

    def get_profile(self, asset_id: str) -> dict[str, Any] | None:
        with self._lock:
            prof = self._profiles.get(asset_id)
            return dict(prof) if prof else None

    def list_profiles(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(v) for v in self._profiles.values()]

    def search_columns(
        self,
        *,
        column: str | None = None,
        source_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._columns)
        if column and column.strip():
            needle = column.strip().lower()
            rows = [r for r in rows if needle in str(r["column_name"]).lower()]
        if source_id and source_id.strip():
            src_u = source_id.strip().upper()
            rows = [r for r in rows if str(r.get("source_id", "")).upper() == src_u]
        return rows[:limit]


class PostgresAssetProfileRepository:
    """Persistencia PostgreSQL para `asset_profiles` y `asset_columns_catalog`."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def save_profile(self, profile: dict[str, Any]) -> None:
        asset_id = str(profile["asset_id"])
        source_id = str(profile.get("source_id", "UNKNOWN")).upper()
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.asset_profiles (
                        asset_id, source_id, file_format, file_size_bytes, sha256_hash,
                        detected_encoding, csv_delimiter, sheet_names, row_count,
                        column_count, columns_summary, sample_rows, archive_entries, profiled_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                        %s::jsonb, %s::jsonb, %s::jsonb, NOW()
                    )
                    ON CONFLICT (asset_id) DO UPDATE SET
                        file_format = EXCLUDED.file_format,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        sha256_hash = EXCLUDED.sha256_hash,
                        detected_encoding = EXCLUDED.detected_encoding,
                        csv_delimiter = EXCLUDED.csv_delimiter,
                        sheet_names = EXCLUDED.sheet_names,
                        row_count = EXCLUDED.row_count,
                        column_count = EXCLUDED.column_count,
                        columns_summary = EXCLUDED.columns_summary,
                        sample_rows = EXCLUDED.sample_rows,
                        archive_entries = EXCLUDED.archive_entries,
                        profiled_at = NOW();
                    """,
                    (
                        asset_id,
                        source_id,
                        str(profile.get("file_format", "unknown")),
                        int(profile.get("file_size_bytes", 0)),
                        profile.get("sha256_hash"),
                        profile.get("detected_encoding"),
                        profile.get("csv_delimiter"),
                        json.dumps(profile.get("sheet_names") or [], ensure_ascii=False),
                        int(profile.get("row_count", 0)),
                        int(profile.get("column_count", 0)),
                        json.dumps(profile.get("columns_summary") or [], ensure_ascii=False),
                        json.dumps(profile.get("sample_rows") or [], ensure_ascii=False, default=str),
                        json.dumps(profile.get("archive_entries") or [], ensure_ascii=False),
                    ),
                )
                cur.execute(
                    "DELETE FROM public.asset_columns_catalog WHERE asset_id = %s;",
                    (asset_id,),
                )
                for col in profile.get("columns_summary") or []:
                    cur.execute(
                        """
                        INSERT INTO public.asset_columns_catalog (
                            asset_id, source_id, dataset_id, sheet_name, column_name,
                            inferred_type, null_pct, distinct_count, sample_values, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                        ON CONFLICT (asset_id, sheet_name, column_name) DO UPDATE SET
                            inferred_type = EXCLUDED.inferred_type,
                            null_pct = EXCLUDED.null_pct,
                            distinct_count = EXCLUDED.distinct_count,
                            sample_values = EXCLUDED.sample_values,
                            updated_at = NOW();
                        """,
                        (
                            asset_id,
                            source_id,
                            profile.get("dataset_id"),
                            str(col.get("sheet_name") or ""),
                            str(col["column_name"]),
                            str(col.get("inferred_type", "STRING")),
                            float(col.get("null_pct", 0.0)),
                            int(col.get("distinct_count", 0)),
                            json.dumps(list(col.get("sample_values") or [])[:5], default=str),
                        ),
                    )
            conn.commit()

    def get_profile(self, asset_id: str) -> dict[str, Any] | None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.asset_profiles WHERE asset_id = %s;",
                    (asset_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "asset_id": row["asset_id"],
                    "source_id": row["source_id"],
                    "file_format": row["file_format"],
                    "file_size_bytes": int(row["file_size_bytes"] or 0),
                    "sha256_hash": (row.get("sha256_hash") or "").strip() or None,
                    "detected_encoding": row.get("detected_encoding"),
                    "csv_delimiter": row.get("csv_delimiter"),
                    "sheet_names": row.get("sheet_names") or [],
                    "row_count": int(row.get("row_count") or 0),
                    "column_count": int(row.get("column_count") or 0),
                    "columns_summary": row.get("columns_summary") or [],
                    "sample_rows": row.get("sample_rows") or [],
                    "archive_entries": row.get("archive_entries") or [],
                    "profiled_at": (
                        row["profiled_at"].isoformat()
                        if hasattr(row.get("profiled_at"), "isoformat")
                        else str(row.get("profiled_at") or "")
                    ),
                }

    def list_profiles(self) -> list[dict[str, Any]]:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT asset_id FROM public.asset_profiles ORDER BY profiled_at DESC LIMIT 200;")
                ids = [r["asset_id"] for r in cur.fetchall()]
        out: list[dict[str, Any]] = []
        for aid in ids:
            p = self.get_profile(aid)
            if p:
                out.append(p)
        return out

    def search_columns(
        self,
        *,
        column: str | None = None,
        source_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM public.asset_columns_catalog WHERE 1=1"
        params: list[Any] = []
        if column and column.strip():
            sql += " AND LOWER(column_name) LIKE %s"
            params.append(f"%{column.strip().lower()}%")
        if source_id and source_id.strip():
            sql += " AND UPPER(source_id) = UPPER(%s)"
            params.append(source_id.strip())
        sql += " ORDER BY updated_at DESC LIMIT %s;"
        params.append(limit)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return [
                    {
                        "asset_id": r["asset_id"],
                        "source_id": r["source_id"],
                        "dataset_id": r.get("dataset_id"),
                        "sheet_name": r.get("sheet_name") or "",
                        "column_name": r["column_name"],
                        "inferred_type": r["inferred_type"],
                        "null_pct": float(r.get("null_pct") or 0.0),
                        "distinct_count": int(r.get("distinct_count") or 0),
                        "sample_values": r.get("sample_values") or [],
                    }
                    for r in cur.fetchall()
                ]
