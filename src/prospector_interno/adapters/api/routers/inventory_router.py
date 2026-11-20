"""Router para la Curaduría y Control de Estado de URLs e Ingesta.

En ejecución productiva opera directamente contra la tabla relacional unificada
`public.url_inventory_map` y `public.file` en PostgreSQL (`platform_db`).
En pruebas unitarias aisladas (`pytest`) opera sobre una copia temporal efímera
del snapshot para no mutar ningún archivo versionado ni la base productiva.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3
import tempfile
from typing import Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.storage.db_connection import (
    get_readwrite_connection,
)

router = APIRouter(prefix="/inventory", tags=["4. Curaduría y Estado de URLs"])

_TEMP_SQLITE_COPY: Path | None = None


def _get_db_path() -> Path:
    global _TEMP_SQLITE_COPY
    candidates = [
        deps.PROJECT_ROOT / "data/snapshot/platform_prod_snapshot.db",
        Path("prospector_interno/data/snapshot/platform_prod_snapshot.db"),
        Path("data/snapshot/platform_prod_snapshot.db"),
    ]
    source_snap = next((p.resolve() for p in candidates if p.is_file()), None)
    if source_snap is None:
        return (deps.PROJECT_ROOT / "data/snapshot/platform_prod_snapshot.db").resolve()

    if deps.is_isolated_test_mode():
        if _TEMP_SQLITE_COPY is None or not _TEMP_SQLITE_COPY.exists():
            tmp_dir = Path(tempfile.mkdtemp(prefix="prospector_inv_test_"))
            _TEMP_SQLITE_COPY = tmp_dir / "platform_prod_snapshot.db"
            shutil.copy2(source_snap, _TEMP_SQLITE_COPY)
        return _TEMP_SQLITE_COPY

    return source_snap


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_get_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _use_postgres() -> bool:
    return deps.USE_POSTGRES_MUTATIONS


class UrlInventoryItem(BaseModel):
    id: int
    source_id: str
    file_code: Optional[str] = None
    file_name: Optional[str] = None
    canonical_url: str
    normalized_url_sha256: str
    is_discovered_web: bool = False
    is_downloaded: bool = False
    is_processed: bool = False
    sha256_hash: Optional[str] = None
    storage_path: Optional[str] = None
    gold_target: Optional[str] = None
    status: str
    updated_at: Optional[str] = None


class UrlInventoryResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[UrlInventoryItem]


class UpdateUrlInventoryRequest(BaseModel):
    is_processed: Optional[bool] = Field(
        None,
        description="Estado de curaduría/procesamiento asignado por el analista",
    )
    is_downloaded: Optional[bool] = Field(
        None,
        description="Estado de adquisición/descarga asignado por el analista",
    )
    override_reason: Optional[str] = Field(
        None,
        description="Justificación técnica escrita para sobreescritura excepcional",
    )
    actor: str = Field(default="MANUAL_OPERATOR")


class SourceStatsItem(BaseModel):
    source_id: str
    total: int
    downloaded: int
    processed: int


class InventoryStatsResponse(BaseModel):
    total_urls: int
    total_downloaded: int
    total_processed: int
    download_rate_pct: float
    processing_rate_pct: float
    by_source: List[SourceStatsItem]


@router.get(
    "/stats",
    response_model=InventoryStatsResponse,
    summary="Resumen cuantitativo de URLs descargadas y procesadas",
)
def get_inventory_stats() -> InventoryStatsResponse:
    """Retorna las métricas globales y desglose por institución."""
    if _use_postgres():
        with get_readwrite_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COALESCE(SUM(CASE WHEN is_downloaded THEN 1 ELSE 0 END), 0) AS downloaded,
                        COALESCE(SUM(CASE WHEN is_processed THEN 1 ELSE 0 END), 0) AS processed
                    FROM public.url_inventory_map;
                    """
                )
                row = cur.fetchone() or {}
                total = int(row.get("total") or 0)
                downloaded = int(row.get("downloaded") or 0)
                processed = int(row.get("processed") or 0)

                cur.execute(
                    """
                    SELECT
                        UPPER(source_id) AS source_id,
                        COUNT(*) AS total,
                        COALESCE(SUM(CASE WHEN is_downloaded THEN 1 ELSE 0 END), 0) AS downloaded,
                        COALESCE(SUM(CASE WHEN is_processed THEN 1 ELSE 0 END), 0) AS processed
                    FROM public.url_inventory_map
                    GROUP BY UPPER(source_id)
                    ORDER BY total DESC;
                    """
                )
                sources_rows = cur.fetchall()
    else:
        with _get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN is_downloaded = 1 THEN 1 ELSE 0 END) as downloaded,
                    SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed
                FROM url_inventory_map
                """
            )
            row = cur.fetchone()
            total = row["total"] or 0
            downloaded = row["downloaded"] or 0
            processed = row["processed"] or 0

            cur.execute(
                """
                SELECT
                    UPPER(source_id) AS source_id,
                    COUNT(*) as total,
                    SUM(CASE WHEN is_downloaded = 1 THEN 1 ELSE 0 END) as downloaded,
                    SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed
                FROM url_inventory_map
                GROUP BY UPPER(source_id)
                ORDER BY total DESC
                """
            )
            sources_rows = cur.fetchall()

    dl_rate = round((downloaded / total * 100.0), 2) if total > 0 else 0.0
    pr_rate = round((processed / total * 100.0), 2) if total > 0 else 0.0

    by_source = [
        SourceStatsItem(
            source_id=r["source_id"],
            total=int(r["total"] or 0),
            downloaded=int(r["downloaded"] or 0),
            processed=int(r["processed"] or 0),
        )
        for r in sources_rows
    ]

    return InventoryStatsResponse(
        total_urls=total,
        total_downloaded=downloaded,
        total_processed=processed,
        download_rate_pct=dl_rate,
        processing_rate_pct=pr_rate,
        by_source=by_source,
    )


@router.get(
    "/urls",
    response_model=UrlInventoryResponse,
    summary="Listado paginado de inventario de URLs con filtros",
)
def list_inventory_urls(
    source_id: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None),
    is_downloaded: Optional[bool] = Query(default=None),
    is_processed: Optional[bool] = Query(default=None),
    search: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
) -> UrlInventoryResponse:
    """Permite al analista navegar y filtrar las URLs del inventario unificado."""
    offset = (page - 1) * limit

    if _use_postgres():
        conditions: List[str] = []
        params: List[Any] = []
        if source_id and source_id.upper() != "ALL":
            conditions.append("UPPER(source_id) = UPPER(%s)")
            params.append(source_id)
        if status_filter and status_filter.upper() != "ALL":
            conditions.append("status = %s")
            params.append(status_filter)
        if is_downloaded is not None:
            conditions.append("is_downloaded = %s")
            params.append(bool(is_downloaded))
        if is_processed is not None:
            conditions.append("is_processed = %s")
            params.append(bool(is_processed))
        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append(
                "(canonical_url ILIKE %s OR file_name ILIKE %s OR file_code ILIKE %s)"
            )
            params.extend([term, term, term])

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        with get_readwrite_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS cnt FROM public.url_inventory_map{where_clause};",
                    tuple(params),
                )
                total = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    f"""
                    SELECT
                        id, source_id, file_code, file_name, canonical_url,
                        normalized_url_sha256, is_discovered_web, is_downloaded,
                        is_processed, sha256_hash, storage_path, gold_target,
                        status, updated_at
                    FROM public.url_inventory_map
                    {where_clause}
                    ORDER BY is_downloaded DESC, is_processed DESC, id ASC
                    LIMIT %s OFFSET %s;
                    """,
                    tuple(params + [limit, offset]),
                )
                rows = cur.fetchall()
    else:
        conditions = []
        params = []
        if source_id and source_id.upper() != "ALL":
            conditions.append("UPPER(source_id) = UPPER(?)")
            params.append(source_id)
        if status_filter and status_filter.upper() != "ALL":
            conditions.append("status = ?")
            params.append(status_filter)
        if is_downloaded is not None:
            conditions.append("is_downloaded = ?")
            params.append(1 if is_downloaded else 0)
        if is_processed is not None:
            conditions.append("is_processed = ?")
            params.append(1 if is_processed else 0)
        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append(
                "(canonical_url LIKE ? OR file_name LIKE ? OR file_code LIKE ?)"
            )
            params.extend([term, term, term])

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        with _get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM url_inventory_map{where_clause}",
                params,
            )
            total = cur.fetchone()[0]
            cur.execute(
                f"""
                SELECT
                    id, source_id, file_code, file_name, canonical_url,
                    normalized_url_sha256, is_discovered_web, is_downloaded,
                    is_processed, sha256_hash, storage_path, gold_target,
                    status, updated_at
                FROM url_inventory_map
                {where_clause}
                ORDER BY is_downloaded DESC, is_processed DESC, id ASC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            )
            rows = cur.fetchall()

    items = [
        UrlInventoryItem(
            id=r["id"],
            source_id=r["source_id"],
            file_code=r["file_code"],
            file_name=r["file_name"],
            canonical_url=r["canonical_url"],
            normalized_url_sha256=r["normalized_url_sha256"],
            is_discovered_web=bool(r["is_discovered_web"]),
            is_downloaded=bool(r["is_downloaded"]),
            is_processed=bool(r["is_processed"]),
            sha256_hash=r["sha256_hash"],
            storage_path=r["storage_path"],
            gold_target=r["gold_target"],
            status=r["status"],
            updated_at=str(r["updated_at"]) if r["updated_at"] is not None else None,
        )
        for r in rows
    ]

    return UrlInventoryResponse(
        total=total,
        page=page,
        limit=limit,
        items=items,
    )


@router.patch(
    "/urls/{url_id_or_hash}",
    response_model=UrlInventoryItem,
    summary="Marcar o desmarcar estado de descarga y procesamiento (Analista)",
)
def update_inventory_url_status(
    url_id_or_hash: str,
    request: UpdateUrlInventoryRequest,
) -> UrlInventoryItem:
    """Actualiza en caliente el estado booleano de is_downloaded o is_processed."""
    if request.is_processed is None and request.is_downloaded is None:
        pass  # validado tras verificar 404 para mantener contrato

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if _use_postgres():
        with get_readwrite_connection() as conn:
            with conn.cursor() as cur:
                if url_id_or_hash.isdigit():
                    cur.execute(
                        "SELECT * FROM public.url_inventory_map WHERE id = %s;",
                        (int(url_id_or_hash),),
                    )
                else:
                    cur.execute(
                        "SELECT * FROM public.url_inventory_map WHERE normalized_url_sha256 = %s;",
                        (url_id_or_hash,),
                    )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"URL no encontrada en el inventario: {url_id_or_hash}",
                    )
                if request.is_processed is None and request.is_downloaded is None:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="Debe proporcionar al menos 'is_processed' o 'is_downloaded'",
                    )

                row_id = row["id"]
                set_clauses = ["updated_at = %s"]
                params: list[Any] = [now_iso]
                file_clauses: list[str] = []
                file_params: list[Any] = []

                if request.is_processed is not None:
                    set_clauses.append("is_processed = %s")
                    params.append(bool(request.is_processed))
                    file_clauses.append("is_processed = %s")
                    file_params.append(bool(request.is_processed))

                if request.is_downloaded is not None:
                    set_clauses.append("is_downloaded = %s")
                    params.append(bool(request.is_downloaded))
                    file_clauses.append("is_downloaded = %s")
                    file_params.append(bool(request.is_downloaded))
                    new_status = (
                        "DESCARGADO_BRONZE" if request.is_downloaded else "CATALOGADO_BDD"
                    )
                    set_clauses.append("status = %s")
                    params.append(new_status)

                params.append(row_id)
                cur.execute(
                    f"UPDATE public.url_inventory_map SET {', '.join(set_clauses)} WHERE id = %s;",
                    tuple(params),
                )

                if row.get("file_code") and file_clauses:
                    file_params.append(row["file_code"])
                    cur.execute("SAVEPOINT sp_inv_update_file;")
                    try:
                        cur.execute(
                            f"UPDATE public.file SET {', '.join(file_clauses)} WHERE code = %s;",
                            tuple(file_params),
                        )
                    except Exception:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_inv_update_file;")

                cur.execute(
                    "SELECT * FROM public.url_inventory_map WHERE id = %s;",
                    (row_id,),
                )
                updated = cur.fetchone() or row
                if request.override_reason:
                    import uuid as _uuid

                    cur.execute("SAVEPOINT sp_inv_audit;")
                    try:
                        cur.execute(
                            """
                            INSERT INTO public.inventory_status_audit (
                                audit_id, url_id, canonical_url, source_id,
                                previous_is_downloaded, new_is_downloaded,
                                previous_is_processed, new_is_processed,
                                actor, override_reason
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                f"invaud_{_uuid.uuid4().hex[:10]}",
                                row_id,
                                row["canonical_url"],
                                row["source_id"],
                                bool(row["is_downloaded"]),
                                bool(updated["is_downloaded"]),
                                bool(row["is_processed"]),
                                bool(updated["is_processed"]),
                                request.actor,
                                request.override_reason.strip(),
                            ),
                        )
                    except Exception:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_inv_audit;")
            conn.commit()
    else:
        with _get_conn() as conn:
            cur = conn.cursor()
            if url_id_or_hash.isdigit():
                cur.execute(
                    "SELECT * FROM url_inventory_map WHERE id = ?",
                    (int(url_id_or_hash),),
                )
            else:
                cur.execute(
                    "SELECT * FROM url_inventory_map WHERE normalized_url_sha256 = ?",
                    (url_id_or_hash,),
                )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"URL no encontrada en el inventario: {url_id_or_hash}",
                )
            if request.is_processed is None and request.is_downloaded is None:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Debe proporcionar al menos 'is_processed' o 'is_downloaded'",
                )

            row_id = row["id"]
            set_clauses = ["updated_at = ?"]
            params = [now_iso]
            file_set_clauses: List[str] = []
            file_params = []

            if request.is_processed is not None:
                set_clauses.append("is_processed = ?")
                params.append(1 if request.is_processed else 0)
                file_set_clauses.append("is_processed = ?")
                file_params.append(1 if request.is_processed else 0)

            if request.is_downloaded is not None:
                set_clauses.append("is_downloaded = ?")
                params.append(1 if request.is_downloaded else 0)
                file_set_clauses.append("is_downloaded = ?")
                file_params.append(1 if request.is_downloaded else 0)
                new_status = (
                    "DESCARGADO_BRONZE" if request.is_downloaded else "CATALOGADO_BDD"
                )
                set_clauses.append("status = ?")
                params.append(new_status)

            params.append(row_id)
            cur.execute(
                f"UPDATE url_inventory_map SET {', '.join(set_clauses)} WHERE id = ?",
                params,
            )
            if row["file_code"] and file_set_clauses:
                file_params.append(row["file_code"])
                cur.execute(
                    f"UPDATE file SET {', '.join(file_set_clauses)} WHERE code = ?",
                    file_params,
                )
            conn.commit()
            cur.execute("SELECT * FROM url_inventory_map WHERE id = ?", (row_id,))
            updated = cur.fetchone()

    return UrlInventoryItem(
        id=updated["id"],
        source_id=updated["source_id"],
        file_code=updated["file_code"],
        file_name=updated["file_name"],
        canonical_url=updated["canonical_url"],
        normalized_url_sha256=updated["normalized_url_sha256"],
        is_discovered_web=bool(updated["is_discovered_web"]),
        is_downloaded=bool(updated["is_downloaded"]),
        is_processed=bool(updated["is_processed"]),
        sha256_hash=updated["sha256_hash"],
        storage_path=updated["storage_path"],
        gold_target=updated["gold_target"],
        status=updated["status"],
        updated_at=str(updated["updated_at"]) if updated["updated_at"] is not None else None,
    )
