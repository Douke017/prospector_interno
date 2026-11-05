"""Router para la Curaduría y Control de Estado de URLs e Ingesta (HUI-03, HUI-15).

Permite a los analistas revisar, filtrar y marcar/desmarcar interactivamente
el estado de descarga (is_downloaded) y procesamiento (is_processed) de los
3,574 URLs e inventario técnico en la base de datos de producción.
"""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/inventory", tags=["4. Curaduría y Estado de URLs"])


def _get_db_path() -> Path:
    candidates = [
        Path("prospector_interno/data/snapshot/platform_prod_snapshot.db"),
        Path("data/snapshot/platform_prod_snapshot.db"),
        Path(__file__).resolve().parents[4] / "data/snapshot/platform_prod_snapshot.db",
        Path(__file__).resolve().parents[5] / "prospector_interno/data/snapshot/platform_prod_snapshot.db",
    ]
    for p in candidates:
        if p.is_file():
            return p.resolve()
    # Fallback predeterminado relativo
    return Path("prospector_interno/data/snapshot/platform_prod_snapshot.db").resolve()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_get_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# --- Modelos Pydantic ---

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
    is_processed: bool = Field(..., description="Estado de curaduría/procesamiento asignado por el analista")


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


# --- Endpoints ---

@router.get(
    "/stats",
    response_model=InventoryStatsResponse,
    summary="Resumen cuantitativo de URLs descargadas y procesadas",
)
def get_inventory_stats() -> InventoryStatsResponse:
    """Retorna las métricas globales y desglose por institución."""
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
                source_id,
                COUNT(*) as total,
                SUM(CASE WHEN is_downloaded = 1 THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed
            FROM url_inventory_map
            GROUP BY source_id
            ORDER BY total DESC
            """
        )
        sources_rows = cur.fetchall()

    dl_rate = round((downloaded / total * 100.0), 2) if total > 0 else 0.0
    pr_rate = round((processed / total * 100.0), 2) if total > 0 else 0.0

    by_source = [
        SourceStatsItem(
            source_id=r["source_id"],
            total=r["total"],
            downloaded=r["downloaded"],
            processed=r["processed"],
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
    source_id: Optional[str] = Query(default=None, description="Filtrar por código de fuente (ej. 'BBV', 'BCB')"),
    status_filter: Optional[str] = Query(default=None, description="Filtrar por status ('DISPONIBLE_WEB_NUEVO', 'CATALOGADO_BDD', etc.)"),
    is_downloaded: Optional[bool] = Query(default=None, description="Filtrar por estado de descarga (true/false)"),
    is_processed: Optional[bool] = Query(default=None, description="Filtrar por estado de procesamiento (true/false)"),
    search: Optional[str] = Query(default=None, description="Término de búsqueda en URL o nombre de archivo"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
) -> UrlInventoryResponse:
    """Permite al analista navegar y filtrar las URLs de la base de datos."""
    conditions: List[str] = []
    params: List[Any] = []

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
        conditions.append("(canonical_url LIKE ? OR file_name LIKE ? OR file_code LIKE ?)")
        params.extend([term, term, term])

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * limit

    with _get_conn() as conn:
        cur = conn.cursor()
        count_query = f"SELECT COUNT(*) FROM url_inventory_map{where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()[0]

        data_query = f"""
            SELECT 
                id, source_id, file_code, file_name, canonical_url,
                normalized_url_sha256, is_discovered_web, is_downloaded,
                is_processed, sha256_hash, storage_path, gold_target,
                status, updated_at
            FROM url_inventory_map
            {where_clause}
            ORDER BY is_downloaded DESC, is_processed DESC, id ASC
            LIMIT ? OFFSET ?
        """
        cur.execute(data_query, params + [limit, offset])
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
            updated_at=r["updated_at"],
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
    with _get_conn() as conn:
        cur = conn.cursor()
        
        # Buscar por ID numérico o por SHA-256
        if url_id_or_hash.isdigit():
            cur.execute("SELECT * FROM url_inventory_map WHERE id = ?", (int(url_id_or_hash),))
        else:
            cur.execute("SELECT * FROM url_inventory_map WHERE normalized_url_sha256 = ?", (url_id_or_hash,))
        
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"URL no encontrada en el inventario: {url_id_or_hash}",
            )

        row_id = row["id"]
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        processed_val = 1 if request.is_processed else 0

        sql = "UPDATE url_inventory_map SET is_processed = ?, updated_at = ? WHERE id = ?"
        cur.execute(sql, [processed_val, now_iso, row_id])

        # Si tiene correspondencia en la tabla file, propagar
        if row["file_code"]:
            cur.execute(
                "UPDATE file SET is_processed = ? WHERE code = ?",
                [processed_val, row["file_code"]],
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
        updated_at=updated["updated_at"],
    )
