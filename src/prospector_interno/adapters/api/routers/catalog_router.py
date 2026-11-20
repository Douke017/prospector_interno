"""Router de Catálogo de Fuentes, Activos y Grafo de Linaje (HUI-17)."""

import hashlib
from pathlib import Path
import sqlite3
from typing import Any
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

import psycopg

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.schemas import (
    CatalogStatsResponse,
    DatasetResponse,
    SourceResponse,
)
from prospector_interno.adapters.storage.db_connection import (
    get_readonly_connection,
    get_readwrite_connection,
)
from prospector_interno.application.catalog.asset_catalog_service import (
    AssetCatalogService,
)
from prospector_interno.application.lineage.lineage_tracker import (
    LineageTracker,
)
from prospector_interno.domain.catalog.specifications import (
    AssetSpecification,
    BaseAssetSpecification,
)
from prospector_interno.domain.contracts.lineage_spec import (
    AssetLineageGraphResponse,
    InternalAssetRecord,
    LineageIntegrityCheckResult,
)
from prospector_interno.domain.exceptions import EntityNotFoundError, LegacySeederError

router = APIRouter(prefix="/catalog", tags=["1. Catálogo Portal Nexus"])

CATALOG_VIEW_CTE = """(
    WITH deduplicated_file AS (
        SELECT DISTINCT ON (file.code) file.id_file,
            file.id_source,
            file.code,
            file.name,
            file.specific_url,
            file.main_url,
            file.publication_frequency,
            file.type,
            file.creation_date,
            file.state
        FROM file
        WHERE ((file.code IS NOT NULL) AND (TRIM(BOTH FROM file.code) <> ''))
        ORDER BY file.code, file.id_file DESC
    )
    SELECT s.id_source,
        s.short_name AS source_code,
        s.name AS source_name,
        CASE
            WHEN (TRIM(BOTH FROM s.homepage) = ANY (
                ARRAY['vacio', 'no-data', '']
            )) THEN NULL
            ELSE TRIM(BOTH FROM s.homepage)
        END AS source_homepage,
        f.id_file,
        f.code AS legacy_file_code,
        'DS_BO_' || upper(
            regexp_replace(s.short_name, '[^A-Za-z0-9]+', '_', 'g')
        ) || '_' || upper(
            regexp_replace(f.code, '[^A-Za-z0-9]+', '_', 'g')
        ) AS canonical_dataset_code,
        f.name AS dataset_name,
        f.publication_frequency,
        COALESCE(
            NULLIF(TRIM(BOTH FROM f.specific_url), ''),
            NULLIF(TRIM(BOTH FROM f.main_url), '')
        ) AS target_url,
        f.type AS format_type,
        f.creation_date,
        max(d.download_date) AS last_download_date,
        max(d.download_hash) AS last_known_hash,
        count(d.id_download) AS total_historical_downloads
    FROM source s
    JOIN deduplicated_file f ON s.id_source = f.id_source
    LEFT JOIN download d ON f.id_file = d.id_file
    GROUP BY
        s.id_source, s.short_name, s.name, s.homepage,
        f.id_file, f.code, f.name, f.publication_frequency,
        f.specific_url, f.main_url, f.type, f.creation_date
)"""


def _get_catalog_source(conn: psycopg.Connection, alias: str = "") -> str:
    """Retorna la tabla o CTE canónica del catálogo con alias opcional."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_nexus_active_catalog';"
        )
        has_mv = cur.fetchone() is not None

    table_name = "mv_nexus_active_catalog" if has_mv else CATALOG_VIEW_CTE
    if alias:
        return f"{table_name} AS {alias}" if not has_mv else f"{table_name} {alias}"
    return table_name if has_mv else f"{table_name} AS mv_nexus_active_catalog"


_default_lineage_adapter = deps.lineage_adapter
_lineage_tracker = deps.lineage_tracker
_catalog_service = deps.asset_catalog_service
_bronze_storage = deps.bronze_storage
_profile_repo = deps.asset_profile_repo
_raw_profiler = deps.raw_file_profiler


def set_bronze_storage(storage: Any) -> None:
    """Permite inyectar un repositorio Bronze aislado en pruebas."""
    global _bronze_storage
    _bronze_storage = storage


def set_profile_services(profile_repo: Any, profiler: Any) -> None:
    """Permite inyectar perfilador y repositorio de perfiles en pruebas."""
    global _profile_repo, _raw_profiler
    _profile_repo = profile_repo
    _raw_profiler = profiler


def get_lineage_tracker() -> LineageTracker:
    """Obtiene la instancia activa del servicio de linaje."""
    return _lineage_tracker


def set_lineage_tracker(tracker: LineageTracker) -> None:
    """Sobrescribe la instancia activa de LineageTracker (útil para tests)."""
    global _lineage_tracker, _catalog_service
    _lineage_tracker = tracker
    _catalog_service = AssetCatalogService(tracker._repository)


def get_asset_catalog_service() -> AssetCatalogService:
    """Obtiene la instancia activa del servicio de catálogo técnico."""
    return _catalog_service


def set_asset_catalog_service(service: AssetCatalogService) -> None:
    """Sobrescribe la instancia activa de AssetCatalogService (útil para tests)."""
    global _catalog_service
    _catalog_service = service


def _get_sqlite_snapshot_path() -> Path | None:
    candidates = [
        Path("prospector_interno/data/snapshot/platform_prod_snapshot.db"),
        Path("data/snapshot/platform_prod_snapshot.db"),
        Path(__file__).resolve().parents[4] / "data/snapshot/platform_prod_snapshot.db",
        Path(__file__).resolve().parents[5] / "prospector_interno/data/snapshot/platform_prod_snapshot.db",
    ]
    for p in candidates:
        if p.is_file():
            return p.resolve()
    return None


def _fallback_stats_sqlite() -> CatalogStatsResponse:
    snap_path = _get_sqlite_snapshot_path()
    if not snap_path:
        raise HTTPException(status_code=503, detail="No hay base de datos de producción ni snapshot SQLite disponible")
    with sqlite3.connect(str(snap_path)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT s.short_name) AS total_sources,
                COUNT(f.id_file) AS total_datasets,
                (SELECT COUNT(*) FROM download) AS total_downloads
            FROM source s
            JOIN file f ON s.id_source = f.id_source;
            """
        )
        row = cur.fetchone()
        return CatalogStatsResponse(
            total_active_sources=row["total_sources"] or 0,
            total_canonical_datasets=row["total_datasets"] or 0,
            total_historical_downloads=int(row["total_downloads"] or 0),
            view_status="FALLBACK_SQLITE_SNAPSHOT",
            query_engine="SQLite Snapshot",
        )


def _fallback_sources_sqlite() -> list[SourceResponse]:
    snap_path = _get_sqlite_snapshot_path()
    if not snap_path:
        raise HTTPException(status_code=503, detail="No hay base de datos de producción ni snapshot SQLite disponible")
    with sqlite3.connect(str(snap_path)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                s.id_source,
                'BO_' || UPPER(s.short_name) AS canonical_code,
                s.name AS official_name,
                s.short_name,
                CASE
                    WHEN TRIM(s.homepage) IN ('vacio', 'no-data', '') THEN NULL
                    ELSE TRIM(s.homepage)
                END AS homepage,
                COUNT(f.id_file) AS total_active_datasets
            FROM source s
            INNER JOIN file f ON s.id_source = f.id_source
            GROUP BY s.id_source, s.short_name, s.name, s.homepage
            ORDER BY total_active_datasets DESC;
            """
        )
        rows = cur.fetchall()
        return [
            SourceResponse(
                source_id=r["id_source"],
                canonical_code=r["canonical_code"],
                official_name=r["official_name"],
                short_name=r["short_name"],
                homepage=r["homepage"],
                country_iso="BOL",
                is_active=True,
                total_active_datasets=r["total_active_datasets"],
            )
            for r in rows
        ]


def _fallback_datasets_sqlite(
    q: str | None = None,
    source_code: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[DatasetResponse]:
    snap_path = _get_sqlite_snapshot_path()
    if not snap_path:
        raise HTTPException(status_code=503, detail="No hay base de datos de producción ni snapshot SQLite disponible")
    params: list[Any] = []
    where_parts: list[str] = []
    if source_code:
        where_parts.append("UPPER(s.short_name) = UPPER(?)")
        params.append(source_code)
    if q:
        where_parts.append("(f.name LIKE ? OR ('DS_BO_' || UPPER(s.short_name) || '_' || UPPER(f.code)) LIKE ?)")
        term = f"%{q.strip()}%"
        params.extend([term, term])

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = f"""
        SELECT
            'DS_BO_' || UPPER(s.short_name) || '_' || UPPER(f.code) AS canonical_dataset_code,
            s.short_name AS source_code,
            s.name AS source_name,
            f.name AS dataset_name,
            f.publication_frequency,
            COALESCE(NULLIF(TRIM(f.specific_url), ''), NULLIF(TRIM(f.main_url), '')) AS target_url,
            f.type AS format_type,
            MAX(d.download_date) AS last_download_date,
            MAX(d.download_hash) AS last_known_hash,
            COUNT(d.id_download) AS total_historical_downloads
        FROM source s
        JOIN file f ON s.id_source = f.id_source
        LEFT JOIN download d ON f.id_file = d.id_file
        {where_sql}
        GROUP BY s.id_source, s.short_name, s.name, f.id_file, f.code, f.name, f.publication_frequency, f.specific_url, f.main_url, f.type
        ORDER BY total_historical_downloads DESC, canonical_dataset_code
        LIMIT ? OFFSET ?;
    """
    params.extend([limit, offset])

    with sqlite3.connect(str(snap_path)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        return [
            DatasetResponse(
                canonical_dataset_code=r["canonical_dataset_code"],
                source_code=r["source_code"],
                source_name=r["source_name"],
                dataset_name=r["dataset_name"],
                publication_frequency=r["publication_frequency"] or "Mensual",
                target_url=r["target_url"],
                format_type=r["format_type"],
                last_download_date=r["last_download_date"],
                last_known_hash=r["last_known_hash"],
                total_historical_downloads=r["total_historical_downloads"] or 0,
            )
            for r in rows
        ]


@router.get(
    "/stats",
    response_model=CatalogStatsResponse,
    summary="Obtener métricas consolidadas del catálogo",
    description="Retorna conteo de fuentes, datasets y descargas en la MV.",
)
def get_catalog_stats() -> CatalogStatsResponse:
    """Consulta las estadísticas pre-agregadas en la Vista Materializada o CTE."""
    try:
        with get_readonly_connection() as conn:
            with conn.cursor() as cur:
                catalog_src = _get_catalog_source(conn, "m")
                cur.execute(
                    f"""
                    SELECT
                        COUNT(DISTINCT m.source_code) AS total_sources,
                        COUNT(*) AS total_datasets,
                        COALESCE(
                            SUM(m.total_historical_downloads), 0
                        ) AS total_downloads
                    FROM {catalog_src};
                    """
                )
                row = cur.fetchone()
                if not row:
                    return _fallback_stats_sqlite()

                return CatalogStatsResponse(
                    total_active_sources=row["total_sources"],
                    total_canonical_datasets=row["total_datasets"],
                    total_historical_downloads=int(row["total_downloads"]),
                    view_status="MATERIALIZED_IN_POSTGRESQL",
                    query_engine="DuckDB + PostgreSQL MV",
                )
    except (psycopg.Error, LegacySeederError):
        return _fallback_stats_sqlite()


@router.get(
    "/sources",
    response_model=list[SourceResponse],
    summary="Listar instituciones emisoras activas (Nexus Ready)",
    description="Filtra fuentes muertas y entrega solo entidades vivas con datasets.",
)
def list_active_sources() -> list[SourceResponse]:
    """Retorna las instituciones emisoras que tienen series y datos vigentes."""
    try:
        with get_readonly_connection() as conn:
            with conn.cursor() as cur:
                catalog_src = _get_catalog_source(conn, "m")
                cur.execute(
                    f"""
                    SELECT
                        s.id_source,
                        'BO_' || UPPER(s.short_name) AS canonical_code,
                        s.name AS official_name,
                        s.short_name,
                        CASE
                            WHEN TRIM(s.homepage) IN ('vacio', 'no-data', '') THEN NULL
                            ELSE TRIM(s.homepage)
                        END AS homepage,
                        COUNT(m.canonical_dataset_code) AS total_active_datasets
                    FROM source s
                    INNER JOIN {catalog_src} ON s.id_source = m.id_source
                    GROUP BY s.id_source, s.short_name, s.name, s.homepage
                    ORDER BY total_active_datasets DESC;
                    """
                )
                rows = cur.fetchall()

                return [
                    SourceResponse(
                        source_id=r["id_source"],
                        canonical_code=r["canonical_code"],
                        official_name=r["official_name"],
                        short_name=r["short_name"],
                        homepage=r["homepage"],
                        country_iso="BOL",
                        is_active=True,
                        total_active_datasets=r["total_active_datasets"],
                    )
                    for r in rows
                ]
    except (psycopg.Error, LegacySeederError):
        return _fallback_sources_sqlite()


@router.get(
    "/datasets",
    response_model=list[DatasetResponse],
    summary="Buscar y filtrar conjuntos de datos en la Vista Materializada",
    description="Búsqueda de alta velocidad (<5ms) con soporte de filtros.",
)
def search_datasets(
    q: str | None = Query(
        default=None,
        description="Término de búsqueda (ej. 'inflacion', 'seguros')",
    ),
    source_code: str | None = Query(
        default=None,
        description="Filtrar por código de fuente (ej. 'BCB', 'INE', 'ASFI')",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[DatasetResponse]:
    """Busca series canónicas en la vista materializada indexada o CTE."""
    params: list[Any] = []
    try:
        with get_readonly_connection() as conn:
            with conn.cursor() as cur:
                catalog_src = _get_catalog_source(conn, "m")
                query = f"""
                    SELECT
                        m.canonical_dataset_code,
                        m.source_code,
                        m.source_name,
                        m.dataset_name,
                        m.publication_frequency,
                        m.target_url,
                        m.format_type,
                        m.last_download_date::text AS last_download_date,
                        m.last_known_hash,
                        m.total_historical_downloads
                    FROM {catalog_src}
                    WHERE 1=1
                """

                if source_code:
                    query += " AND UPPER(m.source_code) = UPPER(%s)"
                    params.append(source_code)

                if q:
                    query += (
                        " AND (m.dataset_name ILIKE %s OR "
                        "m.canonical_dataset_code ILIKE %s)"
                    )
                    like_term = f"%{q}%"
                    params.extend([like_term, like_term])

                query += (
                    " ORDER BY m.total_historical_downloads DESC, "
                    "m.canonical_dataset_code LIMIT %s OFFSET %s"
                )
                params.extend([limit, offset])

                cur.execute(query, params)
                rows = cur.fetchall()

                return [
                    DatasetResponse(
                        canonical_dataset_code=r["canonical_dataset_code"],
                        source_code=r["source_code"],
                        source_name=r["source_name"],
                        dataset_name=r["dataset_name"],
                        publication_frequency=r["publication_frequency"] or "Mensual",
                        target_url=r["target_url"],
                        format_type=r["format_type"],
                        last_download_date=r["last_download_date"],
                        last_known_hash=r["last_known_hash"],
                        total_historical_downloads=r["total_historical_downloads"] or 0,
                    )
                    for r in rows
                ]
    except (psycopg.Error, LegacySeederError):
        return _fallback_datasets_sqlite(q=q, source_code=source_code, limit=limit, offset=offset)


@router.get(
    "/assets",
    response_model=list[InternalAssetRecord],
    summary="Listar activos de datos catalogados en inventario técnico (HUI-15/HUI-17)",
    description=(
        "Consulta el inventario de activos catalogados filtrando opcionalmente "
        "por capa de persistencia (BRONZE, SILVER, GOLD) y fuente."
    ),
)
def list_catalog_assets(
    storage_layer: str | None = Query(
        default=None,
        description="Filtro por capa (BRONZE, SILVER, GOLD)",
    ),
    source_id: str | None = Query(
        default=None,
        description="Filtro por código de fuente (ej: 'finrural', 'asfi')",
    ),
) -> list[InternalAssetRecord]:
    """Retorna los activos registrados en el catálogo relacional."""
    service = get_asset_catalog_service()
    spec: BaseAssetSpecification = AssetSpecification.all()
    if storage_layer:
        spec = spec.by_layer(storage_layer)
    if source_id:
        spec = spec.by_source(source_id)
    return service.find_assets(spec)


@router.get(
    "/assets/search",
    response_model=list[InternalAssetRecord],
    summary="Búsqueda avanzada de activos con Specification Pattern (HUI-15)",
    description=(
        "Permite componer filtros dinámicos por fuente, dataset, capa, "
        "rango temporal y mínimo de filas sobre el inventario técnico."
    ),
)
def search_catalog_assets(
    source_id: str | None = Query(
        default=None,
        description="Código de la fuente emisora (ej: 'finrural')",
    ),
    dataset_id: str | None = Query(
        default=None,
        description="Código del conjunto de datos (ej: 'mart_cartera')",
    ),
    storage_layer: str | None = Query(
        default=None,
        description="Capa Medallion (BRONZE, SILVER, GOLD)",
    ),
    start_date: str | None = Query(
        default=None,
        description="Fecha inicial del periodo (YYYY-MM-DD)",
    ),
    end_date: str | None = Query(
        default=None,
        description="Fecha final del periodo (YYYY-MM-DD)",
    ),
    min_rows: int | None = Query(
        default=None,
        ge=0,
        description="Cantidad mínima de filas requeridas",
    ),
) -> list[InternalAssetRecord]:
    """Ejecuta una búsqueda componible mediante el Specification Pattern."""
    spec: BaseAssetSpecification = AssetSpecification.all()

    if source_id:
        spec = spec.by_source(source_id)
    if dataset_id:
        spec = spec.by_dataset(dataset_id)
    if storage_layer:
        spec = spec.by_layer(storage_layer)
    if start_date and end_date:
        spec = spec.in_range(start_date, end_date)
    if min_rows is not None:
        spec = spec.with_min_rows(min_rows)

    service = get_asset_catalog_service()
    return service.find_assets(spec)


@router.get(
    "/assets/summary",
    response_model=dict[str, Any],
    summary="Resumen ejecutivo del inventario de activos catalogados (HUI-15)",
    description="Retorna métricas de volumen, distribución por capas y fuentes.",
)
def get_inventory_summary() -> dict[str, Any]:
    """Calcula el resumen agregado del inventario técnico."""
    service = get_asset_catalog_service()
    return service.get_inventory_summary()


@router.get(

    "/assets/{asset_id}/lineage",
    response_model=AssetLineageGraphResponse,
    summary="Consultar grafo de linaje y cadena de custodia (HUI-17)",
    description=(
        "Reconstruye la ruta de procedencia criptográfica de un activo: "
        "archivos Bronze de origen, hashes SHA-256, run_id del Externo "
        "y pasos ejecutados."
    ),

)
def get_asset_lineage(asset_id: str) -> AssetLineageGraphResponse:
    """Retorna el árbol de linaje y cadena de custodia de un activo."""
    tracker = get_lineage_tracker()
    try:
        return tracker.get_asset_lineage_graph(asset_id)
    except EntityNotFoundError as err:
        raise HTTPException(
            status_code=404,
            detail=f"Activo '{asset_id}' no encontrado en el inventario de linaje.",
        ) from err


@router.get(
    "/assets/{asset_id}/verify",
    response_model=LineageIntegrityCheckResult,
    summary="Auditar integridad criptográfica de un activo y sus insumos",
)
def verify_asset_integrity(
    asset_id: str,
    bronze_dir: str | None = Query(
        default=None,
        description="Directorio base de insumos Bronze para validación cruzada",
    ),
) -> LineageIntegrityCheckResult:
    """Verifica la integridad criptográfica de un activo y sus insumos."""
    tracker = get_lineage_tracker()
    resolved_bronze = Path(bronze_dir) if bronze_dir else deps.BRONZE_DIR
    try:
        return tracker.verify_asset_integrity(
            asset_id=asset_id,
            base_bronze_dir=resolved_bronze,
        )
    except EntityNotFoundError as err:
        raise HTTPException(
            status_code=404,
            detail=f"Activo '{asset_id}' no encontrado para verificación.",
        ) from err


@router.get(
    "/lineage/datamart/{datamart_name}",
    response_model=list[dict[str, Any]],
    summary="Consultar trazabilidad documental completa de un repositorio Gold",
)
def get_datamart_lineage(
    datamart_name: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """Retorna todos los vínculos criptográficos Gold -> Bronze de un repositorio."""
    tracker = get_lineage_tracker()
    repo = tracker._repository
    if hasattr(repo, "list_lineage_for_datamart"):
        return repo.list_lineage_for_datamart(datamart_name=datamart_name, limit=limit)

    spec = AssetSpecification.all().by_dataset(datamart_name)
    assets = get_asset_catalog_service().find_assets(spec)
    out: list[dict[str, Any]] = []
    for a in assets:
        for l_rec in repo.get_lineage_by_asset(a.asset_id):
            out.append(
                {
                    "asset_id": a.asset_id,
                    "datamart_name": a.dataset_id,
                    "source_id": a.source_id,
                    "period_start": a.period_start,
                    "row_count": a.row_count,
                    "gold_sha256": a.content_hash_sha256,
                    "lineage_id": l_rec.lineage_id,
                    "bronze_source_file": l_rec.bronze_source_file,
                    "bronze_file_sha256": l_rec.bronze_file_sha256,
                    "external_run_id": l_rec.external_run_id,
                    "pipeline_version": l_rec.pipeline_version,
                    "transformed_at": l_rec.transformed_at.isoformat(),
                    "canonical_url": None,
                }
            )
            if len(out) >= limit:
                return out
    return out


def _resolve_raw_asset_target(asset_id: str) -> tuple[Path | None, str | None, str, str, str | None]:
    """Resuelve (file_path, expected_sha256, source_id, filename, media_type) para un activo."""
    tracker = get_lineage_tracker()
    asset_rec = tracker._repository.get_asset(asset_id)
    if asset_rec is not None:
        fp = Path(asset_rec.file_path)
        if not fp.is_file() and hasattr(_bronze_storage, "find_by_checksum"):
            b_art = _bronze_storage.find_by_checksum(asset_rec.content_hash_sha256)
            if b_art is not None:
                fp = Path(b_art.file_path)
        fname = fp.name if fp.name and not fp.name.endswith(".dat") else f"{asset_rec.dataset_id}_{asset_rec.asset_id}{fp.suffix or '.bin'}"
        return fp, asset_rec.content_hash_sha256, asset_rec.source_id, fname, None

    if hasattr(_bronze_storage, "find_by_artifact_id"):
        b_art = _bronze_storage.find_by_artifact_id(asset_id)
        if b_art is not None:
            fp = Path(b_art.file_path)
            ext = fp.suffix if fp.suffix and fp.suffix != ".dat" else ".bin"
            if b_art.content_type:
                ct_low = b_art.content_type.lower()
                if "csv" in ct_low:
                    ext = ".csv"
                elif "spreadsheet" in ct_low or "xlsx" in ct_low:
                    ext = ".xlsx"
                elif "pdf" in ct_low:
                    ext = ".pdf"
                elif "json" in ct_low:
                    ext = ".json"
                elif "zip" in ct_low:
                    ext = ".zip"
                elif "parquet" in ct_low:
                    ext = ".parquet"
            fname = f"{b_art.dataset_code}_{b_art.artifact_id}{ext}"
            return fp, b_art.sha256_hash, b_art.domain, fname, b_art.content_type

    return None, None, "UNKNOWN", f"{asset_id}.bin", None


def _mark_asset_integrity_corrupted(asset_id: str) -> None:
    if not deps.USE_POSTGRES_MUTATIONS:
        return
    try:
        with get_readwrite_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.internal_assets SET integrity_status = 'CORRUPTED' WHERE asset_id = %s;",
                    (asset_id,),
                )
            conn.commit()
    except Exception:
        return


@router.get(
    "/assets/{asset_id}/download",
    summary="Descargar en streaming el archivo binario original intacto con verificación SHA-256 (Tarea 4.1)",
)
def download_raw_asset(asset_id: str) -> StreamingResponse:
    """Transmite el binario original de custodia verificando su firma SHA-256 en disco."""
    file_path, expected_sha, _source_id, filename, media_type = _resolve_raw_asset_target(asset_id)
    if file_path is None and expected_sha is None:
        raise HTTPException(
            status_code=404,
            detail=f"Activo '{asset_id}' no encontrado en el catálogo ni en custodia Bronze.",
        )
    if file_path is None or not file_path.is_file():
        _mark_asset_integrity_corrupted(asset_id)
        raise HTTPException(
            status_code=409,
            detail=(
                f"Integridad comprometida (CORRUPTED): el archivo físico para '{asset_id}' "
                "no existe en disco."
            ),
        )

    hasher = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while chunk := fh.read(65536):
            hasher.update(chunk)
    actual_sha = hasher.hexdigest().lower()

    if expected_sha and actual_sha != expected_sha.strip().lower():
        _mark_asset_integrity_corrupted(asset_id)
        raise HTTPException(
            status_code=409,
            detail=(
                f"Integridad corrupta (SHA-256 mismatch) para '{asset_id}': "
                f"esperado={expected_sha[:16]}..., calculado={actual_sha[:16]}..."
            ),
        )

    size_bytes = file_path.stat().st_size

    def _file_iterator():
        with open(file_path, "rb") as fh:
            while chunk := fh.read(65536):
                yield chunk

    return StreamingResponse(
        _file_iterator(),
        media_type=media_type or "application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(size_bytes),
            "X-Content-SHA256": actual_sha,
            "Access-Control-Expose-Headers": "X-Content-SHA256, Content-Disposition, Content-Length",
        },
    )


@router.get(
    "/assets/{asset_id}/profile",
    summary="Obtener el perfil técnico estructural y muestra cruda de 20 filas de un activo (Tarea 4.2)",
)
def get_asset_technical_profile(
    asset_id: str,
    refresh: bool = Query(default=False, description="Recalcular el perfil técnico desde disco"),
) -> dict[str, Any]:
    """Devuelve hojas, delimitador/encoding, columnas, tipos, % nulos y 20 filas crudas."""
    if not refresh and _profile_repo is not None:
        cached = _profile_repo.get_profile(asset_id)
        if cached is not None:
            return cached

    file_path, expected_sha, source_id, filename, _media_type = _resolve_raw_asset_target(asset_id)
    if file_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Activo '{asset_id}' no encontrado para perfilado técnico.",
        )
    if not file_path.is_file():
        raise HTTPException(
            status_code=409,
            detail=f"Archivo físico de '{asset_id}' ausente en disco.",
        )
    return _raw_profiler.profile_file(
        asset_id=asset_id,
        file_path=file_path,
        source_id=source_id,
        sha256_hash=expected_sha,
        filename_hint=filename,
        persist=True,
    )


@router.get(
    "/search",
    summary="Búsqueda global de archivos brutos y datasets por nombre de columna o metadatos (Tarea 4.2)",
)
def global_catalog_search(
    column: str | None = Query(default=None, description="Nombre exacto o parcial de columna"),
    q: str | None = Query(default=None, description="Texto libre en dataset, fuente, ruta o columna"),
    source_id: str | None = Query(default=None, description="Filtrar por entidad/fuente"),
    extension: str | None = Query(default=None, description="Filtrar por formato (.csv, .xlsx, .parquet...)"),
    sha256: str | None = Query(default=None, description="Buscar por huella SHA-256"),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    """Localiza archivos brutos y tablas consolidadas por nombre de columna y metadatos."""
    column_matches: list[dict[str, Any]] = []
    asset_matches: list[dict[str, Any]] = []

    # 1. Buscar en el índice de columnas de archivos brutos perfilados
    if _profile_repo is not None and (column or q):
        col_query = column or q
        raw_cols = _profile_repo.search_columns(
            column=col_query,
            source_id=source_id,
            limit=limit,
        )
        for c in raw_cols:
            column_matches.append(
                {
                    **c,
                    "origin": "RAW_FILE_PROFILE",
                    "download_url": f"/api/v1/catalog/assets/{c['asset_id']}/download",
                    "profile_url": f"/api/v1/catalog/assets/{c['asset_id']}/profile",
                }
            )

    # 2. Complementar con columnas del diccionario de datasets Gold
    if (column or q) and hasattr(deps.semantic_dict_repo, "list_datamart_columns"):
        try:
            term_low = (column or q or "").strip().lower()
            for dcol in deps.semantic_dict_repo.list_datamart_columns():
                cname = str(dcol.get("column_name", ""))
                blabel = str(dcol.get("business_label", ""))
                if term_low in cname.lower() or (q and term_low in blabel.lower()):
                    column_matches.append(
                        {
                            "asset_id": dcol.get("datamart_name"),
                            "source_id": "GOLD",
                            "dataset_id": dcol.get("datamart_name"),
                            "sheet_name": "",
                            "column_name": cname,
                            "inferred_type": dcol.get("data_type", "VARCHAR"),
                            "null_pct": 0.0,
                            "distinct_count": 0,
                            "origin": "GOLD_DICTIONARY",
                            "export_parquet_url": f"/api/v1/analytics/export/parquet?datamart={dcol.get('datamart_name')}",
                        }
                    )
        except Exception:
            pass

    # 3. Buscar en activos registrados (internal_assets) y custodia Bronze
    ext_clean = (extension or "").strip().lower().lstrip(".")
    sha_clean = (sha256 or "").strip().lower()
    q_clean = (q or "").strip().lower()
    seen_ids: set[str] = set()

    for asset in get_asset_catalog_service().find_assets(AssetSpecification.all()):
        if source_id and asset.source_id.upper() != source_id.strip().upper():
            continue
        if sha_clean and sha_clean not in asset.content_hash_sha256.lower():
            continue
        if ext_clean and not asset.file_path.lower().endswith(f".{ext_clean}"):
            continue
        if q_clean and not (
            q_clean in asset.asset_id.lower()
            or q_clean in asset.dataset_id.lower()
            or q_clean in asset.source_id.lower()
            or q_clean in asset.file_path.lower()
            or q_clean in asset.content_hash_sha256.lower()
        ):
            continue
        seen_ids.add(asset.asset_id)
        asset_matches.append(
            {
                "asset_id": asset.asset_id,
                "source_id": asset.source_id,
                "dataset_id": asset.dataset_id,
                "storage_layer": asset.storage_layer.value,
                "period_start": asset.period_start,
                "period_end": asset.period_end,
                "size_bytes": asset.size_bytes,
                "sha256_hash": asset.content_hash_sha256,
                "download_url": f"/api/v1/catalog/assets/{asset.asset_id}/download",
                "profile_url": f"/api/v1/catalog/assets/{asset.asset_id}/profile",
            }
        )

    if hasattr(_bronze_storage, "get_artifacts"):
        for art in _bronze_storage.get_artifacts():
            if art.artifact_id in seen_ids:
                continue
            if source_id and source_id.strip().lower() not in art.domain.lower() and source_id.strip().lower() not in art.dataset_code.lower():
                continue
            if sha_clean and sha_clean not in art.sha256_hash.lower():
                continue
            if q_clean and not (
                q_clean in art.artifact_id.lower()
                or q_clean in art.dataset_code.lower()
                or q_clean in art.domain.lower()
                or q_clean in art.sha256_hash.lower()
            ):
                continue
            asset_matches.append(
                {
                    "asset_id": art.artifact_id,
                    "source_id": art.domain,
                    "dataset_id": art.dataset_code,
                    "storage_layer": "BRONZE",
                    "size_bytes": art.size_bytes,
                    "sha256_hash": art.sha256_hash,
                    "download_url": f"/api/v1/catalog/assets/{art.artifact_id}/download",
                    "profile_url": f"/api/v1/catalog/assets/{art.artifact_id}/profile",
                }
            )

    return {
        "query": {"column": column, "q": q, "source_id": source_id, "extension": extension, "sha256": sha256},
        "total_column_matches": len(column_matches[:limit]),
        "total_asset_matches": len(asset_matches[:limit]),
        "column_matches": column_matches[:limit],
        "asset_matches": asset_matches[:limit],
    }
