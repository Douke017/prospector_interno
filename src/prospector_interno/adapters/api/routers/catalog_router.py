"""Router de Catálogo de Fuentes, Activos y Grafo de Linaje (HUI-17)."""

from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query

import psycopg

from prospector_interno.adapters.api.schemas import (
    CatalogStatsResponse,
    DatasetResponse,
    SourceResponse,
)
from prospector_interno.adapters.storage.db_connection import (
    get_readonly_connection,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
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
from prospector_interno.domain.exceptions import EntityNotFoundError

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


_default_lineage_adapter = SqliteLineageAdapter(
    Path("data/catalog/datax_catalog.db")
)
_lineage_tracker = LineageTracker(_default_lineage_adapter)
_catalog_service = AssetCatalogService(_default_lineage_adapter)


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
                    raise HTTPException(
                        status_code=500,
                        detail="Vista no disponible",
                    )

                return CatalogStatsResponse(
                    total_active_sources=row["total_sources"],
                    total_canonical_datasets=row["total_datasets"],
                    total_historical_downloads=int(row["total_downloads"]),
                    view_status="MATERIALIZED_IN_POSTGRESQL",
                    query_engine="DuckDB + PostgreSQL MV",
                )
    except psycopg.Error as err:
        raise HTTPException(
            status_code=503,
            detail=f"Error consultando la base de datos: {err}",
        ) from err


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
    except psycopg.Error as err:
        raise HTTPException(
            status_code=503,
            detail=f"Error consultando fuentes: {err}",
        ) from err


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
    except psycopg.Error as err:
        raise HTTPException(
            status_code=503,
            detail=f"Error en consulta de datasets: {err}",
        ) from err


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
    summary="Auditar integridad criptográfica de un activo y sus insumos (HUI-17)",
    description=(
        "Recalcula en tiempo real los hashes SHA-256 de los archivos en disco "
        "y los compara contra los registros inmutables de la cadena de custodia."
    ),
)
def verify_asset_integrity(
    asset_id: str,
    bronze_dir: str | None = Query(
        default="data/bronze",
        description="Directorio base de insumos Bronze para validación cruzada",
    ),
) -> LineageIntegrityCheckResult:
    """Verifica la integridad criptográfica de un activo y sus insumos."""
    tracker = get_lineage_tracker()
    try:
        return tracker.verify_asset_integrity(
            asset_id=asset_id,
            base_bronze_dir=Path(bronze_dir) if bronze_dir else None,
        )
    except EntityNotFoundError as err:
        raise HTTPException(
            status_code=404,
            detail=f"Activo '{asset_id}' no encontrado para verificación.",
        ) from err
