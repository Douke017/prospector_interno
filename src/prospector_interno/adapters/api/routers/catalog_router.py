"""Router de Catálogo de Fuentes, Activos y Grafo de Linaje (HUI-17)."""

from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query

import psycopg
from psycopg.rows import dict_row

from prospector_interno.adapters.api.schemas import (
    CatalogStatsResponse,
    DatasetResponse,
    SourceResponse,
)
from prospector_interno.adapters.storage.sqlite_catalog_lineage import (
    SqliteLineageAdapter,
)
from prospector_interno.application.lineage.lineage_tracker import (
    LineageTracker,
)
from prospector_interno.domain.contracts.lineage_spec import (
    AssetLineageGraphResponse,
    InternalAssetRecord,
    LineageIntegrityCheckResult,
)
from prospector_interno.domain.exceptions import EntityNotFoundError

router = APIRouter(prefix="/catalog", tags=["1. Catálogo Portal Nexus"])

DB_CONN = (
    "host=localhost port=5434 dbname=platform_db "
    "user=postgres password=postgres"
)

_default_lineage_adapter = SqliteLineageAdapter(
    Path("data/catalog/datax_catalog.db")
)
_lineage_tracker = LineageTracker(_default_lineage_adapter)


def get_lineage_tracker() -> LineageTracker:
    """Obtiene la instancia activa del servicio de linaje."""
    return _lineage_tracker


def set_lineage_tracker(tracker: LineageTracker) -> None:
    """Sobrescribe la instancia activa de LineageTracker (útil para tests)."""
    global _lineage_tracker
    _lineage_tracker = tracker


@router.get(

    "/stats",
    response_model=CatalogStatsResponse,
    summary="Obtener métricas consolidadas del catálogo",
    description="Retorna conteo de fuentes, datasets y descargas en la MV.",
)
def get_catalog_stats() -> CatalogStatsResponse:
    """Consulta las estadísticas pre-agregadas en la Vista Materializada."""
    try:
        with psycopg.connect(DB_CONN, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT source_code) AS total_sources,
                        COUNT(*) AS total_datasets,
                        COALESCE(SUM(total_historical_downloads), 0) AS total_downloads
                    FROM mv_nexus_active_catalog;
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
        with psycopg.connect(DB_CONN, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
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
                        COUNT(m.canonical_dataset_code) AS total_active_datasets
                    FROM source s
                    INNER JOIN mv_nexus_active_catalog m ON s.id_source = m.id_source
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
    """Busca series canónicas en la vista materializada indexada."""
    query = """
        SELECT
            canonical_dataset_code,
            source_code,
            source_name,
            dataset_name,
            publication_frequency,
            target_url,
            format_type,
            last_download_date::text AS last_download_date,
            last_known_hash,
            total_historical_downloads
        FROM mv_nexus_active_catalog
        WHERE 1=1
    """
    params: list[Any] = []

    if source_code:
        query += " AND UPPER(source_code) = UPPER(%s)"
        params.append(source_code)

    if q:
        query += " AND (dataset_name ILIKE %s OR canonical_dataset_code ILIKE %s)"
        like_term = f"%{q}%"
        params.extend([like_term, like_term])

    query += (
        " ORDER BY total_historical_downloads DESC, canonical_dataset_code "
        "LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])

    try:
        with psycopg.connect(DB_CONN, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
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
    tracker = get_lineage_tracker()
    return tracker._repository.list_assets(
        storage_layer=storage_layer,
        source_id=source_id,
    )


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
