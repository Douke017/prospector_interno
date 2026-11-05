"""Motor de Conciliación y Diff con DuckDB (HUI-18).

Compara los mapas generados por el Prospector Externo contra el inventario
y almacenamiento local del Prospector Interno a velocidad vectorizada.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import duckdb

from prospector_interno.domain.exceptions import (
    ExternalMapCorruptError,
    ExternalMapNotFoundError,
)
from prospector_interno.domain.models import (
    DiscoveredResourceItem,
    ReconciliationDiffReport,
    ReconciliationDiffReportItem,
    ResourceChangeCategory,
)
from prospector_interno.adapters.external.rest_prospector_client import (
    HttpExternalCatalogClient,
)
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    ExternalCatalogClientPort,
    ReconciliationEnginePort,
)


class DuckDBDiffEngine(ReconciliationEnginePort):
    """Motor de cálculo de discrepancias entre inventario externo y local."""

    def __init__(
        self,
        storage: BronzeStoragePort,
        external_client: ExternalCatalogClientPort | None = None,
    ) -> None:
        self._storage = storage
        self._external_client = external_client or HttpExternalCatalogClient()

    def _resolve_map_path(self, path: Path | str) -> Path | None:
        """Resuelve rutas relativas o absolutas de mapas externos de forma resiliente."""
        p = Path(path)
        if p.exists():
            return p
        raw_str = str(path)
        if "../" in raw_str:
            clean_p = Path(raw_str.replace("../", ""))
            if clean_p.exists():
                return clean_p
        repo_root = Path("/home/douke017/Datax")
        candidate = repo_root / raw_str.lstrip("./").lstrip("../")
        if candidate.exists():
            return candidate
        return None

    def _extract_resources_with_duckdb(
        self,
        path: Path,
        source_filter: str | None = None,
    ) -> tuple[str, str, list[DiscoveredResourceItem]]:
        """Extrae de forma transparente recursos de mapas compactos o completos."""
        con = duckdb.connect(":memory:")
        posix_path = path.resolve().as_posix()

        # Soporte nativo para mapas canónicos en CSV (entregados por el supervisor)
        if path.suffix.lower() == ".csv":
            try:
                where_clause = "WHERE main_url IS NOT NULL AND main_url != ''"
                if source_filter:
                    where_clause += f" AND main_url ILIKE '%{source_filter}%'"

                csv_query = f"""
                    SELECT
                        COALESCE(report_code, file_code) AS resource_key,
                        main_url AS url,
                        file_name AS title,
                        CAST(file_updated_to AS VARCHAR) AS period_label,
                        relacion_fabrica_vieja
                    FROM read_csv_auto('{posix_path}')
                    {where_clause}
                """
                rows = con.execute(csv_query).fetchall()
                if rows:
                    source_id = source_filter or path.stem.split("_")[0].lower()
                    run_id = "canonico_supervisor_v1"
                    items: list[DiscoveredResourceItem] = []
                    for row in rows:
                        key, url, title, period, rel = row
                        raw_ext = Path(str(url)).suffix
                        ext = raw_ext if raw_ext else ".xlsx"
                        items.append(
                            DiscoveredResourceItem(
                                resource_key=str(key),
                                url=str(url),
                                source_id=source_id,
                                title=str(title or key),
                                file_extension=ext,
                                period_label=str(period) if period else None,
                                change_status=(
                                    "CONFIRMED"
                                    if rel == "ANTECEDENTE_VIEJO_IDENTIFICADO"
                                    else "UNCHANGED"
                                ),
                            )
                        )
                    return source_id, run_id, items
            except Exception as err:
                raise ExternalMapCorruptError(
                    f"Fallo al leer mapa canónico CSV '{path}': {err}"
                ) from err

        # Intentar formato completo (mapa_*.json)
        try:
            full_query = f"""
                SELECT
                    source.id AS source_id,
                    run.run_id AS run_id,
                    unnest(datasets[1].resources) AS r
                FROM read_json_auto('{posix_path}')
            """
            rows = con.execute(full_query).fetchall()
            if rows:
                source_id = str(rows[0][0])
                run_id = str(rows[0][1])
                items: list[DiscoveredResourceItem] = []

                for row in rows:
                    r: dict[str, Any] = row[2]
                    items.append(
                        DiscoveredResourceItem(
                            resource_key=str(r.get("resource_key", "")),
                            url=str(r.get("url", "")),
                            source_id=source_id,
                            title=str(r.get("title", "")),
                            file_extension=str(r.get("file_extension", "")),
                            content_type=r.get("content_type"),
                            content_length_bytes=r.get("content_length_bytes"),
                            last_modified_header=r.get("last_modified_header"),
                            etag=r.get("etag"),
                            period_label=r.get("period_label"),
                            content_hash=r.get("content_hash"),
                            change_status=str(r.get("change_status", "UNCHANGED")),
                        )
                    )
                return source_id, run_id, items
        except Exception:
            pass

        # Intentar formato de proyección Datax (datax_projection.json / datax-projection-1.0)
        try:
            projection_query = f"""
                WITH raw_fam AS (
                    SELECT source_id, run_id, unnest(families) AS f FROM read_json_auto('{posix_path}')
                ),
                raw_periods AS (
                    SELECT source_id, run_id, unnest(f.periods) AS p FROM raw_fam
                ),
                raw_reps AS (
                    SELECT source_id, run_id, unnest(p.representations) AS r FROM raw_periods
                )
                SELECT
                    source_id,
                    run_id,
                    r
                FROM raw_reps
            """
            rows = con.execute(projection_query).fetchall()
            if rows:
                source_id = str(rows[0][0])
                run_id = str(rows[0][1])
                items = []
                for row in rows:
                    r: dict[str, Any] = row[2]
                    fmt = str(r.get("format", "") or "")
                    if fmt and not fmt.startswith("."):
                        ext = f".{fmt}"
                    else:
                        ext = fmt or Path(str(r.get("url", ""))).suffix or ".xlsx"

                    items.append(
                        DiscoveredResourceItem(
                            resource_key=str(r.get("resource_key", "")),
                            url=str(r.get("url", "")),
                            source_id=source_id,
                            title=str(r.get("title", "") or r.get("description", "")),
                            file_extension=ext,
                            content_type=r.get("content_type"),
                            content_length_bytes=None,
                            last_modified_header=r.get("last_modified_header"),
                            etag=r.get("etag"),
                            period_label=r.get("period_label"),
                            content_hash=r.get("content_hash"),
                            change_status=str(r.get("change_status", "UNCHANGED")),
                        )
                    )
                return source_id, run_id, items
        except Exception:
            pass

        # Intentar formato compacto (mapa_*_compact.json)
        try:
            compact_query = f"""
                SELECT
                    source_id,
                    run_id,
                    unnest(resources) AS r
                FROM read_json_auto('{posix_path}')
            """
            rows = con.execute(compact_query).fetchall()
            if rows:
                source_id = str(rows[0][0])
                run_id = str(rows[0][1])
                items = []

                for row in rows:
                    r = row[2]
                    items.append(
                        DiscoveredResourceItem(
                            resource_key=str(r.get("key", "")),
                            url=str(r.get("url", "")),
                            source_id=source_id,
                            title=str(r.get("title", "")),
                            file_extension=str(r.get("ext", "")),
                            period_label=r.get("period"),
                            change_status=str(r.get("status", "UNCHANGED")),
                        )
                    )
                return source_id, run_id, items
        except Exception as err:
            raise ExternalMapCorruptError(
                f"No fue posible procesar el archivo JSON en '{path}': {err}"
            ) from err

        raise ExternalMapCorruptError(
            f"Estructura JSON no reconocida en '{path}'"
        )

    def compute_diff(
        self,
        source_id: str,
        external_map_path: Path | None = None,
        external_api_url: str | None = None,
    ) -> ReconciliationDiffReport:
        """Compara el inventario externo con el almacenamiento local Bronze."""
        if external_map_path is not None:
            resolved_path = self._resolve_map_path(external_map_path)
            if resolved_path is None:
                raise ExternalMapNotFoundError(
                    f"No se encontró el mapa del Prospector Externo en '{external_map_path}'"
                )
            src_id, run_id, discovered = self._extract_resources_with_duckdb(
                resolved_path, source_filter=source_id
            )
        else:
            discovered = self._external_client.fetch_resources(
                source_id=source_id,
                base_url=external_api_url,
            )
            src_id = source_id
            run_id = (
                self._external_client.get_latest_run_id(
                    source_id=source_id,
                    base_url=external_api_url,
                )
                or f"run_rest_{source_id}"
            )

            # Fallback a búsqueda local de mapas si REST no devolvió recursos
            if not discovered:
                candidates = [
                    Path(f"mapas_externo_last/{source_id}/datax_projection.json"),
                    Path(f"prospector_externo/output/{source_id}/mapa_{source_id}.json"),
                    Path(f"prospector_externo/output/{source_id}/mapa_{source_id}_compact.json"),
                    Path(f"archivos_prospector_interno/{source_id}_mapa_interno_canonico_v1.csv"),
                ]
                for cand in candidates:
                    resolved = self._resolve_map_path(cand)
                    if resolved and resolved.exists():
                        try:
                            src_id, run_id, discovered = self._extract_resources_with_duckdb(
                                resolved, source_filter=source_id
                            )
                            if discovered:
                                break
                        except Exception:
                            continue

        diff_items: list[ReconciliationDiffReportItem] = []
        confirmed_count = 0
        new_count = 0
        modified_count = 0

        for item in discovered:
            domain = urlparse(item.url).netloc or source_id
            dataset_code = f"DS_BO_{source_id.upper()}"

            # 1. Verificar si ya existe en Capa Bronze por URL exacta de descarga
            cached = self._storage.find_by_url(item.url)

            # 2. Verificar por clave canónica de recurso (resource_key)
            if cached is None and item.resource_key:
                cached = self._storage.find_by_resource_key(item.resource_key)

            # 3. Verificar si ya existe por checksum de contenido
            if cached is None and item.content_hash:
                cached = self._storage.find_by_checksum(item.content_hash)

            # 4. Si no hay content_hash o no se halló, verificar por periodo
            if cached is None and item.period_label:
                cached = self._storage.find_by_period(
                    domain=domain,
                    dataset_code=dataset_code,
                    period=item.period_label,
                )
                if cached is None and domain != source_id:
                    cached = self._storage.find_by_period(
                        domain=source_id,
                        dataset_code=dataset_code,
                        period=item.period_label,
                    )

            if cached is not None:
                category = ResourceChangeCategory.CONFIRMED
                confirmed_count += 1
                detail = "Recurso verificado y disponible en almacenamiento local."
            elif item.change_status == "MODIFIED":
                category = ResourceChangeCategory.MODIFIED
                modified_count += 1
                detail = "Recurso detectado con cambio de contenido en origen."
            else:
                category = ResourceChangeCategory.NEW
                new_count += 1
                detail = "Nuevo recurso descubierto pendiente de adquisición."

            diff_items.append(
                ReconciliationDiffReportItem(
                    resource_key=item.resource_key,
                    url=item.url,
                    source_id=src_id,
                    category=category,
                    file_extension=item.file_extension,
                    period_label=item.period_label,
                    size_bytes=item.content_length_bytes,
                    detail=detail,
                )
            )

        return ReconciliationDiffReport(
            source_id=src_id,
            run_id=run_id,
            generated_at=datetime.now(timezone.utc),
            total_external_resources=len(discovered),
            total_confirmed=confirmed_count,
            total_new=new_count,
            total_modified=modified_count,
            items=diff_items,
        )
