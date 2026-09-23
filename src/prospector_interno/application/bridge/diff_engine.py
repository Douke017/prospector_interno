"""Motor de Conciliación y Diff con DuckDB (HUI-18).

Compara los mapas generados por el Prospector Externo contra el inventario
y almacenamiento local del Prospector Interno a velocidad vectorizada.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
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
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    ReconciliationEnginePort,
)


class DuckDBDiffEngine(ReconciliationEnginePort):
    """Motor de cálculo de discrepancias entre inventario externo y local."""

    def __init__(self, storage: BronzeStoragePort) -> None:
        self._storage = storage

    def _extract_resources_with_duckdb(
        self,
        path: Path,
    ) -> tuple[str, str, list[DiscoveredResourceItem]]:
        """Extrae de forma transparente recursos de mapas compactos o completos."""
        con = duckdb.connect(":memory:")
        posix_path = path.as_posix()

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
        external_map_path: Path,
    ) -> ReconciliationDiffReport:
        """Compara las URLs externas con el inventario Bronze local."""
        map_path = Path(external_map_path)
        if not map_path.exists():
            raise ExternalMapNotFoundError(
                f"No se encontró el mapa del Prospector Externo en '{map_path}'"
            )

        src_id, run_id, discovered = self._extract_resources_with_duckdb(map_path)

        diff_items: list[ReconciliationDiffReportItem] = []
        confirmed_count = 0
        new_count = 0
        modified_count = 0

        for item in discovered:
            # 1. Verificar si ya existe en Capa Bronze por checksum
            cached = None
            if item.content_hash:
                cached = self._storage.find_by_checksum(item.content_hash)

            # 2. Si no hay content_hash o no se halló, verificar por periodo/url
            if cached is None and item.period_label:
                cached = self._storage.find_by_period(
                    domain=source_id,
                    dataset_code=f"DS_BO_{source_id.upper()}",
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
