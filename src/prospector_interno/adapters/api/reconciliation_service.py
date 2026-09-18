"""Servicio de Conciliación y Diff con DuckDB.

Utiliza el motor vectorizado en memoria de DuckDB para consultar e interrogar
los 3 JSONs del Prospector Externo (map.json, map_metadata.json, map_dictionary.json)
a velocidad sub-milisegundo.
"""

from pathlib import Path
import time
from typing import Any

import duckdb

from prospector_interno.adapters.api.schemas import (
    ReconciliationDiffItem,
    ReconciliationDiffResponse,
)


class DuckDBReconciliationService:
    """Motor de consulta y cruce analítico rápido sobre archivos JSON."""

    def __init__(self) -> None:
        self._con = duckdb.connect(":memory:")

    def diff_with_external_map(
        self,
        source_code: str,
        json_path: str | Path,
        search_pattern: str | None = None,
    ) -> ReconciliationDiffResponse:
        """Consulta directamente el archivo JSON del Prospector Externo con DuckDB."""
        start_time = time.perf_counter()
        target_path = Path(json_path)

        if not target_path.exists():
            duration_ms = (time.perf_counter() - start_time) * 1000
            return ReconciliationDiffResponse(
                source_code=source_code,
                matches_count=0,
                duration_ms=round(duration_ms, 2),
                items=[],
            )

        # Consulta SQL vectorizada directa sobre el archivo JSON
        # DuckDB infiere el esquema y realiza proyección columnar
        query = f"SELECT * FROM read_json_auto('{target_path.as_posix()}')"
        params: list[Any] = []

        if search_pattern:
            query += " WHERE url ILIKE ?"
            params.append(search_pattern)

        query += " LIMIT 100"

        try:
            rel = self._con.execute(query, params)
            df = rel.fetchall()
            columns = [desc[0] for desc in rel.description]

            items: list[ReconciliationDiffItem] = []
            for row in df:
                row_dict = dict(zip(columns, row))
                url = str(row_dict.get("url", ""))
                length = row_dict.get("content_length")
                modified = row_dict.get("last_modified")
                ext = row_dict.get("file_extension")

                items.append(
                    ReconciliationDiffItem(
                        url=url,
                        content_length=int(length) if length is not None else None,
                        last_modified=str(modified) if modified else None,
                        file_extension=str(ext) if ext else None,
                        status="VERIFIED_IN_MAP",
                    )
                )

            duration_ms = (time.perf_counter() - start_time) * 1000
            return ReconciliationDiffResponse(
                source_code=source_code,
                matches_count=len(items),
                duration_ms=round(duration_ms, 2),
                items=items,
            )

        except Exception:
            duration_ms = (time.perf_counter() - start_time) * 1000
            # En caso de error de sintaxis JSON, retornar respuesta vacía controlada
            return ReconciliationDiffResponse(
                source_code=source_code,
                matches_count=0,
                duration_ms=round(duration_ms, 2),
                items=[],
            )
