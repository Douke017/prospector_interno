"""Repositorios relacionales sobre PostgreSQL para el Prospector Interno.

Centraliza la persistencia operativa en la réplica PostgreSQL (`platform_db`):
- Seguimiento en tiempo real de cola y descargas (`download_tracking`)
- Catálogo técnico de activos e inventario (`internal_assets`, `canonical_*`)
- Trazabilidad y linaje criptográfico (`data_lineage`)
- Curaduría e inventario unificado de URLs (`url_inventory_map`, `file`)
- Diccionario técnico dinámico y relaciones (`semantic_dictionary`, `asset_relationships`)
- Llaves de acceso para autoservicio Excel / Power BI y bitácora (`api_keys`, `access_logs`)
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import secrets
from typing import Any

from prospector_interno.adapters.storage.db_connection import (
    DatabaseConfig,
    get_readwrite_connection,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.contracts.lineage_spec import (
    DataLineageRecord,
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.models import (
    DatasetSpecification,
    Periodicity,
    SourceDefinition,
)
from prospector_interno.domain.ports import (
    CatalogRepositoryPort,
    DownloadTrackingPort,
    LineageRepositoryPort,
)


class PostgresDownloadTrackingAdapter(DownloadTrackingPort):
    """Implementación PostgreSQL de DownloadTrackingPort para monitoreo en vivo."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def record_download(self, record: DownloadRecord) -> None:
        """Registra o actualiza una transferencia física en download_tracking."""
        subscribers_json = json.dumps(record.subscribers)
        started_str = record.started_at.isoformat() if record.started_at else None
        completed_str = record.completed_at.isoformat() if record.completed_at else None

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.download_tracking (
                        download_id, source_id, canonical_url, local_path,
                        status, sha256_checksum, bytes_downloaded, total_bytes,
                        consecutive_failures, last_error_code, task_id,
                        subscribers, subscribers_jsonb, started_at, completed_at,
                        priority, speed_bps, retry_count, max_retries, next_retry_at,
                        worker_id, resource_key, dataset_code, period_label, etag,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s::jsonb, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        NOW(), NOW()
                    )
                    ON CONFLICT (download_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        task_id = EXCLUDED.task_id,
                        local_path = COALESCE(EXCLUDED.local_path, public.download_tracking.local_path),
                        sha256_checksum = COALESCE(EXCLUDED.sha256_checksum, public.download_tracking.sha256_checksum),
                        bytes_downloaded = EXCLUDED.bytes_downloaded,
                        total_bytes = EXCLUDED.total_bytes,
                        consecutive_failures = EXCLUDED.consecutive_failures,
                        last_error_code = EXCLUDED.last_error_code,
                        priority = EXCLUDED.priority,
                        speed_bps = EXCLUDED.speed_bps,
                        retry_count = EXCLUDED.retry_count,
                        max_retries = EXCLUDED.max_retries,
                        next_retry_at = EXCLUDED.next_retry_at,
                        worker_id = COALESCE(EXCLUDED.worker_id, public.download_tracking.worker_id),
                        resource_key = COALESCE(EXCLUDED.resource_key, public.download_tracking.resource_key),
                        dataset_code = COALESCE(EXCLUDED.dataset_code, public.download_tracking.dataset_code),
                        period_label = COALESCE(EXCLUDED.period_label, public.download_tracking.period_label),
                        etag = COALESCE(EXCLUDED.etag, public.download_tracking.etag),
                        subscribers = EXCLUDED.subscribers,
                        subscribers_jsonb = EXCLUDED.subscribers_jsonb,
                        started_at = COALESCE(EXCLUDED.started_at, public.download_tracking.started_at),
                        completed_at = COALESCE(EXCLUDED.completed_at, public.download_tracking.completed_at),
                        updated_at = NOW();
                    """,
                    (
                        record.download_id,
                        record.source_id,
                        record.canonical_url,
                        record.local_path,
                        record.status.value,
                        record.sha256_checksum,
                        record.bytes_downloaded,
                        record.total_bytes,
                        record.consecutive_failures,
                        record.last_error_code,
                        record.task_id,
                        subscribers_json,
                        subscribers_json,
                        started_str,
                        completed_str,
                        record.priority,
                        record.speed_bps,
                        record.retry_count,
                        record.max_retries,
                        record.next_retry_at,
                        record.worker_id,
                        record.resource_key,
                        record.dataset_code,
                        record.period_label,
                        record.etag,
                    ),
                )
            conn.commit()

    def update_status(
        self,
        download_id: str,
        status: DownloadLifecycleStatus,
        bytes_downloaded: int | None = None,
        total_bytes: int | None = None,
        local_path: str | None = None,
        sha256_checksum: str | None = None,
        error_code: str | None = None,
        speed_bps: float | None = None,
        retry_count: int | None = None,
        worker_id: str | None = None,
    ) -> None:
        """Actualiza el estado y avance de una descarga en PostgreSQL."""
        updates: list[str] = ["status = %s", "updated_at = NOW()"]
        params: list[Any] = [status.value]

        if bytes_downloaded is not None:
            updates.append("bytes_downloaded = %s")
            params.append(bytes_downloaded)
        if total_bytes is not None:
            updates.append("total_bytes = %s")
            params.append(total_bytes)
        if local_path is not None:
            updates.append("local_path = %s")
            params.append(local_path)
        if sha256_checksum is not None:
            updates.append("sha256_checksum = %s")
            params.append(sha256_checksum)
        if error_code is not None:
            updates.append("last_error_code = %s")
            params.append(error_code)
        if speed_bps is not None:
            updates.append("speed_bps = %s")
            params.append(speed_bps)
        if retry_count is not None:
            updates.append("retry_count = %s")
            params.append(retry_count)
        if worker_id is not None:
            updates.append("worker_id = %s")
            params.append(worker_id)

        if status == DownloadLifecycleStatus.DOWNLOADING:
            updates.append("started_at = COALESCE(started_at, %s)")
            params.append(datetime.now(timezone.utc).isoformat())

        if status in (
            DownloadLifecycleStatus.COMPLETED,
            DownloadLifecycleStatus.DEDUPLICATED,
            DownloadLifecycleStatus.FAILED,
            DownloadLifecycleStatus.CANCELLED,
        ):
            updates.append("completed_at = %s")
            params.append(datetime.now(timezone.utc).isoformat())

        params.append(download_id)
        sql = f"UPDATE public.download_tracking SET {', '.join(updates)} WHERE download_id = %s"

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
            conn.commit()

    def claim_next_pending(self, worker_id: str) -> DownloadRecord | None:
        """Reclama atómicamente la siguiente descarga en cola mediante FOR UPDATE SKIP LOCKED."""
        now_iso = datetime.now(timezone.utc).isoformat()
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH next_job AS (
                        SELECT download_id
                        FROM public.download_tracking
                        WHERE status IN ('QUEUED', 'PENDING')
                          AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                        ORDER BY priority ASC, created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE public.download_tracking dt
                    SET status = 'DOWNLOADING',
                        worker_id = %s,
                        started_at = COALESCE(dt.started_at, %s),
                        updated_at = NOW()
                    FROM next_job
                    WHERE dt.download_id = next_job.download_id
                    RETURNING dt.*;
                    """,
                    (worker_id, now_iso),
                )
                row = cur.fetchone()
            conn.commit()
        return self._row_to_record(row) if row else None

    def get_download(self, download_id: str) -> DownloadRecord | None:
        """Obtiene el detalle de una descarga por su ID."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.download_tracking WHERE download_id = %s;",
                    (download_id,),
                )
                row = cur.fetchone()
                return self._row_to_record(row) if row else None

    def find_by_url(self, canonical_url: str) -> DownloadRecord | None:
        """Busca el último registro asociado a una URL canónica."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.download_tracking
                    WHERE canonical_url = %s
                    ORDER BY started_at DESC NULLS LAST
                    LIMIT 1;
                    """,
                    (canonical_url,),
                )
                row = cur.fetchone()
                return self._row_to_record(row) if row else None

    def subscribe_request(self, download_id: str, request_id: str) -> bool:
        """Suscribe una solicitud concurrente a una descarga en cola o en curso."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT subscribers FROM public.download_tracking WHERE download_id = %s FOR UPDATE;",
                    (download_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False
                try:
                    subs = json.loads(row["subscribers"] or "[]")
                except (TypeError, json.JSONDecodeError):
                    subs = []
                if request_id not in subs:
                    subs.append(request_id)
                    cur.execute(
                        "UPDATE public.download_tracking SET subscribers = %s WHERE download_id = %s;",
                        (json.dumps(subs), download_id),
                    )
            conn.commit()
        return True

    def list_active_downloads(self) -> list[DownloadRecord]:
        """Lista todas las descargas activas o en cola."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.download_tracking
                    WHERE status IN ('QUEUED', 'PENDING', 'DOWNLOADING', 'VERIFYING_SHA256')
                    ORDER BY priority ASC, started_at DESC NULLS LAST;
                    """
                )
                rows = cur.fetchall()
                return [self._row_to_record(r) for r in rows]

    def list_recent_downloads(
        self,
        limit: int = 30,
        source_id: str | None = None,
    ) -> list[DownloadRecord]:
        """Lista tanto descargas activas como las más recientes para monitoreo."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                if source_id:
                    cur.execute(
                        """
                        SELECT * FROM public.download_tracking
                        WHERE UPPER(source_id) = UPPER(%s)
                        ORDER BY
                            CASE WHEN status IN ('DOWNLOADING', 'VERIFYING_SHA256', 'QUEUED', 'PENDING') THEN 0 ELSE 1 END,
                            COALESCE(completed_at, started_at) DESC NULLS LAST
                        LIMIT %s;
                        """,
                        (source_id, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM public.download_tracking
                        ORDER BY
                            CASE WHEN status IN ('DOWNLOADING', 'VERIFYING_SHA256', 'QUEUED', 'PENDING') THEN 0 ELSE 1 END,
                            COALESCE(completed_at, started_at) DESC NULLS LAST
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                rows = cur.fetchall()
                return [self._row_to_record(r) for r in rows]

    def find_by_checksum(self, sha256_checksum: str) -> DownloadRecord | None:
        """Localiza una descarga previa por su firma SHA-256."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.download_tracking WHERE sha256_checksum = %s LIMIT 1;",
                    (sha256_checksum,),
                )
                row = cur.fetchone()
                return self._row_to_record(row) if row else None

    @staticmethod
    def _row_to_record(row: dict[str, Any]) -> DownloadRecord:
        started = datetime.fromisoformat(row["started_at"]) if row.get("started_at") else None
        completed = (
            datetime.fromisoformat(row["completed_at"]) if row.get("completed_at") else None
        )
        try:
            subscribers = json.loads(row.get("subscribers") or "[]")
        except (TypeError, json.JSONDecodeError):
            subscribers = []

        return DownloadRecord(
            download_id=row["download_id"],
            source_id=row["source_id"],
            canonical_url=row["canonical_url"],
            local_path=row.get("local_path"),
            status=DownloadLifecycleStatus(row["status"]),
            sha256_checksum=row.get("sha256_checksum"),
            bytes_downloaded=int(row.get("bytes_downloaded") or 0),
            total_bytes=int(row.get("total_bytes") or 0),
            consecutive_failures=int(row.get("consecutive_failures") or 0),
            last_error_code=row.get("last_error_code"),
            task_id=row.get("task_id"),
            subscribers=subscribers,
            started_at=started,
            completed_at=completed,
            priority=int(row.get("priority") or 5),
            speed_bps=float(row.get("speed_bps") or 0.0),
            retry_count=int(row.get("retry_count") or 0),
            max_retries=int(row.get("max_retries") if row.get("max_retries") is not None else 3),
            next_retry_at=row.get("next_retry_at"),
            worker_id=row.get("worker_id"),
            resource_key=row.get("resource_key"),
            dataset_code=row.get("dataset_code"),
            period_label=row.get("period_label"),
            etag=row.get("etag"),
        )


class PostgresLineageAndCatalogAdapter(LineageRepositoryPort, CatalogRepositoryPort):
    """Repositorio PostgreSQL para activos consolidados, linaje SHA-256 y catálogo."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def record_asset(self, asset: InternalAssetRecord) -> None:
        """Registra o actualiza un activo consolidado en internal_assets."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.internal_assets (
                        asset_id, source_id, dataset_id, period_start, period_end,
                        storage_layer, file_path, content_hash_sha256, row_count,
                        size_bytes, created_at, last_updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_id) DO UPDATE SET
                        source_id = EXCLUDED.source_id,
                        dataset_id = EXCLUDED.dataset_id,
                        period_start = EXCLUDED.period_start,
                        period_end = EXCLUDED.period_end,
                        storage_layer = EXCLUDED.storage_layer,
                        file_path = EXCLUDED.file_path,
                        content_hash_sha256 = EXCLUDED.content_hash_sha256,
                        row_count = EXCLUDED.row_count,
                        size_bytes = EXCLUDED.size_bytes,
                        last_updated_at = EXCLUDED.last_updated_at;
                    """,
                    (
                        asset.asset_id,
                        asset.source_id,
                        asset.dataset_id,
                        asset.period_start,
                        asset.period_end,
                        asset.storage_layer.value,
                        asset.file_path,
                        asset.content_hash_sha256,
                        asset.row_count,
                        asset.size_bytes,
                        asset.created_at.isoformat(),
                        asset.last_updated_at.isoformat(),
                    ),
                )
            conn.commit()

    def get_asset(self, asset_id: str) -> InternalAssetRecord | None:
        """Obtiene un activo por su identificador único."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.internal_assets WHERE asset_id = %s;",
                    (asset_id,),
                )
                row = cur.fetchone()
                return self._row_to_asset(row) if row else None

    def list_assets(
        self,
        storage_layer: str | None = None,
        source_id: str | None = None,
    ) -> list[InternalAssetRecord]:
        """Lista los activos catalogados filtrando por capa o entidad."""
        query = "SELECT * FROM public.internal_assets WHERE 1=1"
        params: list[Any] = []
        if storage_layer:
            query += " AND storage_layer = %s"
            params.append(storage_layer.upper())
        if source_id:
            query += " AND LOWER(source_id) = LOWER(%s)"
            params.append(source_id)
        query += " ORDER BY period_start DESC, asset_id ASC;"

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall()
                return [self._row_to_asset(r) for r in rows]

    def find_by_specification(self, specification: Any) -> list[InternalAssetRecord]:
        """Evalúa una especificación componible sobre PostgreSQL."""
        if hasattr(specification, "to_sql_criteria"):
            sql_criteria, params = specification.to_sql_criteria()
            pg_criteria = sql_criteria.replace("?", "%s")
            query = (
                f"SELECT * FROM public.internal_assets WHERE {pg_criteria} "
                f"ORDER BY period_start DESC, asset_id ASC;"
            )
            with get_readwrite_connection(self._config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, tuple(params))
                    rows = cur.fetchall()
                    return [self._row_to_asset(r) for r in rows]

        all_assets = self.list_assets()
        return [
            a for a in all_assets
            if getattr(specification, "is_satisfied_by", lambda _: True)(a)
        ]

    def record_lineage(self, lineage: DataLineageRecord) -> None:
        """Persiste un vínculo inmutable de linaje Gold -> Bronze."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.data_lineage (
                        lineage_id, gold_asset_id, bronze_source_file,
                        bronze_file_sha256, external_run_id, pipeline_version,
                        transformed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lineage_id) DO UPDATE SET
                        transformed_at = EXCLUDED.transformed_at;
                    """,
                    (
                        lineage.lineage_id,
                        lineage.gold_asset_id,
                        lineage.bronze_source_file,
                        lineage.bronze_file_sha256,
                        lineage.external_run_id,
                        lineage.pipeline_version,
                        lineage.transformed_at.isoformat(),
                    ),
                )
            conn.commit()

    def get_lineage_by_asset(self, gold_asset_id: str) -> list[DataLineageRecord]:
        """Recupera los insumos Bronze vinculados a un activo Gold."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.data_lineage
                    WHERE gold_asset_id = %s
                    ORDER BY transformed_at ASC;
                    """,
                    (gold_asset_id,),
                )
                rows = cur.fetchall()
                return [self._row_to_lineage(r) for r in rows]

    def get_lineage_by_checksum(self, bronze_file_sha256: str) -> list[DataLineageRecord]:
        """Recupera activos Gold derivados de un archivo Bronze por su SHA-256."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM public.data_lineage
                    WHERE bronze_file_sha256 = %s
                    ORDER BY transformed_at ASC;
                    """,
                    (bronze_file_sha256,),
                )
                rows = cur.fetchall()
                return [self._row_to_lineage(r) for r in rows]

    def list_lineage_for_datamart(
        self,
        datamart_name: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Retorna el detalle de linaje enriquecido para un repositorio consolidado."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        a.asset_id,
                        a.dataset_id AS datamart_name,
                        a.source_id,
                        a.period_start,
                        a.row_count,
                        a.content_hash_sha256 AS gold_sha256,
                        l.lineage_id,
                        l.bronze_source_file,
                        l.bronze_file_sha256,
                        l.external_run_id,
                        l.pipeline_version,
                        l.transformed_at,
                        u.canonical_url
                    FROM public.internal_assets a
                    JOIN public.data_lineage l ON a.asset_id = l.gold_asset_id
                    LEFT JOIN public.url_inventory_map u ON u.sha256_hash = l.bronze_file_sha256
                    WHERE a.dataset_id = %s
                    ORDER BY a.period_start DESC, l.transformed_at DESC
                    LIMIT %s;
                    """,
                    (datamart_name, limit),
                )
                return list(cur.fetchall())

    def save_source(self, source: SourceDefinition) -> None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.canonical_sources (
                        source_id, canonical_code, official_name, short_name,
                        homepage, country_iso, is_active, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id) DO UPDATE SET
                        canonical_code = EXCLUDED.canonical_code,
                        official_name = EXCLUDED.official_name,
                        short_name = EXCLUDED.short_name,
                        homepage = EXCLUDED.homepage,
                        country_iso = EXCLUDED.country_iso,
                        is_active = EXCLUDED.is_active,
                        notes = EXCLUDED.notes;
                    """,
                    (
                        source.source_id,
                        source.canonical_code,
                        source.official_name,
                        source.short_name,
                        source.homepage,
                        source.country_iso,
                        source.is_active,
                        source.notes,
                    ),
                )
            conn.commit()

    def get_source_by_id(self, source_id: int) -> SourceDefinition | None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.canonical_sources WHERE source_id = %s;",
                    (source_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return SourceDefinition(
                    source_id=row["source_id"],
                    canonical_code=row["canonical_code"],
                    official_name=row["official_name"],
                    short_name=row["short_name"],
                    homepage=row["homepage"],
                    country_iso=row["country_iso"],
                    is_active=bool(row["is_active"]),
                    notes=row["notes"],
                )

    def get_source_by_code(self, canonical_code: str) -> SourceDefinition | None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.canonical_sources WHERE UPPER(canonical_code) = UPPER(%s);",
                    (canonical_code,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return SourceDefinition(
                    source_id=row["source_id"],
                    canonical_code=row["canonical_code"],
                    official_name=row["official_name"],
                    short_name=row["short_name"],
                    homepage=row["homepage"],
                    country_iso=row["country_iso"],
                    is_active=bool(row["is_active"]),
                    notes=row["notes"],
                )

    def list_sources(self) -> list[SourceDefinition]:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM public.canonical_sources ORDER BY source_id;")
                rows = cur.fetchall()
                return [
                    SourceDefinition(
                        source_id=r["source_id"],
                        canonical_code=r["canonical_code"],
                        official_name=r["official_name"],
                        short_name=r["short_name"],
                        homepage=r["homepage"],
                        country_iso=r["country_iso"],
                        is_active=bool(r["is_active"]),
                        notes=r["notes"],
                    )
                    for r in rows
                ]

    def save_dataset(self, dataset: DatasetSpecification) -> None:
        periodicity_val = (
            dataset.periodicity.value
            if hasattr(dataset.periodicity, "value")
            else str(dataset.periodicity)
        )
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.canonical_datasets (
                        dataset_code, source_id, legacy_file_code, name,
                        periodicity, url_template, expected_format, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dataset_code) DO UPDATE SET
                        source_id = EXCLUDED.source_id,
                        legacy_file_code = EXCLUDED.legacy_file_code,
                        name = EXCLUDED.name,
                        periodicity = EXCLUDED.periodicity,
                        url_template = EXCLUDED.url_template,
                        expected_format = EXCLUDED.expected_format,
                        is_active = EXCLUDED.is_active;
                    """,
                    (
                        dataset.dataset_code,
                        dataset.source_id,
                        dataset.legacy_file_code or "",
                        dataset.name,
                        periodicity_val,
                        dataset.url_template,
                        dataset.expected_format,
                        dataset.is_active,
                    ),
                )
            conn.commit()

    def get_dataset(self, dataset_code: str) -> DatasetSpecification | None:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.canonical_datasets WHERE UPPER(dataset_code) = UPPER(%s);",
                    (dataset_code,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return DatasetSpecification(
                    dataset_code=row["dataset_code"],
                    source_id=row["source_id"],
                    legacy_file_code=row["legacy_file_code"],
                    name=row["name"],
                    periodicity=Periodicity(row["periodicity"]),
                    url_template=row["url_template"],
                    expected_format=row["expected_format"],
                    is_active=bool(row["is_active"]),
                )

    def list_datasets(self, source_id: int | None = None) -> list[DatasetSpecification]:
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                if source_id is not None:
                    cur.execute(
                        "SELECT * FROM public.canonical_datasets WHERE source_id = %s ORDER BY dataset_code;",
                        (source_id,),
                    )
                else:
                    cur.execute(
                        "SELECT * FROM public.canonical_datasets ORDER BY dataset_code;"
                    )
                rows = cur.fetchall()
                return [
                    DatasetSpecification(
                        dataset_code=r["dataset_code"],
                        source_id=r["source_id"],
                        legacy_file_code=r["legacy_file_code"],
                        name=r["name"],
                        periodicity=Periodicity(r["periodicity"]),
                        url_template=r["url_template"],
                        expected_format=r["expected_format"],
                        is_active=bool(r["is_active"]),
                    )
                    for r in rows
                ]

    @staticmethod
    def _row_to_asset(row: dict[str, Any]) -> InternalAssetRecord:
        return InternalAssetRecord(
            asset_id=row["asset_id"],
            source_id=row["source_id"],
            dataset_id=row["dataset_id"],
            period_start=row["period_start"],
            period_end=row["period_end"],
            storage_layer=StorageLayer(row["storage_layer"]),
            file_path=row["file_path"],
            content_hash_sha256=row["content_hash_sha256"],
            row_count=row["row_count"],
            size_bytes=int(row["size_bytes"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            last_updated_at=datetime.fromisoformat(row["last_updated_at"]),
        )

    @staticmethod
    def _row_to_lineage(row: dict[str, Any]) -> DataLineageRecord:
        return DataLineageRecord(
            lineage_id=row["lineage_id"],
            gold_asset_id=row["gold_asset_id"],
            bronze_source_file=row["bronze_source_file"],
            bronze_file_sha256=row["bronze_file_sha256"],
            external_run_id=row["external_run_id"],
            pipeline_version=row["pipeline_version"],
            transformed_at=datetime.fromisoformat(row["transformed_at"]),
        )


class PostgresUrlInventoryRepository:
    """Repositorio PostgreSQL para curaduría e inventario unificado de URLs."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def find_canonical_url(
        self,
        dataset_code: str,
        source_id: str | None = None,
        period: str | None = None,
        domain: str | None = None,
    ) -> str | None:
        """Resuelve una URL real desde el inventario evitando URLs sintéticas."""
        clean_ds = dataset_code.strip()
        inferred_src = (source_id or "").strip()
        if not inferred_src and clean_ds.upper().startswith("DS_BO_"):
            parts = clean_ds.split("_")
            if len(parts) >= 3 and parts[2]:
                inferred_src = parts[2]

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                # 1. Por coincidencia con mv_nexus_active_catalog (si existe vista legacy)
                cur.execute("SAVEPOINT sp_mv_lookup;")
                row = None
                try:
                    cur.execute(
                        """
                        SELECT target_url FROM public.mv_nexus_active_catalog
                        WHERE UPPER(canonical_dataset_code) = UPPER(%s)
                          AND target_url IS NOT NULL AND target_url <> ''
                        LIMIT 1;
                        """,
                        (clean_ds,),
                    )
                    row = cur.fetchone()
                except psycopg.Error:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_mv_lookup;")
                if row and row.get("target_url"):
                    return str(row["target_url"])

                # 2. En url_inventory_map restringiendo siempre por fuente o dominio
                if not inferred_src and not domain:
                    return None
                params: list[Any] = []
                clauses = ["canonical_url <> ''"]
                if inferred_src:
                    clauses.append("UPPER(source_id) = UPPER(%s)")
                    params.append(inferred_src)
                elif domain:
                    clauses.append("canonical_url ILIKE %s")
                    params.append(f"%{domain.strip()}%")
                if period and period != "LATEST":
                    clauses.append("(canonical_url ILIKE %s OR file_name ILIKE %s)")
                    params.extend([f"%{period}%", f"%{period}%"])
                where_sql = " AND ".join(clauses)
                cur.execute(
                    f"""
                    SELECT canonical_url FROM public.url_inventory_map
                    WHERE {where_sql}
                    ORDER BY is_downloaded DESC, id ASC
                    LIMIT 1;
                    """,
                    tuple(params),
                )
                inv_row = cur.fetchone()
                if inv_row and inv_row.get("canonical_url"):
                    return str(inv_row["canonical_url"])
        return None

    def get_downloaded_url_set(self, source_id: str | None = None) -> set[str]:
        """Retorna el conjunto de URLs ya descargadas en PostgreSQL (url_inventory_map)."""
        try:
            with get_readwrite_connection(self._config) as conn:
                with conn.cursor() as cur:
                    if source_id and source_id.strip().upper() != "ALL":
                        cur.execute(
                            """
                            SELECT canonical_url
                            FROM public.url_inventory_map
                            WHERE is_downloaded = TRUE
                              AND UPPER(source_id) = UPPER(%s);
                            """,
                            (source_id.strip(),),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT canonical_url
                            FROM public.url_inventory_map
                            WHERE is_downloaded = TRUE;
                            """
                        )
                    return {
                        str(r["canonical_url"]).strip()
                        for r in cur.fetchall()
                        if r.get("canonical_url")
                    }
        except Exception:
            return set()

    def upsert_downloaded_url(
        self,
        source_id: str,
        canonical_url: str,
        sha256_hash: str,
        storage_path: str,
        gold_target: str | None = None,
    ) -> None:
        """Registra o actualiza una URL custodiada en Bronze dentro de PostgreSQL."""
        url_hash = hashlib.sha256(canonical_url.strip().encode("utf-8")).hexdigest()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.url_inventory_map
                    SET source_id = UPPER(source_id),
                        is_downloaded = TRUE,
                        sha256_hash = %s,
                        storage_path = %s,
                        gold_target = COALESCE(%s, gold_target),
                        status = 'DESCARGADO_BRONZE',
                        updated_at = %s
                    WHERE canonical_url = %s;
                    """,
                    (
                        sha256_hash,
                        storage_path,
                        gold_target,
                        now_str,
                        canonical_url,
                    ),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO public.url_inventory_map (
                            source_id, file_name, canonical_url, normalized_url_sha256,
                            is_discovered_web, is_downloaded, is_processed,
                            sha256_hash, storage_path, gold_target, status, updated_at
                        ) VALUES (%s, %s, %s, %s, TRUE, TRUE, FALSE, %s, %s, %s, 'DESCARGADO_BRONZE', %s)
                        ON CONFLICT (normalized_url_sha256) DO UPDATE SET
                            source_id = UPPER(EXCLUDED.source_id),
                            is_downloaded = TRUE,
                            sha256_hash = EXCLUDED.sha256_hash,
                            storage_path = EXCLUDED.storage_path,
                            gold_target = COALESCE(EXCLUDED.gold_target, public.url_inventory_map.gold_target),
                            status = 'DESCARGADO_BRONZE',
                            updated_at = EXCLUDED.updated_at;
                        """,
                        (
                            source_id.upper(),
                            f"doc_{sha256_hash[:10]}",
                            canonical_url,
                            url_hash,
                            sha256_hash,
                            storage_path,
                            gold_target,
                            now_str,
                        ),
                    )
            conn.commit()

    def mark_processed_by_sha256(
        self,
        sha256_hashes: set[str],
        gold_target: str,
        row_count: int | None = None,
    ) -> int:
        """Marca como procesados los archivos custodiados cuyos hashes ingresaron a Gold."""
        if not sha256_hashes:
            return 0
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        hashes_list = list(sha256_hashes)
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.url_inventory_map
                    SET is_downloaded = TRUE,
                        is_processed = TRUE,
                        gold_target = %s,
                        status = 'CONSOLIDADO_GOLD',
                        updated_at = %s
                    WHERE sha256_hash = ANY(%s);
                    """,
                    (gold_target, now_str, hashes_list),
                )
                updated_rows = cur.rowcount or 0
                if row_count is not None:
                    cur.execute("SAVEPOINT sp_update_legacy_file;")
                    try:
                        cur.execute(
                            """
                            UPDATE public.file
                            SET is_downloaded = TRUE,
                                is_processed = TRUE,
                                gold_table_target = %s,
                                row_count = COALESCE(%s, row_count)
                            WHERE code IN (
                                SELECT file_code FROM public.url_inventory_map
                                WHERE sha256_hash = ANY(%s) AND file_code IS NOT NULL
                            );
                            """,
                            (gold_target, row_count, hashes_list),
                        )
                    except psycopg.Error:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_update_legacy_file;")
            conn.commit()
        return updated_rows


class PostgresSemanticDictionaryRepository:
    """Repositorio PostgreSQL para el diccionario técnico y relaciones entre repositorios."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def list_datamart_columns(self, datamart_name: str | None = None) -> list[dict[str, Any]]:
        """Consulta las definiciones de variables para uno o los 5 repositorios multi-fuente."""
        from prospector_interno.application.pipelines.multi_source_consolidator import (
            MULTI_SOURCE_DICTIONARY,
        )

        if datamart_name:
            dm_clean = datamart_name.strip().lower()
            canonical = [
                dict(c)
                for c in MULTI_SOURCE_DICTIONARY
                if c["datamart_name"] == dm_clean
            ]
            if canonical:
                return canonical

        if datamart_name is None:
            return [dict(c) for c in MULTI_SOURCE_DICTIONARY]

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        datamart_name, column_name, business_label,
                        data_type, unit, formula, description,
                        is_primary_metric, display_order
                    FROM public.semantic_dictionary
                    WHERE datamart_name = %s
                    ORDER BY display_order ASC, id ASC;
                    """,
                    (datamart_name,),
                )
                return list(cur.fetchall())

    def list_relationships(self, datamart_name: str | None = None) -> list[dict[str, Any]]:
        """Retorna las relaciones formales de integración entre los 5 repositorios."""
        from prospector_interno.application.pipelines.multi_source_consolidator import (
            MULTI_SOURCE_RELATIONSHIPS,
        )

        if datamart_name:
            dm_clean = datamart_name.strip().lower()
            filtered = [
                dict(r)
                for r in MULTI_SOURCE_RELATIONSHIPS
                if r["from_datamart"] == dm_clean or r["to_datamart"] == dm_clean
            ]
            if filtered:
                return filtered
        else:
            return [dict(r) for r in MULTI_SOURCE_RELATIONSHIPS]

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, from_datamart, from_column,
                        to_datamart, to_column, relationship_type, business_rule,
                        business_rule AS business_meaning
                    FROM public.asset_relationships
                    WHERE from_datamart = %s OR to_datamart = %s
                    ORDER BY id ASC;
                    """,
                    (datamart_name, datamart_name),
                )
                return list(cur.fetchall())


class PostgresApiKeyRepository:
    """Gestión de llaves API sin login para conectores Excel Power Query y Power BI."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config

    def create_key(
        self,
        label: str,
        allowed_datamarts: str = "*",
        allowed_layers: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Crea una llave API con alcance por capa/dataset y expiración opcional."""
        clean_label = label.strip() or "Cliente Programático DATAX"
        raw_token = f"dtx_live_{secrets.token_hex(16)}"
        key_id = f"key_{secrets.token_hex(6)}"
        key_prefix = f"{raw_token[:13]}..."
        key_sha256 = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
        layers = [
            lyr.strip().upper()
            for lyr in (allowed_layers or ["RAW", "GOLD", "SQL"])
            if lyr.strip()
        ] or ["RAW", "GOLD", "SQL"]

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.api_keys (
                        key_id, label, key_prefix, key_sha256,
                        allowed_datamarts, allowed_layers, expires_at, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                    RETURNING key_id, label, key_prefix, allowed_datamarts,
                              allowed_layers, expires_at, revoked_at,
                              is_active, created_at, last_used_at, usage_count;
                    """,
                    (key_id, clean_label, key_prefix, key_sha256, allowed_datamarts, layers, expires_at),
                )
                row = dict(cur.fetchone() or {})
            conn.commit()

        row["api_key"] = raw_token
        row["raw_api_key"] = raw_token
        for dt_col in ("created_at", "last_used_at", "expires_at", "revoked_at"):
            val = row.get(dt_col)
            if hasattr(val, "isoformat"):
                row[dt_col] = val.isoformat()
        return row

    def list_keys(self) -> list[dict[str, Any]]:
        """Lista las llaves registradas sin exponer el secreto completo."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        key_id, label, key_prefix, allowed_datamarts,
                        allowed_layers, expires_at, revoked_at,
                        is_active, created_at, last_used_at, usage_count
                    FROM public.api_keys
                    ORDER BY created_at DESC;
                    """
                )
                rows = []
                for r in cur.fetchall():
                    item = dict(r)
                    for dt_col in ("created_at", "last_used_at", "expires_at", "revoked_at"):
                        val = item.get(dt_col)
                        if hasattr(val, "isoformat"):
                            item[dt_col] = val.isoformat()
                    rows.append(item)
                return rows

    def revoke_key(self, key_id: str) -> bool:
        """Desactiva inmediatamente una llave API registrando revoked_at."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.api_keys SET is_active = FALSE, revoked_at = NOW() WHERE key_id = %s;",
                    (key_id,),
                )
                changed = (cur.rowcount or 0) > 0
            conn.commit()
        return changed

    def _record_denied_log(
        self,
        *,
        key_id: str | None,
        key_label: str | None,
        datamart_name: str,
        export_format: str,
        client_agent: str | None,
        denial_reason: str,
        layer: str,
    ) -> None:
        try:
            with get_readwrite_connection(self._config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO public.access_logs (
                            key_id, key_label, datamart_name, export_format,
                            rows_returned, client_agent, status, denial_reason, layer
                        ) VALUES (%s, %s, %s, %s, 0, %s, 'DENIED', %s, %s);
                        """,
                        (
                            key_id,
                            key_label or "Llave no autorizada",
                            datamart_name,
                            export_format,
                            client_agent or "Cliente Externo",
                            denial_reason,
                            layer,
                        ),
                    )
                conn.commit()
        except Exception:
            return

    def authorize_and_log(
        self,
        *,
        raw_key: str,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
        required_layer: str = "GOLD",
    ) -> tuple[dict[str, Any] | None, int, str | None]:
        """Valida vigencia, revocación, capa y dataset; registra permitidos y denegados."""
        layer_u = required_layer.strip().upper()
        if not raw_key or not raw_key.strip():
            self._record_denied_log(
                key_id=None,
                key_label=None,
                datamart_name=datamart_name,
                export_format=export_format,
                client_agent=client_agent,
                denial_reason="MISSING_API_KEY",
                layer=layer_u,
            )
            return None, 401, "Llave API ausente."

        key_sha256 = hashlib.sha256(raw_key.strip().encode("utf-8")).hexdigest()
        now_utc = datetime.now(timezone.utc)

        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT key_id, label, allowed_datamarts, allowed_layers,
                           expires_at, is_active
                    FROM public.api_keys
                    WHERE key_sha256 = %s;
                    """,
                    (key_sha256,),
                )
                key_row = cur.fetchone()
                if not key_row:
                    self._record_denied_log(
                        key_id=None,
                        key_label=None,
                        datamart_name=datamart_name,
                        export_format=export_format,
                        client_agent=client_agent,
                        denial_reason="INVALID_API_KEY",
                        layer=layer_u,
                    )
                    return None, 401, "Llave API inválida."

                if not key_row["is_active"]:
                    self._record_denied_log(
                        key_id=key_row["key_id"],
                        key_label=key_row["label"],
                        datamart_name=datamart_name,
                        export_format=export_format,
                        client_agent=client_agent,
                        denial_reason="REVOKED_API_KEY",
                        layer=layer_u,
                    )
                    return None, 401, "Llave API revocada."

                exp = key_row.get("expires_at")
                if exp is not None:
                    exp_utc = exp if exp.tzinfo is not None else exp.replace(tzinfo=timezone.utc)
                    if exp_utc <= now_utc:
                        self._record_denied_log(
                            key_id=key_row["key_id"],
                            key_label=key_row["label"],
                            datamart_name=datamart_name,
                            export_format=export_format,
                            client_agent=client_agent,
                            denial_reason="EXPIRED_API_KEY",
                            layer=layer_u,
                        )
                        return None, 401, "Llave API expirada."

                allowed_layers = [
                    str(x).upper()
                    for x in (key_row.get("allowed_layers") or ["RAW", "GOLD", "SQL"])
                ]
                if "*" not in allowed_layers and layer_u not in allowed_layers:
                    self._record_denied_log(
                        key_id=key_row["key_id"],
                        key_label=key_row["label"],
                        datamart_name=datamart_name,
                        export_format=export_format,
                        client_agent=client_agent,
                        denial_reason=f"LAYER_FORBIDDEN:{layer_u}",
                        layer=layer_u,
                    )
                    return None, 403, f"Llave API sin permiso sobre la capa '{layer_u}'."

                allowed = str(key_row["allowed_datamarts"] or "*").strip()
                if allowed != "*" and datamart_name != "*":
                    allowed_set = {x.strip().lower() for x in allowed.split(",") if x.strip()}
                    if datamart_name.lower() not in allowed_set:
                        self._record_denied_log(
                            key_id=key_row["key_id"],
                            key_label=key_row["label"],
                            datamart_name=datamart_name,
                            export_format=export_format,
                            client_agent=client_agent,
                            denial_reason=f"DATAMART_FORBIDDEN:{datamart_name}",
                            layer=layer_u,
                        )
                        return None, 403, f"Llave API sin autorización para '{datamart_name}'."

                cur.execute(
                    """
                    UPDATE public.api_keys
                    SET last_used_at = NOW(), usage_count = usage_count + 1
                    WHERE key_id = %s;
                    """,
                    (key_row["key_id"],),
                )
                cur.execute(
                    """
                    INSERT INTO public.access_logs (
                        key_id, key_label, datamart_name, export_format,
                        rows_returned, client_agent, status, layer
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'ALLOWED', %s);
                    """,
                    (
                        key_row["key_id"],
                        key_row["label"],
                        datamart_name,
                        export_format,
                        rows_returned,
                        client_agent or "Cliente Programático DATAX",
                        layer_u,
                    ),
                )
            conn.commit()
        return dict(key_row), 200, None

    def validate_and_log(
        self,
        raw_key: str,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
    ) -> dict[str, Any] | None:
        entry, code, _ = self.authorize_and_log(
            raw_key=raw_key,
            datamart_name=datamart_name,
            export_format=export_format,
            rows_returned=rows_returned,
            client_agent=client_agent,
        )
        return entry if code == 200 else None

    def record_direct_export(
        self,
        datamart_name: str,
        export_format: str,
        rows_returned: int,
        client_agent: str | None = None,
    ) -> None:
        """Registra descargas directas desde la consola web en access_logs."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.access_logs (
                        key_id, key_label, datamart_name, export_format,
                        rows_returned, client_agent
                    ) VALUES (NULL, %s, %s, %s, %s, %s);
                    """,
                    (
                        "Consola Web de Autoservicio",
                        datamart_name,
                        export_format,
                        rows_returned,
                        client_agent or "Navegador Analista",
                    ),
                )
            conn.commit()

    def list_access_logs(self, limit: int = 30) -> list[dict[str, Any]]:
        """Retorna el historial reciente de extracciones de autoservicio."""
        with get_readwrite_connection(self._config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        log_id, key_id, key_label, datamart_name,
                        export_format, rows_returned, client_agent,
                        status, denial_reason, layer, accessed_at
                    FROM public.access_logs
                    ORDER BY accessed_at DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                rows = []
                for r in cur.fetchall():
                    item = dict(r)
                    if hasattr(item.get("accessed_at"), "isoformat"):
                        item["accessed_at"] = item["accessed_at"].isoformat()
                    rows.append(item)
                return rows
