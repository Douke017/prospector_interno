"""Adaptador de Ingesta y Normalización desde la Base de Datos Legacy (SQL).

Extrae fuentes, definiciones de archivos y registros históricos desde
el PostgreSQL legacy y los transforma en modelos canónicos para Portal Nexus.
"""

from contextlib import contextmanager
from datetime import datetime
import hashlib
from pathlib import Path
import re
import sqlite3
from typing import Any, Generator

import psycopg

from prospector_interno.adapters.storage.db_connection import (
    DatabaseConfig,
    get_database_config,
    get_readonly_connection,
)
from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.exceptions import LegacySeederError
from prospector_interno.domain.models import (
    DatasetSpecification,
    Periodicity,
    SourceDefinition,
)
from prospector_interno.domain.ports import (
    CatalogRepositoryPort,
    DownloadTrackingPort,
)


class PostgresLegacySeeder:
    """Adaptador de migración que consulta PostgreSQL o Snapshot SQLite."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        config: DatabaseConfig | None = None,
        sqlite_snapshot_path: str | Path | None = None,
    ) -> None:
        self._sqlite_snapshot_path = (
            Path(sqlite_snapshot_path) if sqlite_snapshot_path else None
        )
        if config is not None:
            self._config = config
        else:
            base_cfg = get_database_config()
            self._config = DatabaseConfig(
                host=host if host is not None else base_cfg.host,
                port=port if port is not None else base_cfg.port,
                database=dbname if dbname is not None else base_cfg.database,
                user=user if user is not None else base_cfg.user,
                password=password if password is not None else base_cfg.password,
                connect_timeout=base_cfg.connect_timeout,
                read_only=True,
            )
        self._conn_info = self._config.conn_info

    @contextmanager
    def _get_connection(self) -> Generator[Any, None, None]:
        if self._sqlite_snapshot_path is not None:
            if not self._sqlite_snapshot_path.is_file():
                raise LegacySeederError(
                    f"Snapshot SQLite no encontrado: {self._sqlite_snapshot_path}"
                )
            conn = sqlite3.connect(
                str(self._sqlite_snapshot_path),
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()
        else:
            try:
                with get_readonly_connection(self._config) as conn:
                    yield conn
            except psycopg.Error as err:
                raise LegacySeederError(
                    f"No fue posible conectar a la base legacy: {err}"
                ) from err

    @staticmethod
    def _sanitize_code(text: str) -> str:

        """Convierte una cadena a un identificador seguro en mayúsculas."""
        cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", text.strip().upper())
        return cleaned.strip("_")

    @staticmethod
    def _map_periodicity(freq_str: str | None) -> Periodicity:
        """Normaliza cadenas arbitrarias de frecuencia al Enum Periodicity."""
        if not freq_str:
            return Periodicity.MONTHLY

        normalized = freq_str.strip().lower()
        if "diari" in normalized:
            return Periodicity.DAILY
        if "seman" in normalized:
            return Periodicity.WEEKLY
        if "mensua" in normalized:
            return Periodicity.MONTHLY
        if "trimestr" in normalized:
            return Periodicity.QUARTERLY
        if "anual" in normalized:
            return Periodicity.ANNUAL
        return Periodicity.ON_DEMAND

    def extract_sources(
        self,
        source_codes: list[str] | None = None,
    ) -> list[SourceDefinition]:
        """Extrae y normaliza las entidades emisoras de la tabla `source`."""
        query = """
            SELECT
                id_source,
                TRIM(name) AS name,
                TRIM(short_name) AS short_name,
                CASE
                    WHEN TRIM(homepage) IN ('vacio', 'no-data', '') THEN NULL
                    ELSE TRIM(homepage)
                END AS homepage,
                creation_date,
                observations
            FROM source
            WHERE name IS NOT NULL AND TRIM(name) != ''
            ORDER BY id_source;
        """
        results: list[SourceDefinition] = []
        norm_codes = (
            {c.strip().upper() for c in source_codes}
            if source_codes
            else None
        )

        with self._get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchall()
            finally:
                cur.close()

            for row in rows:
                short = row["short_name"] or f"SRC_{row['id_source']}"
                canonical = f"BO_{self._sanitize_code(short)}"

                if norm_codes and not (
                    short.upper() in norm_codes
                    or canonical.upper() in norm_codes
                ):
                    continue

                source = SourceDefinition(
                    source_id=row["id_source"],
                    canonical_code=canonical,
                    official_name=row["name"],
                    short_name=short,
                    homepage=row["homepage"],
                    country_iso="BOL",
                    is_active=True,
                    notes=row["observations"],
                )
                results.append(source)

        return results

    def extract_datasets(
        self,
        source_codes: list[str] | None = None,
        file_codes: list[str] | None = None,
    ) -> list[DatasetSpecification]:
        """Extrae definiciones de datasets de la tabla `file` (ignorando file9)."""
        query = """
            SELECT
                f.id_file,
                f.id_source,
                TRIM(f.code) AS file_code,
                TRIM(f.name) AS file_name,
                s.short_name AS source_short_name,
                f.publication_frequency,
                COALESCE(f.specific_url, f.main_url, '') AS target_url,
                f.type AS expected_format,
                f.creation_date
            FROM file f
            INNER JOIN source s ON f.id_source = s.id_source
            WHERE f.code IS NOT NULL AND TRIM(f.code) != ''
            ORDER BY f.id_file;
        """
        results: list[DatasetSpecification] = []
        norm_sources = (
            {c.strip().upper() for c in source_codes}
            if source_codes
            else None
        )
        norm_files = (
            {f.strip().upper() for f in file_codes}
            if file_codes
            else None
        )

        with self._get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchall()
            finally:
                cur.close()

            for row in rows:
                src_short = (
                    row["source_short_name"] or f"SRC_{row['id_source']}"
                )
                src_clean = self._sanitize_code(src_short)
                file_code = row["file_code"]
                clean_file_code = self._sanitize_code(file_code)

                dataset_code = f"DS_BO_{src_clean}_{clean_file_code}"

                if norm_sources and not (
                    src_short.upper() in norm_sources
                    or f"BO_{src_clean}".upper() in norm_sources
                ):
                    continue

                if norm_files and not (
                    file_code.upper() in norm_files
                    or dataset_code.upper() in norm_files
                ):
                    continue

                fmt = (row["expected_format"] or "xlsx").lower().strip()

                dataset = DatasetSpecification(
                    dataset_code=dataset_code,
                    source_id=row["id_source"],
                    legacy_file_code=file_code,
                    name=row["file_name"] or file_code,
                    periodicity=self._map_periodicity(
                        row["publication_frequency"]
                    ),
                    url_template=row["target_url"],
                    expected_format=fmt,
                    is_active=True,
                )
                results.append(dataset)

        return results

    def seed_catalog(
        self,
        catalog: CatalogRepositoryPort,
        source_codes: list[str] | None = None,
        file_codes: list[str] | None = None,
    ) -> tuple[int, int]:
        """Puebla el repositorio canónico de Portal Nexus con datos saneados."""
        sources = self.extract_sources(source_codes=source_codes)
        for src in sources:
            catalog.save_source(src)

        datasets = self.extract_datasets(
            source_codes=source_codes,
            file_codes=file_codes,
        )
        for ds in datasets:
            catalog.save_dataset(ds)

        return len(sources), len(datasets)

    def extract_downloads(
        self,
        source_codes: list[str] | None = None,
        file_codes: list[str] | None = None,
    ) -> list[DownloadRecord]:
        """Extrae registros históricos de la tabla `download` normalizados."""
        query = """
            SELECT
                d.id_download,
                d.id_file,
                f.code AS file_code,
                s.short_name AS source_short_name,
                d.download_hash,
                d.downloaded_to,
                d.download_date,
                d.path,
                d.urls,
                d.state,
                d.creation_date
            FROM download d
            INNER JOIN file f ON d.id_file = f.id_file
            INNER JOIN source s ON f.id_source = s.id_source
            ORDER BY d.id_download;
        """
        results: list[DownloadRecord] = []
        norm_sources = (
            {c.strip().upper() for c in source_codes}
            if source_codes
            else None
        )
        norm_files = (
            {f.strip().upper() for f in file_codes}
            if file_codes
            else None
        )

        with self._get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchall()
            finally:
                cur.close()

            for row in rows:
                src_short = (
                    row["source_short_name"] or f"SRC_{row['id_file']}"
                )
                src_clean = self._sanitize_code(src_short)
                file_code = row["file_code"]
                canonical_src = f"BO_{src_clean}"

                if norm_sources and not (
                    src_short.upper() in norm_sources
                    or canonical_src.upper() in norm_sources
                ):
                    continue

                if norm_files and not (
                    file_code.upper() in norm_files
                    or f"DS_BO_{src_clean}_{self._sanitize_code(file_code)}".upper()
                    in norm_files
                ):
                    continue

                hashes = [
                    h.strip()
                    for h in (row["download_hash"] or "").split(";")
                    if h.strip()
                ]
                paths = [
                    p.strip()
                    for p in (row["path"] or "").split(";")
                    if p.strip()
                ]
                urls = [
                    u.strip()
                    for u in (row["urls"] or "").split(";")
                    if u.strip()
                ]
                n_items = max(len(hashes), len(paths), len(urls), 1)

                raw_date = row["download_date"] or row["creation_date"]
                parsed_dt: datetime | None = None
                if raw_date:
                    try:
                        parsed_dt = datetime.fromisoformat(str(raw_date))
                    except ValueError:
                        parsed_dt = None

                state_str = str(row["state"] or "").strip().lower()
                status = (
                    DownloadLifecycleStatus.COMPLETED
                    if state_str == "downloaded"
                    else DownloadLifecycleStatus.FAILED
                )

                for i in range(n_items):
                    h = (
                        hashes[i]
                        if i < len(hashes)
                        else (hashes[0] if hashes else None)
                    )
                    p = (
                        paths[i]
                        if i < len(paths)
                        else (paths[0] if paths else None)
                    )
                    u = (
                        urls[i]
                        if i < len(urls)
                        else (urls[0] if urls else "-")
                    )

                    period = row["downloaded_to"] or "unknown"
                    if u and u != "-":
                        canon_url = (
                            f"{u}#subitem={i}" if n_items > 1 else u
                        )
                    else:
                        suffix = f"/subitem-{i}" if n_items > 1 else ""
                        canon_url = (
                            f"legacy://BO/{src_clean}/{file_code}/"
                            f"{period}{suffix}"
                        )

                    key = f"{row['id_download']}_{i}_{canon_url}"
                    dl_id = hashlib.sha256(key.encode()).hexdigest()

                    record = DownloadRecord(
                        download_id=dl_id,
                        source_id=canonical_src,
                        canonical_url=canon_url,
                        local_path=p,
                        status=status,
                        sha256_checksum=h,
                        bytes_downloaded=0,
                        total_bytes=0,
                        consecutive_failures=0,
                        last_error_code=None,
                        task_id=f"legacy-migration-dl-{row['id_download']}",
                        subscribers=[],
                        started_at=parsed_dt,
                        completed_at=parsed_dt,
                    )
                    results.append(record)

        return results

    def seed_download_history(
        self,
        tracker: DownloadTrackingPort,
        source_codes: list[str] | None = None,
        file_codes: list[str] | None = None,
    ) -> int:
        """Puebla el repositorio de tracking con registros históricos."""
        records = self.extract_downloads(
            source_codes=source_codes,
            file_codes=file_codes,
        )
        for rec in records:
            tracker.record_download(rec)

        return len(records)
