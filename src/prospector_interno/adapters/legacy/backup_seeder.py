"""Adaptador de Ingesta y Normalización desde la Base de Datos Legacy (SQL).

Extrae fuentes, definiciones de archivos y registros históricos desde
el PostgreSQL legacy y los transforma en modelos canónicos para Portal Nexus.
"""

from contextlib import contextmanager
from typing import Generator
import re

import psycopg
from psycopg.rows import dict_row

from prospector_interno.domain.exceptions import LegacySeederError
from prospector_interno.domain.models import (
    DatasetSpecification,
    Periodicity,
    SourceDefinition,
)
from prospector_interno.domain.ports import CatalogRepositoryPort


class PostgresLegacySeeder:
    """Adaptador de migración que consulta PostgreSQL mediante SQL declarativo."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5434,
        dbname: str = "platform_db",
        user: str = "postgres",
        password: str = "postgres",
    ) -> None:
        self._conn_info = (
            f"host={host} port={port} dbname={dbname} "
            f"user={user} password={password}"
        )

    @contextmanager
    def _get_connection(self) -> Generator[psycopg.Connection, None, None]:
        try:
            with psycopg.connect(self._conn_info, row_factory=dict_row) as conn:
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

    def extract_sources(self) -> list[SourceDefinition]:
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

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

                for row in rows:
                    short = row["short_name"] or f"SRC_{row['id_source']}"
                    canonical = f"BO_{self._sanitize_code(short)}"

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

    def extract_datasets(self) -> list[DatasetSpecification]:
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

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

                for row in rows:
                    src_short = row["source_short_name"] or f"SRC_{row['id_source']}"
                    src_clean = self._sanitize_code(src_short)
                    file_code = row["file_code"]
                    clean_file_code = self._sanitize_code(file_code)

                    dataset_code = f"DS_BO_{src_clean}_{clean_file_code}"
                    fmt = (row["expected_format"] or "xlsx").lower().strip()

                    dataset = DatasetSpecification(
                        dataset_code=dataset_code,
                        source_id=row["id_source"],
                        legacy_file_code=file_code,
                        name=row["file_name"] or file_code,
                        periodicity=self._map_periodicity(row["publication_frequency"]),
                        url_template=row["target_url"],
                        expected_format=fmt,
                        is_active=True,
                    )
                    results.append(dataset)

        return results

    def seed_catalog(
        self,
        catalog: CatalogRepositoryPort,
    ) -> tuple[int, int]:
        """Puebla el repositorio canónico de Portal Nexus con datos saneados."""
        sources = self.extract_sources()
        for src in sources:
            catalog.save_source(src)

        datasets = self.extract_datasets()
        for ds in datasets:
            catalog.save_dataset(ds)

        return len(sources), len(datasets)
