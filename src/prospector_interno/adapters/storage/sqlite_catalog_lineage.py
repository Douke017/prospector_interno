"""Adaptador de persistencia SQLite para Catálogo de Activos y Linaje (HUI-17).

Implementa LineageRepositoryPort almacenando la especificación canónica
definida en la Sección 14.1 de la arquitectura (internal_assets y data_lineage).
"""

from datetime import datetime
from pathlib import Path

import sqlite3
import threading

from prospector_interno.domain.contracts.lineage_spec import (
    DataLineageRecord,
    InternalAssetRecord,
    StorageLayer,
)
from prospector_interno.domain.ports import LineageRepositoryPort


class SqliteLineageAdapter(LineageRepositoryPort):
    """Repositorio relacional liviano para inventario de activos y grafo de linaje."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit manual con transacciones
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Crea tablas e índices relacionales de la Sección 14.1."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA foreign_keys = ON;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS internal_assets (
                    asset_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    storage_layer TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    content_hash_sha256 TEXT NOT NULL,
                    row_count INTEGER,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS data_lineage (
                    lineage_id TEXT PRIMARY KEY,
                    gold_asset_id TEXT NOT NULL,
                    bronze_source_file TEXT NOT NULL,
                    bronze_file_sha256 TEXT NOT NULL,
                    external_run_id TEXT NOT NULL,
                    pipeline_version TEXT NOT NULL,
                    transformed_at TEXT NOT NULL,
                    FOREIGN KEY (gold_asset_id) REFERENCES internal_assets(asset_id)
                        ON DELETE CASCADE
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_lineage_gold_asset "
                "ON data_lineage(gold_asset_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_lineage_sha "
                "ON data_lineage(bronze_file_sha256);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_assets_layer "
                "ON internal_assets(storage_layer);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_assets_source "
                "ON internal_assets(source_id);"
            )
            cur.close()

    def record_asset(self, asset: InternalAssetRecord) -> None:
        """Registra o actualiza un activo de datos en el inventario."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO internal_assets (
                    asset_id, source_id, dataset_id, period_start, period_end,
                    storage_layer, file_path, content_hash_sha256, row_count,
                    size_bytes, created_at, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    source_id=excluded.source_id,
                    dataset_id=excluded.dataset_id,
                    period_start=excluded.period_start,
                    period_end=excluded.period_end,
                    storage_layer=excluded.storage_layer,
                    file_path=excluded.file_path,
                    content_hash_sha256=excluded.content_hash_sha256,
                    row_count=excluded.row_count,
                    size_bytes=excluded.size_bytes,
                    last_updated_at=excluded.last_updated_at;
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
            cur.close()

    def get_asset(self, asset_id: str) -> InternalAssetRecord | None:
        """Obtiene un activo de datos por su identificador único."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM internal_assets WHERE asset_id = ?;",
                (asset_id,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            return self._row_to_asset(row)

    def list_assets(
        self,
        storage_layer: str | None = None,
        source_id: str | None = None,
    ) -> list[InternalAssetRecord]:
        """Lista activos catalogados con filtros opcionales."""
        query = "SELECT * FROM internal_assets WHERE 1=1"
        params: list[str] = []
        if storage_layer:
            query += " AND storage_layer = ?"
            params.append(storage_layer.upper())
        if source_id:
            query += " AND source_id = ?"
            params.append(source_id)

        query += " ORDER BY period_start DESC, asset_id ASC;"

        with self._lock:
            cur = self._conn.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            cur.close()
            return [self._row_to_asset(r) for r in rows]

    def record_lineage(self, lineage: DataLineageRecord) -> None:
        """Inserta un enlace inmutable de linaje entre Gold y Bronze."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO data_lineage (
                    lineage_id, gold_asset_id, bronze_source_file,
                    bronze_file_sha256, external_run_id, pipeline_version,
                    transformed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lineage_id) DO UPDATE SET
                    transformed_at=excluded.transformed_at;
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
            cur.close()

    def get_lineage_by_asset(
        self,
        gold_asset_id: str,
    ) -> list[DataLineageRecord]:
        """Recupera todos los registros de linaje de un activo Gold."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM data_lineage
                WHERE gold_asset_id = ?
                ORDER BY transformed_at ASC;
                """,
                (gold_asset_id,),
            )
            rows = cur.fetchall()
            cur.close()
            return [self._row_to_lineage(r) for r in rows]

    def get_lineage_by_checksum(
        self,
        bronze_file_sha256: str,
    ) -> list[DataLineageRecord]:
        """Recupera los vínculos de linaje asociados a un archivo Bronze por SHA-256."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM data_lineage
                WHERE bronze_file_sha256 = ?
                ORDER BY transformed_at ASC;
                """,
                (bronze_file_sha256,),
            )
            rows = cur.fetchall()
            cur.close()
            return [self._row_to_lineage(r) for r in rows]

    def close(self) -> None:
        """Cierra la conexión SQLite."""
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_asset(row: sqlite3.Row) -> InternalAssetRecord:
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
            size_bytes=row["size_bytes"],
            created_at=datetime.fromisoformat(row["created_at"]),
            last_updated_at=datetime.fromisoformat(row["last_updated_at"]),
        )

    @staticmethod
    def _row_to_lineage(row: sqlite3.Row) -> DataLineageRecord:
        return DataLineageRecord(
            lineage_id=row["lineage_id"],
            gold_asset_id=row["gold_asset_id"],
            bronze_source_file=row["bronze_source_file"],
            bronze_file_sha256=row["bronze_file_sha256"],
            external_run_id=row["external_run_id"],
            pipeline_version=row["pipeline_version"],
            transformed_at=datetime.fromisoformat(row["transformed_at"]),
        )
