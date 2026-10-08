"""Adaptador de persistencia SQLite para el Seguimiento de Descargas (HUI-03).

Implementa DownloadTrackingPort gestionando la tabla `download_tracking`
especificada en la Sección 14.1 del documento de arquitectura.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading

from prospector_interno.domain.contracts.download_lifecycle_spec import (
    DownloadLifecycleStatus,
    DownloadRecord,
)
from prospector_interno.domain.ports import DownloadTrackingPort


class SqliteDownloadTrackingAdapter(DownloadTrackingPort):
    """Repositorio relacional SQLite para el ciclo de vida de transferencias físicas."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit manual
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Crea la tabla e índices relacionales de la Sección 14.1."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS download_tracking (
                    download_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    local_path TEXT,
                    status TEXT NOT NULL,
                    sha256_checksum TEXT,
                    bytes_downloaded INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0,
                    consecutive_failures INTEGER DEFAULT 0,
                    last_error_code TEXT,
                    task_id TEXT,
                    subscribers TEXT DEFAULT '[]',
                    started_at TEXT,
                    completed_at TEXT
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_url "
                "ON download_tracking(canonical_url);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_status "
                "ON download_tracking(status);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_source "
                "ON download_tracking(source_id);"
            )

    def record_download(self, record: DownloadRecord) -> None:
        """Registra o inicializa una tarea de descarga en download_tracking."""
        with self._lock:
            cur = self._conn.cursor()
            subscribers_json = json.dumps(record.subscribers)
            started_str = record.started_at.isoformat() if record.started_at else None
            completed_str = (
                record.completed_at.isoformat() if record.completed_at else None
            )

            cur.execute(
                """
                INSERT INTO download_tracking (
                    download_id, source_id, canonical_url, local_path,
                    status, sha256_checksum, bytes_downloaded, total_bytes,
                    consecutive_failures, last_error_code, task_id,
                    subscribers, started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(download_id) DO UPDATE SET
                    status = excluded.status,
                    task_id = excluded.task_id,
                    bytes_downloaded = excluded.bytes_downloaded,
                    total_bytes = excluded.total_bytes,
                    last_error_code = excluded.last_error_code,
                    started_at = COALESCE(
                        excluded.started_at, download_tracking.started_at
                    ),
                    completed_at = COALESCE(
                        excluded.completed_at, download_tracking.completed_at
                    );
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
                    started_str,
                    completed_str,
                ),
            )

    def update_status(
        self,
        download_id: str,
        status: DownloadLifecycleStatus,
        bytes_downloaded: int | None = None,
        total_bytes: int | None = None,
        local_path: str | None = None,
        sha256_checksum: str | None = None,
        error_code: str | None = None,
    ) -> None:
        """Actualiza el estado y progreso de una descarga."""
        with self._lock:
            cur = self._conn.cursor()
            updates: list[str] = ["status = ?"]
            params: list[object] = [status.value]

            if bytes_downloaded is not None:
                updates.append("bytes_downloaded = ?")
                params.append(bytes_downloaded)
            if total_bytes is not None:
                updates.append("total_bytes = ?")
                params.append(total_bytes)
            if local_path is not None:
                updates.append("local_path = ?")
                params.append(local_path)
            if sha256_checksum is not None:
                updates.append("sha256_checksum = ?")
                params.append(sha256_checksum)
            if error_code is not None:
                updates.append("last_error_code = ?")
                params.append(error_code)

            if status in (
                DownloadLifecycleStatus.COMPLETED,
                DownloadLifecycleStatus.FAILED,
            ):
                updates.append("completed_at = ?")
                params.append(datetime.now(timezone.utc).isoformat())

            set_clause = ", ".join(updates)
            query = f"UPDATE download_tracking SET {set_clause} WHERE download_id = ?"
            params.append(download_id)
            cur.execute(query, tuple(params))

    def _row_to_record(self, row: sqlite3.Row) -> DownloadRecord:
        """Convierte una fila SQL en modelo inmutable de dominio."""
        started = (
            datetime.fromisoformat(row["started_at"]) if row["started_at"] else None
        )
        completed = (
            datetime.fromisoformat(row["completed_at"])
            if row["completed_at"]
            else None
        )
        subscribers_raw = row["subscribers"] or "[]"
        try:
            subscribers = json.loads(subscribers_raw)
        except Exception:
            subscribers = []

        return DownloadRecord(
            download_id=row["download_id"],
            source_id=row["source_id"],
            canonical_url=row["canonical_url"],
            local_path=row["local_path"],
            status=DownloadLifecycleStatus(row["status"]),
            sha256_checksum=row["sha256_checksum"],
            bytes_downloaded=row["bytes_downloaded"],
            total_bytes=row["total_bytes"],
            consecutive_failures=row["consecutive_failures"],
            last_error_code=row["last_error_code"],
            task_id=row["task_id"],
            subscribers=subscribers,
            started_at=started,
            completed_at=completed,
        )

    def get_download(self, download_id: str) -> DownloadRecord | None:
        """Recupera el registro de descarga por su identificador primario."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM download_tracking WHERE download_id = ?;",
                (download_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    def find_by_url(self, canonical_url: str) -> DownloadRecord | None:
        """Recupera el registro de descarga asociado a una URL canónica."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT * FROM download_tracking WHERE canonical_url = ?;",
                (canonical_url,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    def subscribe_request(self, download_id: str, request_id: str) -> bool:
        """Suscribe una petición concurrente a la descarga activa en curso."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT subscribers FROM download_tracking WHERE download_id = ?;",
                (download_id,),
            )
            row = cur.fetchone()
            if not row:
                return False

            raw_subs = row["subscribers"] or "[]"
            try:
                subs = json.loads(raw_subs)
            except Exception:
                subs = []

            if request_id not in subs:
                subs.append(request_id)
                cur.execute(
                    "UPDATE download_tracking SET subscribers = ? "
                    "WHERE download_id = ?;",
                    (json.dumps(subs), download_id),
                )
            return True

    def list_active_downloads(self) -> list[DownloadRecord]:
        """Lista todas las descargas en cola (PENDING) o activas (DOWNLOADING)."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM download_tracking
                WHERE status IN ('PENDING', 'DOWNLOADING')
                ORDER BY started_at DESC;
                """
            )
            rows = cur.fetchall()
            return [self._row_to_record(r) for r in rows]

    def close(self) -> None:
        """Cierra la conexión con SQLite."""
        with self._lock:
            self._conn.close()
