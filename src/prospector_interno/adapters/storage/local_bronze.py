"""Adaptador de Infraestructura: Almacenamiento Local en Capa Bronze.

Implementa HUI-11 con escritura en archivo temporal, cálculo de checksum
SHA-256 en vuelo y renombrado atómico en el sistema de archivos del SO.
"""

import hashlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from prospector_interno.domain.exceptions import (
    AtomicRenameError,
    ChecksumMismatchError,
)
from prospector_interno.domain.models import BronzeArtifact
from prospector_interno.domain.ports import BronzeStoragePort


class LocalBronzeStorage(BronzeStoragePort):
    """Gestor de almacenamiento inmutable en disco local para Capa Bronze."""

    CHUNK_SIZE: int = 64 * 1024  # 64 KB por bloque para streaming I/O

    def __init__(self, base_path: Path) -> None:
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        # Índice en memoria para resolución O(1) de descargas históricas
        # Claves: (domain, dataset_code, period) -> BronzeArtifact
        self._index_by_period: dict[tuple[str, str, str], BronzeArtifact] = {}
        # Claves: checksum_sha256 -> BronzeArtifact
        self._index_by_checksum: dict[str, BronzeArtifact] = {}

    def store_stream(
        self,
        domain: str,
        dataset_code: str,
        period: str,
        stream: BinaryIO,
        expected_checksum: str | None = None,
        content_type: str | None = None,
    ) -> BronzeArtifact:
        """Escribe un stream de bytes en Capa Bronze de forma atómica e inmutable.

        Pasos:
        1. Crea la estructura de carpetas: base_path / domain / YYYY / MM
        2. Escribe en un archivo temporal: .tmp.{uuid}.dat
        3. Calcula SHA-256 y tamaño en bytes mientras transmite bloques.
        4. Valida checksum esperado si fue suministrado.
        5. Renombra atómicamente a {sha256}.dat usando el kernel del SO.
        6. Actualiza índices de catálogo.
        """
        now = datetime.now(timezone.utc)
        target_dir = self._base_path / domain / f"{now.year:04d}" / f"{now.month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)

        temp_filename = f".tmp.{uuid.uuid4().hex}.dat"
        temp_path = target_dir / temp_filename

        hasher = hashlib.sha256()
        total_bytes = 0

        try:
            with open(temp_path, "wb") as f_out:
                while True:
                    chunk = stream.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    hasher.update(chunk)
                    total_bytes += len(chunk)

            calculated_hash = hasher.hexdigest()

            # Validación de integridad criptográfica
            if expected_checksum and calculated_hash != expected_checksum:
                raise ChecksumMismatchError(
                    message=(
                        f"Checksum corrupto para '{dataset_code}'. "
                        f"Esperado: {expected_checksum}, "
                        f"Calculado: {calculated_hash}"
                    ),
                    expected=expected_checksum,
                    calculated=calculated_hash,
                )

            final_path = target_dir / f"{calculated_hash}.dat"

            # Renombrado atómico protegido
            try:
                os.replace(temp_path, final_path)
            except OSError as err:
                raise AtomicRenameError(
                    f"Fallo crítico en renombrado atómico a '{final_path}'"
                ) from err

            artifact = BronzeArtifact(
                artifact_id=f"bronze_{calculated_hash[:16]}",
                dataset_code=dataset_code,
                domain=domain,
                file_path=final_path,
                sha256_hash=calculated_hash,
                size_bytes=total_bytes,
                downloaded_at=now,
                http_status=200,
                content_type=content_type,
            )

            # Registrar en índices
            self._index_by_period[(domain, dataset_code, period)] = artifact
            self._index_by_checksum[calculated_hash] = artifact

            return artifact

        finally:
            # Garantizar que ningún archivo temporal corrupto quede huérfano
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    def find_by_checksum(self, checksum: str) -> BronzeArtifact | None:
        """Localiza un artefacto por su hash SHA-256."""
        return self._index_by_checksum.get(checksum)

    def find_by_period(
        self,
        domain: str,
        dataset_code: str,
        period: str,
    ) -> BronzeArtifact | None:
        """Localiza un artefacto por dominio, dataset y periodo."""
        return self._index_by_period.get((domain, dataset_code, period))

    def register_existing_artifact(
        self,
        artifact: BronzeArtifact,
        period: str,
    ) -> None:
        """Registra un artefacto precargado o importado en los índices."""
        key = (artifact.domain, artifact.dataset_code, period)
        self._index_by_period[key] = artifact
        self._index_by_checksum[artifact.sha256_hash] = artifact
