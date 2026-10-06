"""Adaptador de Infraestructura: Almacenamiento Local en Capa Bronze.

Implementa HUI-11 con escritura en archivo temporal, cálculo de checksum
SHA-256 en vuelo y renombrado atómico en el sistema de archivos del SO.
"""

import hashlib
import json
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
        self._manifest_file = self._base_path / "bronze_manifest.json"
        # Índice en memoria para resolución O(1) de descargas históricas
        # Claves: (domain, dataset_code, period) -> BronzeArtifact
        self._index_by_period: dict[tuple[str, str, str], BronzeArtifact] = {}
        # Claves: checksum_sha256 -> BronzeArtifact
        self._index_by_checksum: dict[str, BronzeArtifact] = {}
        # Claves: url -> BronzeArtifact
        self._index_by_url: dict[str, BronzeArtifact] = {}
        # Claves: resource_key -> BronzeArtifact
        self._index_by_key: dict[str, BronzeArtifact] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        """Carga el índice persistido de artefactos de Capa Bronze si existe."""
        if not self._manifest_file.exists():
            return
        try:
            with open(self._manifest_file, "r", encoding="utf-8") as f_in:
                data = json.load(f_in)
            for entry in data.get("artifacts", []):
                art = BronzeArtifact(
                    artifact_id=entry["artifact_id"],
                    dataset_code=entry["dataset_code"],
                    domain=entry["domain"],
                    file_path=Path(entry["file_path"]),
                    sha256_hash=entry["sha256_hash"],
                    size_bytes=entry["size_bytes"],
                    downloaded_at=datetime.fromisoformat(entry["downloaded_at"]),
                    http_status=entry.get("http_status", 200),
                    content_type=entry.get("content_type"),
                )
                period = entry.get("period", "")
                self._index_by_period[(art.domain, art.dataset_code, period)] = art
                self._index_by_checksum[art.sha256_hash] = art
                url = entry.get("url")
                if url:
                    self._index_by_url[url] = art
                r_key = entry.get("resource_key")
                if r_key:
                    self._index_by_key[r_key] = art
        except Exception:
            pass

    def _save_manifest_entry(
        self,
        artifact: BronzeArtifact,
        period: str,
        url: str | None = None,
        resource_key: str | None = None,
    ) -> None:
        """Registra un nuevo artefacto en el manifiesto atómico en disco."""
        entries = []
        if self._manifest_file.exists():
            try:
                with open(self._manifest_file, "r", encoding="utf-8") as f_in:
                    entries = json.load(f_in).get("artifacts", [])
            except Exception:
                entries = []
        entries.append(
            {
                "artifact_id": artifact.artifact_id,
                "dataset_code": artifact.dataset_code,
                "domain": artifact.domain,
                "file_path": str(artifact.file_path),
                "sha256_hash": artifact.sha256_hash,
                "size_bytes": artifact.size_bytes,
                "downloaded_at": artifact.downloaded_at.isoformat(),
                "http_status": artifact.http_status,
                "content_type": artifact.content_type,
                "period": period,
                "url": url,
                "resource_key": resource_key,
            }
        )
        tmp_manifest = self._manifest_file.with_suffix(".tmp.json")
        try:
            with open(tmp_manifest, "w", encoding="utf-8") as f_out:
                json.dump({"artifacts": entries}, f_out, indent=2)
            os.replace(tmp_manifest, self._manifest_file)
        except OSError:
            pass

    def store_stream(
        self,
        domain: str,
        dataset_code: str,
        period: str,
        stream: BinaryIO,
        expected_checksum: str | None = None,
        content_type: str | None = None,
        url: str | None = None,
        resource_key: str | None = None,
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

            # Registrar en índices y persistir en manifiesto
            self._index_by_period[(domain, dataset_code, period)] = artifact
            self._index_by_checksum[calculated_hash] = artifact
            if url:
                self._index_by_url[url] = artifact
            if resource_key:
                self._index_by_key[resource_key] = artifact
            self._save_manifest_entry(
                artifact, period, url=url, resource_key=resource_key
            )

            return artifact

        finally:
            # Garantizar que ningún archivo temporal corrupto quede huérfano
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    def find_by_checksum(self, checksum: str) -> BronzeArtifact | None:
        """Localiza un artefacto por su hash SHA-256 en memoria o disco."""
        artifact = self._index_by_checksum.get(checksum)
        if artifact is not None:
            return artifact

        # Búsqueda física en disco como respaldo
        matches = list(self._base_path.glob(f"**/{checksum}.dat"))
        if matches:
            file_path = matches[0]
            try:
                st = file_path.stat()
                recovered = BronzeArtifact(
                    artifact_id=f"bronze_{checksum[:16]}",
                    dataset_code="RECOVERED_FROM_STORAGE",
                    domain=file_path.parent.parent.parent.name,
                    file_path=file_path,
                    sha256_hash=checksum,
                    size_bytes=st.st_size,
                    downloaded_at=datetime.fromtimestamp(
                        st.st_mtime, tz=timezone.utc
                    ),
                    http_status=200,
                )
                self._index_by_checksum[checksum] = recovered
                return recovered
            except OSError:
                return None
        return None

    def find_by_period(
        self,
        domain: str,
        dataset_code: str,
        period: str,
    ) -> BronzeArtifact | None:
        """Localiza un artefacto por dominio, dataset y periodo."""
        # 1. Búsqueda exacta
        exact = self._index_by_period.get((domain, dataset_code, period))
        if exact is not None:
            return exact

        # 2. Búsqueda tolerante por período e institución/dominio
        for (d, code, p), art in self._index_by_period.items():
            if p == period and (d == domain or code == dataset_code):
                return art

        return None

    def find_by_url(self, url: str) -> BronzeArtifact | None:
        """Localiza un artefacto por su URL original de descarga."""
        return self._index_by_url.get(url)

    def find_by_resource_key(self, resource_key: str) -> BronzeArtifact | None:
        """Localiza un artefacto por su clave de recurso."""
        return self._index_by_key.get(resource_key)

    def register_existing_artifact(
        self,
        artifact: BronzeArtifact,
        period: str,
    ) -> None:
        """Registra un artefacto precargado o importado en los índices."""
        key = (artifact.domain, artifact.dataset_code, period)
        self._index_by_period[key] = artifact
        self._index_by_checksum[artifact.sha256_hash] = artifact
        self._save_manifest_entry(artifact, period)

    def get_artifacts(self) -> list[BronzeArtifact]:
        """Retorna todos los artefactos indexados en Capa Bronze."""
        return list(self._index_by_checksum.values())

    def get_all_artifacts(self, source_id: str | None = None) -> list[dict]:
        """Obtiene la lista de artefactos persistidos físicamente en Bronze."""
        if not self._manifest_file.exists():
            return []
        try:
            with open(self._manifest_file, "r", encoding="utf-8") as f_in:
                data = json.load(f_in)
            artifacts = data.get("artifacts", [])
            if source_id:
                src_low = source_id.lower()
                src_upper = source_id.upper()
                artifacts = [
                    art for art in artifacts
                    if (
                        src_low in art.get("domain", "").lower()
                        or src_upper in art.get("dataset_code", "").upper()
                        or src_low in art.get("dataset_code", "").lower()
                    )
                ]
            # Devolver en orden cronológico inverso (los más recientes primero)
            artifacts.reverse()
            return artifacts
        except Exception:
            return []
