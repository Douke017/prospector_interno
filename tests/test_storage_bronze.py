"""Pruebas unitarias para HUI-11: Almacenamiento Inmutable en Capa Bronze."""

import hashlib
import io
from pathlib import Path
import pytest

from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.domain.exceptions import ChecksumMismatchError


def test_store_stream_success_and_atomic_rename(
    bronze_storage: LocalBronzeStorage,
    temp_bronze_dir: Path,
) -> None:
    """Verifica que el archivo se escribe, se renombra atómicamente
    y calcula su checksum SHA-256 en vuelo.
    """
    content = b"COLUMNA_A,COLUMNA_B\n100,200\n300,400\n"
    expected_sha = hashlib.sha256(content).hexdigest()
    stream = io.BytesIO(content)

    artifact = bronze_storage.store_stream(
        domain="bcb.gob.bo",
        dataset_code="DS_BO_BCB_INFLATION",
        period="2026-08",
        stream=stream,
        expected_checksum=expected_sha,
        content_type="text/csv",
    )

    # Verificaciones de metadatos
    assert artifact.sha256_hash == expected_sha
    assert artifact.size_bytes == len(content)
    assert artifact.domain == "bcb.gob.bo"
    assert artifact.dataset_code == "DS_BO_BCB_INFLATION"
    assert artifact.file_path.exists()
    assert artifact.file_path.name == f"{expected_sha}.dat"

    # Verificar que no quedan archivos temporales en el directorio
    parent_dir = artifact.file_path.parent
    temp_files = list(parent_dir.glob(".tmp.*"))
    assert len(temp_files) == 0

    # Verificar búsqueda por checksum
    found_by_hash = bronze_storage.find_by_checksum(expected_sha)
    assert found_by_hash is not None
    assert found_by_hash.artifact_id == artifact.artifact_id

    # Verificar búsqueda por periodo
    found_by_period = bronze_storage.find_by_period(
        domain="bcb.gob.bo",
        dataset_code="DS_BO_BCB_INFLATION",
        period="2026-08",
    )
    assert found_by_period is not None
    assert found_by_period.artifact_id == artifact.artifact_id


def test_store_stream_checksum_mismatch_aborts_and_cleans_tmp(
    bronze_storage: LocalBronzeStorage,
    temp_bronze_dir: Path,
) -> None:
    """Verifica que ante discrepancia de hash se aborta y no queda basura."""
    content = b"DATOS_CORRUPTOS_O_TRUNCADOS"
    wrong_sha = "0000000000000000000000000000000000000000000000000000000000000000"
    stream = io.BytesIO(content)

    with pytest.raises(ChecksumMismatchError) as exc_info:
        bronze_storage.store_stream(
            domain="bcb.gob.bo",
            dataset_code="DS_BO_BCB_INFLATION",
            period="2026-08",
            stream=stream,
            expected_checksum=wrong_sha,
        )

    assert "Checksum corrupto" in str(exc_info.value)

    # Asegurar que ningún archivo temporal ni definitivo quedó en disco
    all_files = list(temp_bronze_dir.rglob("*.dat"))
    assert len(all_files) == 0
