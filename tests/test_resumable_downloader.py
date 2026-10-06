"""Pruebas unitarias para el Descargador Streaming Reanudable (HUI-08)."""

import hashlib
from pathlib import Path
import pytest
import httpx

from prospector_interno.adapters.downloader.circuit_breaker import (
    DomainCircuitBreaker,
)
from prospector_interno.adapters.downloader.resumable_stream_downloader import (
    ResumableStreamDownloader,
)
from prospector_interno.domain.contracts.downloader_spec import CircuitState
from prospector_interno.domain.exceptions import (
    ChecksumMismatchError,
    CircuitOpenError,
    DownloadExecutionError,
)


def test_full_download_from_scratch(tmp_path: Path) -> None:
    """Verifica descarga completa por bloques desde cero con validación SHA-256."""
    payload = b"BOLETIN FINANCIERO OFICIAL DE PRUEBA 2026" * 1000
    expected_hash = hashlib.sha256(payload).hexdigest()
    destination = tmp_path / "boletin_completo.pdf"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Range") is None
        assert "Sec-Ch-Ua" in request.headers
        return httpx.Response(
            200,
            content=payload,
            headers={"content-type": "application/pdf"},
        )

    downloader = ResumableStreamDownloader(
        chunk_size=1024,
        transport=httpx.MockTransport(handler),
    )
    result = downloader.download_file(
        url="https://finrural-bo.org/boletin_2026.pdf",
        domain="finrural-bo.org",
        destination_path=destination,
        expected_checksum=expected_hash,
    )

    assert result.was_resumed is False
    assert result.total_file_size == len(payload)
    assert result.bytes_downloaded == len(payload)
    assert result.sha256_hash == expected_hash
    assert destination.exists()
    assert destination.read_bytes() == payload
    # Verificar que el temporal no haya quedado huérfano
    assert not destination.with_name(destination.name + ".tmp").exists()


def test_resumable_download_with_range_header_206(tmp_path: Path) -> None:
    """Verifica la reanudación ante un archivo parcial enviando Range: bytes=N-."""
    full_payload = b"PARTE_UNO_DEL_ARCHIVO_12345" + b"PARTE_DOS_COMPLEMENTARIA_67890"
    split_index = len(b"PARTE_UNO_DEL_ARCHIVO_12345")
    expected_hash = hashlib.sha256(full_payload).hexdigest()

    destination = tmp_path / "documento_reanudado.pdf"
    tmp_file = destination.with_name(destination.name + ".tmp")
    # Simular descarga cortada previamente
    tmp_file.write_bytes(full_payload[:split_index])

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Range") == f"bytes={split_index}-"
        return httpx.Response(
            206,
            content=full_payload[split_index:],
            headers={
                "content-type": "application/pdf",
                "content-range": (
                    f"bytes {split_index}-{len(full_payload)-1}/{len(full_payload)}"
                ),
            },
        )

    downloader = ResumableStreamDownloader(
        transport=httpx.MockTransport(handler),
    )
    result = downloader.download_file(
        url="https://asfi.gob.bo/reporte_parcial.pdf",
        domain="asfi.gob.bo",
        destination_path=destination,
        expected_checksum=expected_hash,
        allow_resume=True,
    )

    assert result.was_resumed is True
    assert result.bytes_downloaded == len(full_payload[split_index:])
    assert result.total_file_size == len(full_payload)
    assert result.sha256_hash == expected_hash
    assert destination.exists()
    assert destination.read_bytes() == full_payload


def test_server_ignores_range_restarts_cleanly(tmp_path: Path) -> None:
    """Verifica que si el servidor devuelve 200 en vez de 206 se reinicie."""
    full_payload = b"CONTENIDO_COMPLETO_NUEVO"
    expected_hash = hashlib.sha256(full_payload).hexdigest()

    destination = tmp_path / "ignora_range.pdf"
    tmp_file = destination.with_name(destination.name + ".tmp")
    tmp_file.write_bytes(b"BASURA_VIEJA_INTERRUMPIDA")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=full_payload)

    downloader = ResumableStreamDownloader(
        transport=httpx.MockTransport(handler),
    )
    result = downloader.download_file(
        url="https://bcb.gob.bo/reporte.pdf",
        domain="bcb.gob.bo",
        destination_path=destination,
        expected_checksum=expected_hash,
        allow_resume=True,
    )

    assert result.was_resumed is False
    assert result.total_file_size == len(full_payload)
    assert destination.read_bytes() == full_payload


def test_checksum_mismatch_cleans_tmp_and_raises(tmp_path: Path) -> None:
    """Verifica que ante discrepancia de hash se elimine el .tmp y lance excepción."""
    payload = b"CONTENIDO CORRUPTO"
    expected_hash = "0000000000000000000000000000000000000000000000000000000000000000"
    destination = tmp_path / "corrupto.pdf"
    tmp_file = destination.with_name(destination.name + ".tmp")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload)

    downloader = ResumableStreamDownloader(
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(ChecksumMismatchError) as exc_info:
        downloader.download_file(
            url="https://finrural-bo.org/corrupto.pdf",
            domain="finrural-bo.org",
            destination_path=destination,
            expected_checksum=expected_hash,
        )

    assert exc_info.value.expected == expected_hash
    assert not destination.exists()
    assert not tmp_file.exists()


def test_circuit_breaker_trips_on_429_with_retry_after(tmp_path: Path) -> None:
    """Verifica integración con Circuit Breaker ante respuesta 429."""
    circuit_breaker = DomainCircuitBreaker()
    destination = tmp_path / "bloqueado.pdf"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "45"},
            content=b"Too Many Requests",
        )

    downloader = ResumableStreamDownloader(
        circuit_breaker=circuit_breaker,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(DownloadExecutionError):
        downloader.download_file(
            url="https://asfi.gob.bo/reporte_bloqueado.pdf",
            domain="asfi.gob.bo",
            destination_path=destination,
        )

    # El disyuntor debe estar OPEN
    status = circuit_breaker.get_status("asfi.gob.bo")
    assert status.state == CircuitState.OPEN
    assert status.failure_count == 1
    assert 40.0 <= status.cooldown_seconds <= 45.0

    # Petición subsiguiente al mismo dominio debe ser rechazada inmediatamente
    with pytest.raises(CircuitOpenError):
        downloader.download_file(
            url="https://asfi.gob.bo/otro.pdf",
            domain="asfi.gob.bo",
            destination_path=destination,
        )


def test_download_to_stream_compatibility(tmp_path: Path) -> None:
    """Verifica compatibilidad con el método download_to_stream del puerto base."""
    payload = b"CONTENIDO EN STREAM COMPATIBLE"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=payload,
            headers={"content-type": "application/pdf"},
        )

    downloader = ResumableStreamDownloader(
        transport=httpx.MockTransport(handler),
    )
    stream, status_code, content_type = downloader.download_to_stream(
        url="https://finrural-bo.org/stream.pdf",
        domain="finrural-bo.org",
    )

    assert status_code == 200
    assert content_type == "application/pdf"
    data = stream.read()
    stream.close()
    assert data == payload
