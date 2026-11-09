"""Pruebas unitarias para el parser de boletines PDF de FINRURAL."""

from decimal import Decimal
from pathlib import Path
import pytest

from prospector_interno.application.pipelines.finrural_parser import (
    FinruralParserError,
    FinruralPdfParser,
)
from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    StatementType,
)


def test_finrural_parser_init_error_when_no_pdftotext(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifica que se lance FinruralParserError si pdftotext no existe en PATH."""
    monkeypatch.setattr("shutil.which", lambda _: None)
    with pytest.raises(FinruralParserError, match="pdftotext"):
        FinruralPdfParser()


def test_parse_period_valid_dates() -> None:
    """Valida la extracción correcta de periodos mensuales en español."""
    parser = FinruralPdfParser()

    text_jan = "FINRURAL - Información Financiera al 31 de enero de 2026"
    gestion, mes, period = parser.parse_period(text_jan)
    assert gestion == 2026
    assert mes == 1
    assert period == "2026-01"

    text_dec = "Boletín Estadístico al 31 de diciembre de 2025 (Cifras en Bs.)"
    gestion, mes, period = parser.parse_period(text_dec)
    assert gestion == 2025
    assert mes == 12
    assert period == "2025-12"


def test_parse_period_missing_date_raises_error() -> None:
    """Verifica error cuando el texto no contiene un encabezado de fecha."""
    parser = FinruralPdfParser()
    with pytest.raises(FinruralParserError, match="No fue posible encontrar"):
        parser.parse_period("Documento sin fecha explícita")


def test_parse_period_unknown_month_raises_error() -> None:
    """Verifica error cuando el mes no corresponde al calendario."""
    parser = FinruralPdfParser()
    with pytest.raises(FinruralParserError, match="no reconocido"):
        parser.parse_period("Reporte al 15 de inventado de 2026")


def test_parse_row_numbers_handles_dashes_and_commas() -> None:
    """Valida la conversión de strings con formato contable a Decimals."""
    parser = FinruralPdfParser()
    line = "CARTERA VENCIDA   1,234.50   -   500.00   10.25   -   -   -   -   1,744.75"
    numbers = parser.parse_row_numbers(line, "CARTERA VENCIDA")

    assert len(numbers) == 9
    assert numbers[0] == Decimal("1234.50")
    assert numbers[1] == Decimal("0")
    assert numbers[2] == Decimal("500.00")
    assert numbers[3] == Decimal("10.25")
    assert numbers[8] == Decimal("1744.75")


def test_parse_cuadro_patrimonial_with_real_bronze_file() -> None:
    """Ejecuta el parser sobre un archivo real de FINRURAL en la Capa Bronze si existe."""
    finrural_dir = (
        Path(__file__).resolve().parent.parent / "data" / "bronze" / "www.finrural.org.bo"
    )
    bronze_files = sorted(finrural_dir.rglob("*.dat")) if finrural_dir.exists() else []
    if not bronze_files:
        pytest.skip("No hay archivos de FINRURAL en data/bronze para prueba real")

    parser = FinruralPdfParser()
    records = None
    for candidate_pdf in bronze_files:
        try:
            parsed = parser.parse_cuadro_patrimonial(
                candidate_pdf, source_file_sha256="test_sha_123"
            )
            if len(parsed) == 64:
                records = parsed
                break
        except FinruralParserError:
            continue

    assert records is not None and len(records) == 64  # 8 instituciones x 8 cuentas
    first = records[0]
    assert first.source_id == "finrural"
    assert first.statement_type == StatementType.SITUACION_PATRIMONIAL
    assert first.account_category in {
        AccountCategory.CARTERA,
        AccountCategory.ACTIVO,
        AccountCategory.PASIVO,
        AccountCategory.PATRIMONIO,
    }
    assert first.source_file_sha256 == "test_sha_123"
    assert first.gestion in {2025, 2026}


def test_parse_cuadro_patrimonial_fails_on_empty_text(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verifica excepción cuando el PDF no produce líneas de balance válidas."""
    dummy_pdf = tmp_path / "dummy.pdf"
    dummy_pdf.write_bytes(b"%PDF-1.4 dummy")

    parser = FinruralPdfParser()
    monkeypatch.setattr(
        parser,
        "extract_text_layout",
        lambda *args, **kwargs: (
            "Boletín al 31 de enero de 2026\n"
            "Texto sin tablas ni cuentas financieras..."
        ),
    )

    with pytest.raises(FinruralParserError, match="No se pudieron extraer"):
        parser.parse_cuadro_patrimonial(dummy_pdf, source_file_sha256="dummy_sha")
