"""Extractor y parser especializado para boletines PDF de FINRURAL."""

from decimal import Decimal
from pathlib import Path
import re
import shutil
import subprocess

from prospector_interno.domain.contracts.silver_spec import (
    AccountCategory,
    SilverFinancialRecord,
    StatementType,
)
from prospector_interno.domain.exceptions import ProspectorError

SPANISH_MONTHS: dict[str, int] = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

STANDARD_INSTITUTIONS: list[tuple[str, str]] = [
    ("CRECER", "Crecer IFD"),
    ("PRO_MUJER", "Pro Mujer IFD"),
    ("DIACONIA", "Diaconía IFD"),
    ("IDEPRO", "Idepro IFD"),
    ("IMPRO", "Impro IFD"),
    ("CIDRE", "Cidre IFD"),
    ("FONDECO", "Fondeco IFD"),
    ("FUBODE", "Fubode IFD"),
]

TARGET_ACCOUNTS: list[tuple[str, str, AccountCategory]] = [
    ("CARTERA BRUTA", "CARTERA_BRUTA", AccountCategory.CARTERA),
    ("CARTERA VIGENTE", "CARTERA_VIGENTE", AccountCategory.CARTERA),
    ("CARTERA VENCIDA", "CARTERA_VENCIDA", AccountCategory.CARTERA),
    ("CARTERA EN EJECUCIÓN", "CARTERA_EN_EJECUCION", AccountCategory.CARTERA),
    (
        "PREVISIÓN PARA CARTERA INCOBRABLE",
        "PREVISION_CARTERA",
        AccountCategory.CARTERA,
    ),
    ("TOTAL DEL ACTIVO", "TOTAL_ACTIVO", AccountCategory.ACTIVO),
    ("TOTAL DEL PASIVO", "TOTAL_PASIVO", AccountCategory.PASIVO),
    ("TOTAL DEL PATRIMONIO", "TOTAL_PATRIMONIO", AccountCategory.PATRIMONIO),
]


class FinruralParserError(ProspectorError):
    """Fallo durante la extracción estructurada del PDF de FINRURAL."""


class FinruralPdfParser:
    """Parser para extraer cuadros financieros de documentos PDF de FINRURAL."""

    def __init__(self) -> None:
        if not shutil.which("pdftotext"):
            raise FinruralParserError(
                "La utilidad del sistema 'pdftotext' no está disponible en el PATH."
            )

    def extract_text_layout(
        self,
        pdf_path: Path,
        first_page: int = 1,
        last_page: int = 1,
    ) -> str:
        """Extrae el texto respetando el layout tabular de las páginas deseadas."""
        cmd = [
            "pdftotext",
            "-layout",
            "-f",
            str(first_page),
            "-l",
            str(last_page),
            str(pdf_path),
            "-",
        ]
        try:
            res = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=15.0,
            )
            return res.stdout
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as err:
            raise FinruralParserError(
                f"Fallo al ejecutar pdftotext sobre '{pdf_path}': {err}"
            ) from err

    def parse_period(self, text: str) -> tuple[int, int, str]:
        """Extrae la fecha de corte del reporte a partir del texto."""
        date_pattern = re.compile(
            r"(?:al\s+)?\d{1,2}\s+de\s+([a-zA-ZáéíóúÁÉÍÓÚ]+)\s+de\s+(\d{4})",
            re.IGNORECASE,
        )
        match = date_pattern.search(text)
        if not match:
            raise FinruralParserError(
                "No fue posible encontrar la fecha de corte en el encabezado del PDF."
            )

        month_str = match.group(1).lower().replace("é", "e").replace("ó", "o")
        year = int(match.group(2))

        mes = SPANISH_MONTHS.get(month_str)
        if not mes:
            raise FinruralParserError(
                f"Mes '{month_str}' no reconocido en el encabezado del documento."
            )

        period = f"{year:04d}-{mes:02d}"
        return year, mes, period

    def parse_row_numbers(self, line: str, account_prefix: str) -> list[Decimal]:
        """Extrae los valores numéricos correspondientes a una fila de balance."""
        tail = line.replace(account_prefix, "")
        tokens = re.findall(r"[-]?[\d,]+(?:\.\d+)?|-", tail)
        numbers: list[Decimal] = []
        for t in tokens:
            clean = t.replace(",", "")
            if clean == "-":
                numbers.append(Decimal("0"))
            else:
                try:
                    numbers.append(Decimal(clean))
                except Exception:
                    continue
        return numbers

    def parse_cuadro_patrimonial(
        self,
        pdf_path: Path,
        source_file_sha256: str,
    ) -> list[SilverFinancialRecord]:
        """Parsea el Cuadro 1 (Situación Patrimonial) generando registros Silver."""
        text = self.extract_text_layout(pdf_path, first_page=1, last_page=1)
        gestion, mes, period = self.parse_period(text)

        lines = text.split("\n")
        records: list[SilverFinancialRecord] = []

        for row_label, canon_name, category in TARGET_ACCOUNTS:
            matched_line = None
            for line in lines:
                if row_label in line and "PRODUCTOS" not in line:
                    matched_line = line
                    break

            if not matched_line:
                continue

            numbers = self.parse_row_numbers(matched_line, row_label)
            if len(numbers) < 8:
                continue

            # Valores para las 8 instituciones estándar
            inst_values = numbers[:8]
            reported_total = numbers[8] if len(numbers) > 8 else sum(inst_values)

            # Validación de cuadre contable (tolerancia de 2 bolivianos por redondeo)
            computed_sum = sum(inst_values)
            if abs(computed_sum - reported_total) > Decimal("2.0"):
                continue

            for (inst_id, inst_name), amount in zip(
                STANDARD_INSTITUTIONS, inst_values
            ):
                records.append(
                    SilverFinancialRecord(
                        source_id="finrural",
                        institution_id=inst_id,
                        institution_name=inst_name,
                        period=period,
                        gestion=gestion,
                        mes=mes,
                        statement_type=StatementType.SITUACION_PATRIMONIAL,
                        account_category=category,
                        account_name=canon_name,
                        amount_bob=amount,
                        source_file_sha256=source_file_sha256,
                    )
                )

        if not records:
            raise FinruralParserError(
                f"No se pudieron extraer registros válidos de '{pdf_path}'."
            )

        return records
