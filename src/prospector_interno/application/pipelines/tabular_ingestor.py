"""Ingestores especializados multi-formato hacia los 4 Repositorios Gold.

Soporta la extracción real desde archivos custodiados en Capa Bronze (`.dat`):
- Libros Excel (`.xlsx` abiertos en flujo binario `rb` con `openpyxl`) del BCB y reportes tabulares.
- Estados Financieros PDF (`pdftotext -layout`) de la Bolsa Boliviana de Valores (BBV).
- Archivos CSV tabulares y consolidación prudencial supervisada para ASFI.
"""

from __future__ import annotations

import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
import io
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any

import openpyxl

from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
    GoldMartEstadisticasBbvRecord,
    GoldMartIndicadoresAsfiRecord,
    GoldMartTasasBcbRecord,
)
from prospector_interno.domain.models import BronzeArtifact


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
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def detect_binary_kind(path: Path) -> str:
    """Detecta el formato físico de un artefacto Bronze por sus primeros bytes."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(16)
    except OSError:
        return "unknown"

    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"PK\x03\x04"):
        return "xlsx"
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        return "xls"
    try:
        sample = head.decode("utf-8")
        if "," in sample or ";" in sample or sample.strip():
            return "csv"
    except UnicodeDecodeError:
        pass
    return "unknown"


def _to_decimal(val: Any, scale: Decimal = Decimal("1")) -> Decimal:
    """Convierte valores numéricos o cadenas contables a Decimal seguro."""
    if val is None or isinstance(val, bool):
        return Decimal("0")
    if isinstance(val, Decimal):
        return val * scale
    if isinstance(val, (int, float)):
        return Decimal(str(val)) * scale

    text = str(val).strip()
    if not text or text in {"-", "—", "n.d.", "N/A", "s/d"}:
        return Decimal("0")

    neg = text.startswith("(") and text.endswith(")")
    clean = text.strip("()").replace(" ", "")
    # Si contiene tanto coma como punto, eliminar separador de miles
    if "," in clean and "." in clean:
        if clean.rfind(",") > clean.rfind("."):
            clean = clean.replace(".", "").replace(",", ".")
        else:
            clean = clean.replace(",", "")
    elif "," in clean:
        # En balances BBV '38,445' representa miles enteros (3 digits tras coma)
        parts = clean.split(",")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            clean = "".join(parts)
        else:
            clean = clean.replace(",", ".")

    try:
        res = Decimal(clean) * scale
        return -res if neg else res
    except InvalidOperation:
        return Decimal("0")


class BcbWorkbookIngestor:
    """Extrae series monetarias, cambiarias e inflacionarias desde libros Excel del BCB."""

    _CACHE: dict[tuple[str, str, int, int], list[GoldMartTasasBcbRecord]] = {}

    def __init__(self, min_gestion: int = 2020) -> None:
        self._min_gestion = min_gestion

    def parse_artifact(self, artifact: BronzeArtifact) -> list[GoldMartTasasBcbRecord]:
        path = Path(artifact.file_path)
        if not path.exists():
            return []
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            return []
        cache_key = (str(path.resolve()), artifact.sha256_hash, mtime_ns, self._min_gestion)
        cached = self._CACHE.get(cache_key)
        if cached is not None:
            return list(cached)

        kind = detect_binary_kind(path)
        if kind == "xlsx":
            res = self._parse_xlsx(path, artifact.sha256_hash)
        elif kind == "csv":
            res = self._parse_csv(path, artifact.sha256_hash)
        else:
            res = []
        self._CACHE[cache_key] = res
        return list(res)

    def _parse_xlsx(self, path: Path, sha256: str) -> list[GoldMartTasasBcbRecord]:
        records_by_ym: dict[tuple[int, int], tuple[datetime, GoldMartTasasBcbRecord]] = {}
        try:
            with open(path, "rb") as fh:
                wb = openpyxl.load_workbook(fh, read_only=True, data_only=True)
                try:
                    for sheet_name in wb.sheetnames:
                        ws = wb[sheet_name]
                        rows = list(ws.iter_rows(values_only=True))
                        if not rows:
                            continue
                        # Caso 1: Formato Estadística Semanal / Boletín Mensual BCB (fechas en columnas)
                        col_dates = self._find_date_columns(rows[:10])
                        if col_dates:
                            parsed = self._extract_bcb_matrix(rows, col_dates, sha256)
                            for dt, rec in parsed:
                                ym = (rec.gestion, rec.mes)
                                prev = records_by_ym.get(ym)
                                if prev is None or dt >= prev[0]:
                                    records_by_ym[ym] = (dt, rec)
                        else:
                            # Caso 2: Tabla tabular plana con columnas gestion, mes, ...
                            tabular = self._extract_tabular_rows(rows, sha256)
                            for rec in tabular:
                                ym = (rec.gestion, rec.mes)
                                records_by_ym[ym] = (datetime(rec.gestion, rec.mes, 1), rec)
                finally:
                    wb.close()
        except Exception:
            return []

        return [pair[1] for _, pair in sorted(records_by_ym.items())]

    def _find_date_columns(
        self,
        header_rows: list[tuple[Any, ...]],
    ) -> dict[int, datetime]:
        best_cols: dict[int, datetime] = {}
        for row in header_rows:
            current: dict[int, datetime] = {}
            for idx, cell in enumerate(row):
                if isinstance(cell, datetime) and cell.year >= self._min_gestion:
                    current[idx] = cell
            if len(current) > len(best_cols):
                best_cols = current
        return best_cols

    def _extract_bcb_matrix(
        self,
        rows: list[tuple[Any, ...]],
        col_dates: dict[int, datetime],
        sha256: str,
    ) -> list[tuple[datetime, GoldMartTasasBcbRecord]]:
        metric_rows: dict[str, tuple[Any, ...]] = {}
        section = ""

        for idx, row in enumerate(rows):
            raw_lbl = ""
            for col_candidate in (3, 2, 1, 0):
                if col_candidate < len(row) and isinstance(row[col_candidate], str):
                    if row[col_candidate].strip():
                        raw_lbl = row[col_candidate].strip()
                        break
            if not raw_lbl:
                continue

            low = raw_lbl.lower()
            if "depósitos en entidades de intermediación" in low:
                section = "depositos"
            elif "a la vista" in low or "caja de ahorro" in low or "encaje constituido" in low:
                if section == "depositos":
                    section = "sub_depositos"
            elif "crédito del sistema financiero al sector privado" in low:
                section = "cartera"
            elif "reservas internacionales" in low and section == "cartera":
                section = "rin"

            if low == "base monetaria" and "base_monetaria" not in metric_rows:
                metric_rows["base_monetaria"] = row
            elif low == "emisión monetaria" and "emision_monetaria" not in metric_rows:
                metric_rows["emision_monetaria"] = row
            elif low == "reservas bancarias" and "reservas_bancarias" not in metric_rows:
                metric_rows["reservas_bancarias"] = row
            elif "variación mensual inflación total" in low and "inflacion" not in metric_rows:
                metric_rows["inflacion"] = row
            elif (
                section == "depositos"
                and "pro memoria: moneda nacional (en millones de bs)" in low
                and "depositos_bs" not in metric_rows
            ):
                metric_rows["depositos_bs"] = row
            elif (
                section == "depositos"
                and "bolivianización (%)" in low
                and "bolivianizacion" not in metric_rows
            ):
                metric_rows["bolivianizacion"] = row
            elif (
                section == "cartera"
                and "pro memoria: moneda nacional (en millones de bs)" in low
                and "cartera_bs" not in metric_rows
            ):
                metric_rows["cartera_bs"] = row
            elif (
                "reservas internacionales netas (i + ii + iii + iv - g)" in low
                and "rin_usd" not in metric_rows
            ):
                metric_rows["rin_usd"] = row
            elif "ufv (bs/ufv último día del mes)" in low and "ufv" not in metric_rows:
                metric_rows["ufv"] = row
            elif "ufv (bs/ufv día hábil" in low and "ufv_fallback" not in metric_rows:
                metric_rows["ufv_fallback"] = row

        if "base_monetaria" not in metric_rows:
            return []

        million = Decimal("1000000")
        pct_scale = Decimal("0.01")
        out: list[tuple[datetime, GoldMartTasasBcbRecord]] = []

        for col_idx, dt in sorted(col_dates.items(), key=lambda item: item[1]):
            def _cell(key: str) -> Any:
                r = metric_rows.get(key)
                if r is None or col_idx >= len(r):
                    return None
                return r[col_idx]

            base_bs = _to_decimal(_cell("base_monetaria"), million).quantize(Decimal("0.01"))
            if base_bs <= Decimal("0"):
                continue

            emision_bs = _to_decimal(_cell("emision_monetaria"), million).quantize(Decimal("0.01"))
            reservas_bs = _to_decimal(_cell("reservas_bancarias"), million).quantize(Decimal("0.01"))
            dep_bs = _to_decimal(_cell("depositos_bs"), million).quantize(Decimal("0.01"))
            cart_bs = _to_decimal(_cell("cartera_bs"), million).quantize(Decimal("0.01"))
            rin_usd = _to_decimal(_cell("rin_usd"), million).quantize(Decimal("0.01"))
            infl_pct = _to_decimal(_cell("inflacion"), pct_scale).quantize(Decimal("0.0001"))
            boliv_pct = _to_decimal(_cell("bolivianizacion"), pct_scale).quantize(Decimal("0.0001"))
            ufv_raw = _cell("ufv") if _cell("ufv") is not None else _cell("ufv_fallback")
            ufv_val = _to_decimal(ufv_raw).quantize(Decimal("0.00001"))

            out.append(
                (
                    dt,
                    GoldMartTasasBcbRecord(
                        gestion=dt.year,
                        mes=dt.month,
                        institucion_id="BCB",
                        institucion_nombre="Banco Central de Bolivia",
                        base_monetaria_bs=base_bs,
                        emision_monetaria_bs=emision_bs,
                        reservas_bancarias_bs=reservas_bs,
                        depositos_sistema_bs=dep_bs,
                        cartera_sistema_bs=cart_bs,
                        reservas_internacionales_usd=rin_usd,
                        inflacion_mensual_pct=infl_pct,
                        bolivianizacion_depositos_pct=boliv_pct,
                        ufv_cierre=ufv_val,
                        source_file_sha256=sha256,
                    ),
                )
            )
        return out

    def _extract_tabular_rows(
        self,
        rows: list[tuple[Any, ...]],
        sha256: str,
    ) -> list[GoldMartTasasBcbRecord]:
        if len(rows) < 2:
            return []
        headers = [str(c or "").strip().lower() for c in rows[0]]
        if "gestion" not in headers or "mes" not in headers:
            return []
        idx = {h: i for i, h in enumerate(headers) if h}
        out: list[GoldMartTasasBcbRecord] = []
        for row in rows[1:]:
            try:
                g = int(row[idx["gestion"]])
                m = int(row[idx["mes"]])
                out.append(
                    GoldMartTasasBcbRecord(
                        gestion=g,
                        mes=m,
                        institucion_id=str(row[idx["institucion_id"]]) if "institucion_id" in idx and row[idx["institucion_id"]] else "BCB",
                        institucion_nombre=str(row[idx["institucion_nombre"]]) if "institucion_nombre" in idx and row[idx["institucion_nombre"]] else "Banco Central de Bolivia",
                        base_monetaria_bs=_to_decimal(row[idx["base_monetaria_bs"]]) if "base_monetaria_bs" in idx else Decimal("0"),
                        emision_monetaria_bs=_to_decimal(row[idx["emision_monetaria_bs"]]) if "emision_monetaria_bs" in idx else Decimal("0"),
                        reservas_bancarias_bs=_to_decimal(row[idx["reservas_bancarias_bs"]]) if "reservas_bancarias_bs" in idx else Decimal("0"),
                        depositos_sistema_bs=_to_decimal(row[idx["depositos_sistema_bs"]]) if "depositos_sistema_bs" in idx else Decimal("0"),
                        cartera_sistema_bs=_to_decimal(row[idx["cartera_sistema_bs"]]) if "cartera_sistema_bs" in idx else Decimal("0"),
                        reservas_internacionales_usd=_to_decimal(row[idx["reservas_internacionales_usd"]]) if "reservas_internacionales_usd" in idx else Decimal("0"),
                        inflacion_mensual_pct=_to_decimal(row[idx["inflacion_mensual_pct"]]) if "inflacion_mensual_pct" in idx else Decimal("0"),
                        bolivianizacion_depositos_pct=_to_decimal(row[idx["bolivianizacion_depositos_pct"]]) if "bolivianizacion_depositos_pct" in idx else Decimal("0"),
                        ufv_cierre=_to_decimal(row[idx["ufv_cierre"]]) if "ufv_cierre" in idx else Decimal("0"),
                        source_file_sha256=sha256,
                    )
                )
            except Exception:
                continue
        return out

    def _parse_csv(self, path: Path, sha256: str) -> list[GoldMartTasasBcbRecord]:
        try:
            text = path.read_text(encoding="utf-8-sig")
            reader = list(csv.reader(io.StringIO(text)))
            return self._extract_tabular_rows([tuple(r) for r in reader], sha256)
        except Exception:
            return []


class BbvFinancialIngestor:
    """Extrae Estados Financieros auditados / mensuales desde PDFs y tablas de la BBV."""

    _CACHE: dict[tuple[str, str, int], list[GoldMartEstadisticasBbvRecord]] = {}

    def parse_artifact(
        self,
        artifact: BronzeArtifact,
    ) -> list[GoldMartEstadisticasBbvRecord]:
        path = Path(artifact.file_path)
        if not path.exists():
            return []
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            return []
        cache_key = (str(path.resolve()), artifact.sha256_hash, mtime_ns)
        cached = self._CACHE.get(cache_key)
        if cached is not None:
            return list(cached)

        kind = detect_binary_kind(path)
        if kind == "pdf":
            res = self._parse_balance_pdf(path, artifact.sha256_hash)
        elif kind in {"xlsx", "csv"}:
            res = self._parse_tabular(path, kind, artifact.sha256_hash)
        else:
            res = []
        self._CACHE[cache_key] = res
        return list(res)

    def _parse_balance_pdf(
        self,
        path: Path,
        sha256: str,
    ) -> list[GoldMartEstadisticasBbvRecord]:
        if not shutil.which("pdftotext"):
            return []
        try:
            proc = subprocess.run(
                ["pdftotext", "-f", "1", "-l", "10", "-layout", str(path), "-"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            text = proc.stdout or ""
        except Exception:
            return []

        upper = text.upper()
        if "BALANCE GENERAL" not in upper or "TOTAL ACTIVO" not in upper:
            return []

        # Extraer fecha del Balance General: "Balance General al 31 de marzo de 2026"
        date_match = re.search(
            r"Balance\s+General\s+al\s+\d{1,2}\s+de\s+([a-zA-ZáéíóúÁÉÍÓÚ]+)\s+de\s+(\d{4})",
            text,
            flags=re.IGNORECASE,
        )
        if not date_match:
            return []

        month_name = date_match.group(1).strip().lower()
        mes = SPANISH_MONTHS.get(month_name)
        gestion = int(date_match.group(2))
        if not mes or not (1990 <= gestion <= 2100):
            return []

        # Escala: "(Expresado en miles de bolivianos)" -> x1000 Bs.
        scale = (
            Decimal("1000")
            if "MILES DE BOLIVIANOS" in upper
            else Decimal("1")
        )

        # Cortar solo la sección de Balance General antes de Estado de Resultados
        balance_section = re.split(
            r"Estado\s+de\s+Resultados", text, maxsplit=1, flags=re.IGNORECASE
        )[0]

        disponible = Decimal("0")
        inv_bursatiles = Decimal("0")
        activo_corriente = Decimal("0")
        inv_permanentes = Decimal("0")
        total_activo = Decimal("0")
        pasivo_corriente = Decimal("0")
        total_pasivo = Decimal("0")
        total_patrimonio = Decimal("0")

        num_tail_re = re.compile(r"([\(\-]?\d[\d,\.]*\)?)\s*$")

        for raw_line in balance_section.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            m = num_tail_re.search(line)
            if not m:
                continue
            amount = _to_decimal(m.group(1), scale).quantize(Decimal("0.01"))
            up_line = line.upper()

            if up_line.startswith("1010000") or " - DISPONIBLE" in up_line:
                disponible = amount
            elif up_line.startswith(("1020000", "1030000", "1040000")) or "INVERSIONES BURS" in up_line:
                inv_bursatiles += amount
            elif up_line.startswith("TOTAL ACTIVO CORRIENTE"):
                activo_corriente = amount
            elif up_line.startswith("1200000") or "INVERSIONES PERMANENTES" in up_line:
                inv_permanentes = amount
            elif up_line.startswith("TOTAL ACTIVO") and "CORRIENTE" not in up_line:
                total_activo = amount
            elif up_line.startswith("TOTAL PASIVO CORRIENTE"):
                pasivo_corriente = amount
            elif (
                up_line.startswith("TOTAL PASIVO")
                and "CORRIENTE" not in up_line
                and "PATRIMONIO" not in up_line
            ):
                total_pasivo = amount
            elif up_line.startswith("TOTAL PATRIMONIO"):
                total_patrimonio = amount

        if total_activo <= Decimal("0"):
            return []

        ratio_liquidez = (
            (activo_corriente / pasivo_corriente).quantize(Decimal("0.0001"))
            if pasivo_corriente > Decimal("0")
            else Decimal("0.0000")
        )
        ratio_endeudamiento = (
            (total_pasivo / total_patrimonio).quantize(Decimal("0.0001"))
            if total_patrimonio > Decimal("0")
            else Decimal("0.0000")
        )

        return [
            GoldMartEstadisticasBbvRecord(
                gestion=gestion,
                mes=mes,
                institucion_id="BBV",
                institucion_nombre="Bolsa Boliviana de Valores S.A.",
                disponible_bs=disponible,
                inversiones_bursatiles_bs=inv_bursatiles,
                activo_corriente_bs=activo_corriente,
                inversiones_permanentes_bs=inv_permanentes,
                total_activo=total_activo,
                pasivo_corriente_bs=pasivo_corriente,
                total_pasivo=total_pasivo,
                total_patrimonio=total_patrimonio,
                ratio_liquidez=ratio_liquidez,
                ratio_endeudamiento=ratio_endeudamiento,
                source_file_sha256=sha256,
            )
        ]

    def _parse_tabular(
        self,
        path: Path,
        kind: str,
        sha256: str,
    ) -> list[GoldMartEstadisticasBbvRecord]:
        rows: list[tuple[Any, ...]] = []
        try:
            if kind == "xlsx":
                with open(path, "rb") as fh:
                    wb = openpyxl.load_workbook(fh, read_only=True, data_only=True)
                    try:
                        ws = wb[wb.sheetnames[0]]
                        rows = list(ws.iter_rows(values_only=True))
                    finally:
                        wb.close()
            else:
                text = path.read_text(encoding="utf-8-sig")
                rows = [tuple(r) for r in csv.reader(io.StringIO(text))]
        except Exception:
            return []

        if len(rows) < 2:
            return []
        headers = [str(c or "").strip().lower() for c in rows[0]]
        if "gestion" not in headers or "mes" not in headers:
            return []
        idx = {h: i for i, h in enumerate(headers) if h}
        out: list[GoldMartEstadisticasBbvRecord] = []
        for row in rows[1:]:
            try:
                g = int(row[idx["gestion"]])
                m = int(row[idx["mes"]])
                act_corr = _to_decimal(row[idx["activo_corriente_bs"]]) if "activo_corriente_bs" in idx else Decimal("0")
                pas_corr = _to_decimal(row[idx["pasivo_corriente_bs"]]) if "pasivo_corriente_bs" in idx else Decimal("0")
                tot_act = _to_decimal(row[idx["total_activo"]]) if "total_activo" in idx else Decimal("0")
                tot_pas = _to_decimal(row[idx["total_pasivo"]]) if "total_pasivo" in idx else Decimal("0")
                tot_pat = _to_decimal(row[idx["total_patrimonio"]]) if "total_patrimonio" in idx else Decimal("0")
                liq = (act_corr / pas_corr).quantize(Decimal("0.0001")) if pas_corr > Decimal("0") else Decimal("0.0000")
                end = (tot_pas / tot_pat).quantize(Decimal("0.0001")) if tot_pat > Decimal("0") else Decimal("0.0000")
                out.append(
                    GoldMartEstadisticasBbvRecord(
                        gestion=g,
                        mes=m,
                        institucion_id=str(row[idx["institucion_id"]]) if "institucion_id" in idx and row[idx["institucion_id"]] else "BBV",
                        institucion_nombre=str(row[idx["institucion_nombre"]]) if "institucion_nombre" in idx and row[idx["institucion_nombre"]] else "Bolsa Boliviana de Valores S.A.",
                        disponible_bs=_to_decimal(row[idx["disponible_bs"]]) if "disponible_bs" in idx else Decimal("0"),
                        inversiones_bursatiles_bs=_to_decimal(row[idx["inversiones_bursatiles_bs"]]) if "inversiones_bursatiles_bs" in idx else Decimal("0"),
                        activo_corriente_bs=act_corr,
                        inversiones_permanentes_bs=_to_decimal(row[idx["inversiones_permanentes_bs"]]) if "inversiones_permanentes_bs" in idx else Decimal("0"),
                        total_activo=tot_act,
                        pasivo_corriente_bs=pas_corr,
                        total_pasivo=tot_pas,
                        total_patrimonio=tot_pat,
                        ratio_liquidez=liq,
                        ratio_endeudamiento=end,
                        source_file_sha256=sha256,
                    )
                )
            except Exception:
                continue
        return out


class AsfiPrudentialIngestor:
    """Genera indicadores prudenciales supervisados ASFI desde archivos directos o entidades reguladas."""

    def parse_artifact(
        self,
        artifact: BronzeArtifact,
    ) -> list[GoldMartIndicadoresAsfiRecord]:
        path = Path(artifact.file_path)
        if not path.exists():
            return []
        kind = detect_binary_kind(path)
        if kind not in {"xlsx", "csv"}:
            return []

        rows: list[tuple[Any, ...]] = []
        try:
            if kind == "xlsx":
                with open(path, "rb") as fh:
                    wb = openpyxl.load_workbook(fh, read_only=True, data_only=True)
                    try:
                        ws = wb[wb.sheetnames[0]]
                        rows = list(ws.iter_rows(values_only=True))
                    finally:
                        wb.close()
            else:
                text = path.read_text(encoding="utf-8-sig")
                rows = [tuple(r) for r in csv.reader(io.StringIO(text))]
        except Exception:
            return []

        if len(rows) < 2:
            return []
        headers = [str(c or "").strip().lower() for c in rows[0]]
        if "gestion" not in headers or "mes" not in headers or "total_activo" not in headers:
            return []
        idx = {h: i for i, h in enumerate(headers) if h}
        out: list[GoldMartIndicadoresAsfiRecord] = []
        for row in rows[1:]:
            try:
                g = int(row[idx["gestion"]])
                m = int(row[idx["mes"]])
                inst_id = str(row[idx["institucion_id"]]) if "institucion_id" in idx and row[idx["institucion_id"]] else "SISTEMA_ASFI"
                inst_name = str(row[idx["institucion_nombre"]]) if "institucion_nombre" in idx and row[idx["institucion_nombre"]] else inst_id
                activo = _to_decimal(row[idx["total_activo"]])
                pasivo = _to_decimal(row[idx["total_pasivo"]]) if "total_pasivo" in idx else Decimal("0")
                patrimonio = _to_decimal(row[idx["total_patrimonio"]]) if "total_patrimonio" in idx else (activo - pasivo)
                bruta = _to_decimal(row[idx["cartera_bruta"]]) if "cartera_bruta" in idx else Decimal("0")
                mora = _to_decimal(row[idx["cartera_en_mora"]]) if "cartera_en_mora" in idx else Decimal("0")
                prevision = _to_decimal(row[idx["prevision_cartera"]]) if "prevision_cartera" in idx else Decimal("0")

                out.append(
                    self._build_record(
                        gestion=g,
                        mes=m,
                        inst_id=inst_id,
                        inst_name=inst_name,
                        activo=activo,
                        pasivo=pasivo,
                        patrimonio=patrimonio,
                        bruta=bruta,
                        mora=mora,
                        prevision=prevision,
                        sha256=artifact.sha256_hash,
                    )
                )
            except Exception:
                continue
        return out

    def derive_from_supervised_entities(
        self,
        cartera_records: list[GoldMartCarteraRecord],
    ) -> list[GoldMartIndicadoresAsfiRecord]:
        """Calcula ratios prudenciales ASFI sobre los estados financieros de entidades supervisadas."""
        out: list[GoldMartIndicadoresAsfiRecord] = []
        for r in cartera_records:
            out.append(
                self._build_record(
                    gestion=r.gestion,
                    mes=r.mes,
                    inst_id=r.institucion_id,
                    inst_name=r.institucion_nombre,
                    activo=r.total_activo,
                    pasivo=r.total_pasivo,
                    patrimonio=r.total_patrimonio,
                    bruta=r.cartera_bruta,
                    mora=r.cartera_en_mora,
                    prevision=r.prevision_cartera,
                    sha256=r.source_file_sha256,
                )
            )
        return out

    @staticmethod
    def _build_record(
        *,
        gestion: int,
        mes: int,
        inst_id: str,
        inst_name: str,
        activo: Decimal,
        pasivo: Decimal,
        patrimonio: Decimal,
        bruta: Decimal,
        mora: Decimal,
        prevision: Decimal,
        sha256: str,
    ) -> GoldMartIndicadoresAsfiRecord:
        solvencia = (
            (patrimonio / activo).quantize(Decimal("0.0001"))
            if activo > Decimal("0")
            else Decimal("0.0000")
        )
        peso_cartera = (
            (bruta / activo).quantize(Decimal("0.0001"))
            if activo > Decimal("0")
            else Decimal("0.0000")
        )
        r_mora = (
            (mora / bruta).quantize(Decimal("0.0001"))
            if bruta > Decimal("0")
            else Decimal("0.0000")
        )
        r_cobertura = (
            (prevision / mora).quantize(Decimal("0.0001"))
            if mora > Decimal("0")
            else Decimal("0.0000")
        )
        apalancamiento = (
            (pasivo / patrimonio).quantize(Decimal("0.0001"))
            if patrimonio > Decimal("0")
            else Decimal("0.0000")
        )
        return GoldMartIndicadoresAsfiRecord(
            gestion=gestion,
            mes=mes,
            institucion_id=inst_id,
            institucion_nombre=inst_name,
            total_activo=activo,
            total_pasivo=pasivo,
            total_patrimonio=patrimonio,
            cartera_bruta=bruta,
            prevision_cartera=prevision,
            coeficiente_solvencia=solvencia,
            peso_cartera_activo=peso_cartera,
            ratio_mora=r_mora,
            ratio_cobertura=r_cobertura,
            apalancamiento_veces=apalancamiento,
            source_file_sha256=sha256,
        )
