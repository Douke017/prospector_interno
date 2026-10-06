"""Materializador dimensional para la Capa Gold (Parquet particionado)."""

from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any
import duckdb

from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
)
from prospector_interno.domain.contracts.lineage_spec import StorageLayer
from prospector_interno.domain.contracts.silver_spec import (
    SilverFinancialRecord,
)


class GoldMaterializer:
    """Consolida registros de Capa Silver en Datamarts Parquet particionados."""

    def __init__(
        self,
        base_gold_path: Path | None = None,
        lineage_tracker: Any = None,
    ) -> None:
        self._base_path = base_gold_path or Path("data/gold")
        self._lineage_tracker = lineage_tracker

    def transform_to_mart_cartera(

        self,
        silver_records: list[SilverFinancialRecord],
    ) -> list[GoldMartCarteraRecord]:
        """Agrupa y pivota registros atómicos de Silver en registros de Gold."""
        # Agrupar por (gestion, mes, institucion_id, institucion_nombre, sha)
        grouped: dict[tuple[int, int, str, str, str], dict[str, Decimal]] = (
            defaultdict(dict)
        )

        for rec in silver_records:
            key = (
                rec.gestion,
                rec.mes,
                rec.institution_id,
                rec.institution_name,
                rec.source_file_sha256,
            )
            grouped[key][rec.account_name] = rec.amount_bob

        gold_records: list[GoldMartCarteraRecord] = []

        for (
            gestion,
            mes,
            inst_id,
            inst_name,
            sha,
        ), accounts in grouped.items():
            bruta = accounts.get("CARTERA_BRUTA", Decimal("0"))
            vigente = accounts.get("CARTERA_VIGENTE", Decimal("0"))
            vencida = accounts.get("CARTERA_VENCIDA", Decimal("0"))
            ejecucion = accounts.get("CARTERA_EN_EJECUCION", Decimal("0"))
            mora = vencida + ejecucion
            prevision = abs(accounts.get("PREVISION_CARTERA", Decimal("0")))

            activo = accounts.get("TOTAL_ACTIVO", Decimal("0"))
            pasivo = accounts.get("TOTAL_PASIVO", Decimal("0"))
            patrimonio = accounts.get("TOTAL_PATRIMONIO", Decimal("0"))

            ratio_mora = (
                (mora / bruta).quantize(Decimal("0.0001"))
                if bruta > Decimal("0")
                else Decimal("0.0000")
            )
            ratio_cobertura = (
                (prevision / mora).quantize(Decimal("0.0001"))
                if mora > Decimal("0")
                else Decimal("0.0000")
            )

            gold_records.append(
                GoldMartCarteraRecord(
                    gestion=gestion,
                    mes=mes,
                    institucion_id=inst_id,
                    institucion_nombre=inst_name,
                    cartera_bruta=bruta,
                    cartera_vigente=vigente,
                    cartera_vencida=vencida,
                    cartera_en_ejecucion=ejecucion,
                    cartera_en_mora=mora,
                    prevision_cartera=prevision,
                    total_activo=activo,
                    total_pasivo=pasivo,
                    total_patrimonio=patrimonio,
                    ratio_mora=ratio_mora,
                    ratio_cobertura=ratio_cobertura,
                    source_file_sha256=sha,
                )
            )

        return gold_records

    def materialize_mart_cartera(
        self,
        gold_records: list[GoldMartCarteraRecord],
        external_run_id: str = "RUN_EXT_UNKNOWN",
    ) -> Path:
        """Escribe las particiones Parquet en disco usando DuckDB vectorizado."""

        mart_dir = self._base_path / "mart_cartera"
        mart_dir.mkdir(parents=True, exist_ok=True)

        if not gold_records:
            return mart_dir

        # Ingesta en tabla DuckDB en memoria para particionado nativo Parquet
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE buffer (
                institucion_id VARCHAR,
                institucion_nombre VARCHAR,
                cartera_bruta DOUBLE,
                cartera_vigente DOUBLE,
                cartera_vencida DOUBLE,
                cartera_en_ejecucion DOUBLE,
                cartera_en_mora DOUBLE,
                prevision_cartera DOUBLE,
                total_activo DOUBLE,
                total_pasivo DOUBLE,
                total_patrimonio DOUBLE,
                ratio_mora DOUBLE,
                ratio_cobertura DOUBLE,
                source_file_sha256 VARCHAR,
                transformed_at VARCHAR,
                gestion INT,
                mes INT
            )
        """
        )

        rows: list[tuple[Any, ...]] = []
        for r in gold_records:
            rows.append(
                (
                    r.institucion_id,
                    r.institucion_nombre,
                    float(r.cartera_bruta),
                    float(r.cartera_vigente),
                    float(r.cartera_vencida),
                    float(r.cartera_en_ejecucion),
                    float(r.cartera_en_mora),
                    float(r.prevision_cartera),
                    float(r.total_activo),
                    float(r.total_pasivo),
                    float(r.total_patrimonio),
                    float(r.ratio_mora),
                    float(r.ratio_cobertura),
                    r.source_file_sha256,
                    r.transformed_at.isoformat(),
                    r.gestion,
                    r.mes,
                )
            )

        placeholders = ",".join(["?"] * 17)
        con.executemany(f"INSERT INTO buffer VALUES ({placeholders})", rows)

        # Escritura atómica particionada por gestion y mes
        posix_mart = mart_dir.resolve().as_posix()
        con.execute(
            f"""
            COPY buffer TO '{posix_mart}'
            (FORMAT PARQUET, PARTITION_BY (gestion, mes), OVERWRITE_OR_IGNORE 1)
        """
        )

        if self._lineage_tracker and gold_records:
            partitions: dict[tuple[int, int], list[GoldMartCarteraRecord]] = (
                defaultdict(list)
            )
            for r in gold_records:
                partitions[(r.gestion, r.mes)].append(r)

            for (gestion, mes), recs in partitions.items():
                asset_id = f"gold_mart_cartera_{gestion}_{mes:02d}"
                part_dir = mart_dir / f"gestion={gestion}" / f"mes={mes}"
                files = list(part_dir.glob("*.parquet"))
                total_size = (
                    sum(f.stat().st_size for f in files) if files else 0
                )
                first_file = files[0] if files else part_dir
                period_str = f"{gestion}-{mes:02d}-01"
                self._lineage_tracker.register_asset(
                    asset_id=asset_id,
                    source_id="finrural",
                    dataset_id="mart_cartera",
                    period_start=period_str,
                    period_end=period_str,
                    storage_layer=StorageLayer.GOLD,
                    file_path=first_file,
                    row_count=len(recs),
                    size_bytes=total_size,
                )
                unique_shas = {r.source_file_sha256 for r in recs}
                for sha in unique_shas:
                    self._lineage_tracker.link_bronze_source(
                        gold_asset_id=asset_id,
                        bronze_source_file=f"insumo_bronze_{sha[:8]}.pdf",
                        bronze_file_sha256=sha,
                        external_run_id=external_run_id,
                    )

        return mart_dir
