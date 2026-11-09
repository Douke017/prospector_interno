"""Materializador dimensional para los 4 Repositorios Consolidados en Capa Gold."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any
import duckdb

from prospector_interno.application.pipelines.silver_pipeline import (
    SilverTransformPipeline,
)
from prospector_interno.application.pipelines.tabular_ingestor import (
    AsfiPrudentialIngestor,
    BbvFinancialIngestor,
    BcbWorkbookIngestor,
)
from prospector_interno.domain.contracts.gold_spec import (
    GoldMartCarteraRecord,
    GoldMartEstadisticasBbvRecord,
    GoldMartIndicadoresAsfiRecord,
    GoldMartTasasBcbRecord,
)
from prospector_interno.domain.contracts.lineage_spec import StorageLayer
from prospector_interno.domain.contracts.silver_spec import (
    SilverFinancialRecord,
)
from prospector_interno.domain.models import BronzeArtifact


class GoldMaterializer:
    """Consolida registros financieros en los 4 repositorios Parquet particionados."""

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

        by_entity_period: dict[tuple[int, int, str], GoldMartCarteraRecord] = {}

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

            record = GoldMartCarteraRecord(
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
            by_entity_period[(gestion, mes, inst_id)] = record

        return list(by_entity_period.values())

    @staticmethod
    def _clean_target_partitions(
        mart_dir: Path,
        partitions: set[tuple[int, int]],
    ) -> None:
        """Limpia archivos Parquet previos de cada partición antes de escribir de forma atómica."""
        for gestion, mes in partitions:
            part_dir = mart_dir / f"gestion={gestion}" / f"mes={mes}"
            if part_dir.exists():
                for old_pq in part_dir.glob("*.parquet"):
                    try:
                        old_pq.unlink()
                    except OSError:
                        pass

    def _register_partitions_lineage(
        self,
        *,
        datamart_name: str,
        source_id: str,
        mart_dir: Path,
        records: list[Any],
        external_run_id: str,
        bronze_path_map: dict[str, str] | None = None,
    ) -> None:
        if not self._lineage_tracker or not records:
            return

        partitions: dict[tuple[int, int], list[Any]] = defaultdict(list)
        for r in records:
            partitions[(r.gestion, r.mes)].append(r)

        prefix = (
            "gold_mart_cartera"
            if datamart_name == "mart_cartera"
            else f"gold_{datamart_name}"
        )

        for (gestion, mes), recs in partitions.items():
            asset_id = f"{prefix}_{gestion}_{mes:02d}"
            part_dir = mart_dir / f"gestion={gestion}" / f"mes={mes}"
            files = sorted(part_dir.glob("*.parquet"))
            total_size = sum(f.stat().st_size for f in files) if files else 0
            first_file = files[0] if files else part_dir
            period_str = f"{gestion}-{mes:02d}-01"

            self._lineage_tracker.register_asset(
                asset_id=asset_id,
                source_id=source_id,
                dataset_id=datamart_name,
                period_start=period_str,
                period_end=period_str,
                storage_layer=StorageLayer.GOLD,
                file_path=first_file,
                row_count=len(recs),
                size_bytes=total_size,
            )
            unique_shas = sorted({r.source_file_sha256 for r in recs if r.source_file_sha256})
            for sha in unique_shas:
                src_file = (
                    bronze_path_map.get(sha)
                    if bronze_path_map and sha in bronze_path_map
                    else f"insumo_bronze_{sha[:8]}.dat"
                )
                self._lineage_tracker.link_bronze_source(
                    gold_asset_id=asset_id,
                    bronze_source_file=src_file,
                    bronze_file_sha256=sha,
                    external_run_id=external_run_id,
                )

    def materialize_mart_cartera(
        self,
        gold_records: list[GoldMartCarteraRecord],
        external_run_id: str = "RUN_EXT_UNKNOWN",
        bronze_path_map: dict[str, str] | None = None,
    ) -> Path:
        """Escribe particiones Parquet de mart_cartera de forma idempotente."""
        mart_dir = self._base_path / "mart_cartera"
        mart_dir.mkdir(parents=True, exist_ok=True)
        if not gold_records:
            return mart_dir

        dedup: dict[tuple[int, int, str], GoldMartCarteraRecord] = {}
        for r in gold_records:
            dedup[(r.gestion, r.mes, r.institucion_id)] = r
        clean_records = list(dedup.values())

        self._clean_target_partitions(
            mart_dir,
            {(r.gestion, r.mes) for r in clean_records},
        )

        con = duckdb.connect(":memory:")
        try:
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
            rows = [
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
                for r in clean_records
            ]
            placeholders = ",".join(["?"] * 17)
            con.executemany(f"INSERT INTO buffer VALUES ({placeholders})", rows)
            posix_mart = mart_dir.resolve().as_posix()
            con.execute(
                f"""
                COPY buffer TO '{posix_mart}'
                (FORMAT PARQUET, PARTITION_BY (gestion, mes), OVERWRITE_OR_IGNORE 1)
                """
            )
        finally:
            con.close()

        self._register_partitions_lineage(
            datamart_name="mart_cartera",
            source_id="finrural",
            mart_dir=mart_dir,
            records=clean_records,
            external_run_id=external_run_id,
            bronze_path_map=bronze_path_map,
        )
        return mart_dir

    def materialize_mart_tasas_bcb(
        self,
        records: list[GoldMartTasasBcbRecord],
        external_run_id: str = "RUN_EXT_BCB",
        bronze_path_map: dict[str, str] | None = None,
    ) -> Path:
        """Escribe particiones Parquet de mart_tasas_bcb de forma idempotente."""
        mart_dir = self._base_path / "mart_tasas_bcb"
        mart_dir.mkdir(parents=True, exist_ok=True)
        if not records:
            return mart_dir

        dedup: dict[tuple[int, int, str], GoldMartTasasBcbRecord] = {}
        for r in records:
            dedup[(r.gestion, r.mes, r.institucion_id)] = r
        clean_records = list(dedup.values())

        self._clean_target_partitions(
            mart_dir,
            {(r.gestion, r.mes) for r in clean_records},
        )

        con = duckdb.connect(":memory:")
        try:
            con.execute(
                """
                CREATE TABLE buffer (
                    institucion_id VARCHAR,
                    institucion_nombre VARCHAR,
                    base_monetaria_bs DOUBLE,
                    emision_monetaria_bs DOUBLE,
                    reservas_bancarias_bs DOUBLE,
                    depositos_sistema_bs DOUBLE,
                    cartera_sistema_bs DOUBLE,
                    reservas_internacionales_usd DOUBLE,
                    inflacion_mensual_pct DOUBLE,
                    bolivianizacion_depositos_pct DOUBLE,
                    ufv_cierre DOUBLE,
                    source_file_sha256 VARCHAR,
                    transformed_at VARCHAR,
                    gestion INT,
                    mes INT
                )
                """
            )
            rows = [
                (
                    r.institucion_id,
                    r.institucion_nombre,
                    float(r.base_monetaria_bs),
                    float(r.emision_monetaria_bs),
                    float(r.reservas_bancarias_bs),
                    float(r.depositos_sistema_bs),
                    float(r.cartera_sistema_bs),
                    float(r.reservas_internacionales_usd),
                    float(r.inflacion_mensual_pct),
                    float(r.bolivianizacion_depositos_pct),
                    float(r.ufv_cierre),
                    r.source_file_sha256,
                    r.transformed_at.isoformat(),
                    r.gestion,
                    r.mes,
                )
                for r in clean_records
            ]
            placeholders = ",".join(["?"] * 15)
            con.executemany(f"INSERT INTO buffer VALUES ({placeholders})", rows)
            posix_mart = mart_dir.resolve().as_posix()
            con.execute(
                f"""
                COPY buffer TO '{posix_mart}'
                (FORMAT PARQUET, PARTITION_BY (gestion, mes), OVERWRITE_OR_IGNORE 1)
                """
            )
        finally:
            con.close()

        self._register_partitions_lineage(
            datamart_name="mart_tasas_bcb",
            source_id="bcb",
            mart_dir=mart_dir,
            records=clean_records,
            external_run_id=external_run_id,
            bronze_path_map=bronze_path_map,
        )
        return mart_dir

    def materialize_mart_estadisticas_bbv(
        self,
        records: list[GoldMartEstadisticasBbvRecord],
        external_run_id: str = "RUN_EXT_BBV",
        bronze_path_map: dict[str, str] | None = None,
    ) -> Path:
        """Escribe particiones Parquet de mart_estadisticas_bbv de forma idempotente."""
        mart_dir = self._base_path / "mart_estadisticas_bbv"
        mart_dir.mkdir(parents=True, exist_ok=True)
        if not records:
            return mart_dir

        dedup: dict[tuple[int, int, str], GoldMartEstadisticasBbvRecord] = {}
        for r in records:
            dedup[(r.gestion, r.mes, r.institucion_id)] = r
        clean_records = list(dedup.values())

        self._clean_target_partitions(
            mart_dir,
            {(r.gestion, r.mes) for r in clean_records},
        )

        con = duckdb.connect(":memory:")
        try:
            con.execute(
                """
                CREATE TABLE buffer (
                    institucion_id VARCHAR,
                    institucion_nombre VARCHAR,
                    disponible_bs DOUBLE,
                    inversiones_bursatiles_bs DOUBLE,
                    activo_corriente_bs DOUBLE,
                    inversiones_permanentes_bs DOUBLE,
                    total_activo DOUBLE,
                    pasivo_corriente_bs DOUBLE,
                    total_pasivo DOUBLE,
                    total_patrimonio DOUBLE,
                    ratio_liquidez DOUBLE,
                    ratio_endeudamiento DOUBLE,
                    source_file_sha256 VARCHAR,
                    transformed_at VARCHAR,
                    gestion INT,
                    mes INT
                )
                """
            )
            rows = [
                (
                    r.institucion_id,
                    r.institucion_nombre,
                    float(r.disponible_bs),
                    float(r.inversiones_bursatiles_bs),
                    float(r.activo_corriente_bs),
                    float(r.inversiones_permanentes_bs),
                    float(r.total_activo),
                    float(r.pasivo_corriente_bs),
                    float(r.total_pasivo),
                    float(r.total_patrimonio),
                    float(r.ratio_liquidez),
                    float(r.ratio_endeudamiento),
                    r.source_file_sha256,
                    r.transformed_at.isoformat(),
                    r.gestion,
                    r.mes,
                )
                for r in clean_records
            ]
            placeholders = ",".join(["?"] * 16)
            con.executemany(f"INSERT INTO buffer VALUES ({placeholders})", rows)
            posix_mart = mart_dir.resolve().as_posix()
            con.execute(
                f"""
                COPY buffer TO '{posix_mart}'
                (FORMAT PARQUET, PARTITION_BY (gestion, mes), OVERWRITE_OR_IGNORE 1)
                """
            )
        finally:
            con.close()

        self._register_partitions_lineage(
            datamart_name="mart_estadisticas_bbv",
            source_id="bbv",
            mart_dir=mart_dir,
            records=clean_records,
            external_run_id=external_run_id,
            bronze_path_map=bronze_path_map,
        )
        return mart_dir

    def materialize_mart_indicadores_asfi(
        self,
        records: list[GoldMartIndicadoresAsfiRecord],
        external_run_id: str = "RUN_EXT_ASFI",
        bronze_path_map: dict[str, str] | None = None,
    ) -> Path:
        """Escribe particiones Parquet de mart_indicadores_asfi de forma idempotente."""
        mart_dir = self._base_path / "mart_indicadores_asfi"
        mart_dir.mkdir(parents=True, exist_ok=True)
        if not records:
            return mart_dir

        dedup: dict[tuple[int, int, str], GoldMartIndicadoresAsfiRecord] = {}
        for r in records:
            dedup[(r.gestion, r.mes, r.institucion_id)] = r
        clean_records = list(dedup.values())

        self._clean_target_partitions(
            mart_dir,
            {(r.gestion, r.mes) for r in clean_records},
        )

        con = duckdb.connect(":memory:")
        try:
            con.execute(
                """
                CREATE TABLE buffer (
                    institucion_id VARCHAR,
                    institucion_nombre VARCHAR,
                    total_activo DOUBLE,
                    total_pasivo DOUBLE,
                    total_patrimonio DOUBLE,
                    cartera_bruta DOUBLE,
                    prevision_cartera DOUBLE,
                    coeficiente_solvencia DOUBLE,
                    peso_cartera_activo DOUBLE,
                    ratio_mora DOUBLE,
                    ratio_cobertura DOUBLE,
                    apalancamiento_veces DOUBLE,
                    source_file_sha256 VARCHAR,
                    transformed_at VARCHAR,
                    gestion INT,
                    mes INT
                )
                """
            )
            rows = [
                (
                    r.institucion_id,
                    r.institucion_nombre,
                    float(r.total_activo),
                    float(r.total_pasivo),
                    float(r.total_patrimonio),
                    float(r.cartera_bruta),
                    float(r.prevision_cartera),
                    float(r.coeficiente_solvencia),
                    float(r.peso_cartera_activo),
                    float(r.ratio_mora),
                    float(r.ratio_cobertura),
                    float(r.apalancamiento_veces),
                    r.source_file_sha256,
                    r.transformed_at.isoformat(),
                    r.gestion,
                    r.mes,
                )
                for r in clean_records
            ]
            placeholders = ",".join(["?"] * 16)
            con.executemany(f"INSERT INTO buffer VALUES ({placeholders})", rows)
            posix_mart = mart_dir.resolve().as_posix()
            con.execute(
                f"""
                COPY buffer TO '{posix_mart}'
                (FORMAT PARQUET, PARTITION_BY (gestion, mes), OVERWRITE_OR_IGNORE 1)
                """
            )
        finally:
            con.close()

        self._register_partitions_lineage(
            datamart_name="mart_indicadores_asfi",
            source_id="asfi",
            mart_dir=mart_dir,
            records=clean_records,
            external_run_id=external_run_id,
            bronze_path_map=bronze_path_map,
        )
        return mart_dir

    def consolidate_all_datamarts(
        self,
        artifacts: list[BronzeArtifact],
        *,
        target_datamart: str | None = None,
        external_run_id: str = "RUN_CONSOLIDACION_GOLD",
        url_inventory_repo: Any = None,
    ) -> dict[str, Any]:
        """Consolida uno o los 4 repositorios Gold a partir del lago documental Bronze."""
        silver_pipeline = SilverTransformPipeline()
        bcb_ingestor = BcbWorkbookIngestor(min_gestion=2020)
        bbv_ingestor = BbvFinancialIngestor()
        asfi_ingestor = AsfiPrudentialIngestor()

        bronze_path_map = {
            a.sha256_hash: str(a.file_path) for a in artifacts if a.sha256_hash
        }

        target = target_datamart.strip().lower() if target_datamart else None
        datamart_stats: dict[str, dict[str, Any]] = {}
        total_silver_records = 0
        total_gold_records = 0

        from prospector_interno.application.pipelines.multi_source_consolidator import (
            consolidate_multi_source_repositories,
        )

        multi_stats = consolidate_multi_source_repositories(
            base_gold_path=self._base_path,
            artifacts=artifacts,
            target_datamart=target,
            lineage_tracker=self._lineage_tracker,
            external_run_id=external_run_id,
        )
        for dm_name, dm_info in multi_stats.items():
            datamart_stats[dm_name] = dm_info
            total_gold_records += int(dm_info.get("records") or 0)

        cartera_records: list[GoldMartCarteraRecord] = []
        if target in ("mart_cartera", "mart_indicadores_asfi"):
            finrural_arts = [
                a for a in artifacts if "finrural" in a.domain.lower()
            ]
            silver_records = silver_pipeline.process_artifacts(finrural_arts)
            total_silver_records += len(silver_records)
            cartera_records = self.transform_to_mart_cartera(silver_records)

        if target == "mart_cartera":
            m_path = self.materialize_mart_cartera(
                cartera_records,
                external_run_id=external_run_id,
                bronze_path_map=bronze_path_map,
            )
            shas = {r.source_file_sha256 for r in cartera_records}
            if url_inventory_repo and shas:
                try:
                    url_inventory_repo.mark_processed_by_sha256(
                        shas, "mart_cartera", len(cartera_records)
                    )
                except Exception:
                    pass
            datamart_stats["mart_cartera"] = {
                "records": len(cartera_records),
                "source_files": len(shas),
                "path": str(m_path),
            }
            total_gold_records += len(cartera_records)

        if target == "mart_tasas_bcb":
            bcb_arts = sorted(
                [
                    a for a in artifacts
                    if "bcb" in a.domain.lower() or "bcb" in a.dataset_code.lower()
                ],
                key=lambda x: x.downloaded_at,
            )
            bcb_records_by_ym: dict[tuple[int, int], GoldMartTasasBcbRecord] = {}
            if len(bcb_arts) > 1:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    parsed_batches = list(pool.map(bcb_ingestor.parse_artifact, bcb_arts))
            else:
                parsed_batches = [bcb_ingestor.parse_artifact(art) for art in bcb_arts]
            for batch in parsed_batches:
                for rec in batch:
                    bcb_records_by_ym[(rec.gestion, rec.mes)] = rec
            bcb_records = list(bcb_records_by_ym.values())
            m_path = self.materialize_mart_tasas_bcb(
                bcb_records,
                external_run_id=external_run_id,
                bronze_path_map=bronze_path_map,
            )
            shas = {r.source_file_sha256 for r in bcb_records}
            if url_inventory_repo and shas:
                try:
                    url_inventory_repo.mark_processed_by_sha256(
                        shas, "mart_tasas_bcb", len(bcb_records)
                    )
                except Exception:
                    pass
            datamart_stats["mart_tasas_bcb"] = {
                "records": len(bcb_records),
                "source_files": len(shas),
                "path": str(m_path),
            }
            total_gold_records += len(bcb_records)

        if target == "mart_estadisticas_bbv":
            bbv_arts = sorted(
                [
                    a for a in artifacts
                    if "bbv" in a.domain.lower() or "bbv" in a.dataset_code.lower()
                ],
                key=lambda x: x.downloaded_at,
            )
            bbv_by_ym: dict[tuple[int, int, str], GoldMartEstadisticasBbvRecord] = {}
            if len(bbv_arts) > 1:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    parsed_batches = list(pool.map(bbv_ingestor.parse_artifact, bbv_arts))
            else:
                parsed_batches = [bbv_ingestor.parse_artifact(art) for art in bbv_arts]
            for batch in parsed_batches:
                for rec in batch:
                    bbv_by_ym[(rec.gestion, rec.mes, rec.institucion_id)] = rec
            bbv_records = list(bbv_by_ym.values())
            m_path = self.materialize_mart_estadisticas_bbv(
                bbv_records,
                external_run_id=external_run_id,
                bronze_path_map=bronze_path_map,
            )
            shas = {r.source_file_sha256 for r in bbv_records}
            if url_inventory_repo and shas:
                try:
                    url_inventory_repo.mark_processed_by_sha256(
                        shas, "mart_estadisticas_bbv", len(bbv_records)
                    )
                except Exception:
                    pass
            datamart_stats["mart_estadisticas_bbv"] = {
                "records": len(bbv_records),
                "source_files": len(shas),
                "path": str(m_path),
            }
            total_gold_records += len(bbv_records)

        if target == "mart_indicadores_asfi":
            asfi_arts = [
                a for a in artifacts
                if "asfi" in a.domain.lower() or "asfi" in a.dataset_code.lower()
            ]
            asfi_records: list[GoldMartIndicadoresAsfiRecord] = []
            for art in asfi_arts:
                asfi_records.extend(asfi_ingestor.parse_artifact(art))
            if cartera_records:
                asfi_records.extend(
                    asfi_ingestor.derive_from_supervised_entities(cartera_records)
                )
            m_path = self.materialize_mart_indicadores_asfi(
                asfi_records,
                external_run_id=external_run_id,
                bronze_path_map=bronze_path_map,
            )
            shas = {r.source_file_sha256 for r in asfi_records}
            if url_inventory_repo and shas:
                try:
                    url_inventory_repo.mark_processed_by_sha256(
                        shas, "mart_indicadores_asfi", len(asfi_records)
                    )
                except Exception:
                    pass
            datamart_stats["mart_indicadores_asfi"] = {
                "records": len(asfi_records),
                "source_files": len(shas),
                "path": str(m_path),
            }
            total_gold_records += len(asfi_records)

        primary_path = (
            next(iter(datamart_stats.values()))["path"]
            if len(datamart_stats) == 1
            else str(self._base_path)
        )

        return {
            "status": "COMPLETED",
            "total_bronze_artifacts": len(artifacts),
            "total_silver_records": total_silver_records,
            "total_gold_records": total_gold_records,
            "mart_path": primary_path,
            "datamarts": datamart_stats,
        }
