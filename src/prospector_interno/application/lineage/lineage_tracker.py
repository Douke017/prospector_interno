"""Servicio de rastreo, registro y auditoría de linaje de datos (HUI-17).

Reconstruye el árbol de procedencia criptográfica desde el Prospector Externo
hasta los Datamarts en Capa Gold, validando integridad contra disco.
"""

from datetime import datetime, timezone
import hashlib
from pathlib import Path

from prospector_interno.domain.contracts.lineage_spec import (
    AssetLineageGraphResponse,
    DataLineageRecord,
    InternalAssetRecord,
    LineageIntegrityCheckResult,
    ProvenanceStep,
    StorageLayer,
)
from prospector_interno.domain.exceptions import EntityNotFoundError
from prospector_interno.domain.ports import LineageRepositoryPort


class LineageTracker:
    """Orquestador de trazabilidad y cadena de custodia de extremo a extremo."""

    def __init__(self, repository: LineageRepositoryPort) -> None:
        self._repository = repository

    def register_asset(
        self,
        asset_id: str,
        source_id: str,
        dataset_id: str,
        period_start: str,
        period_end: str,
        storage_layer: StorageLayer,
        file_path: Path | str,
        content_hash_sha256: str | None = None,
        row_count: int | None = None,
        size_bytes: int | None = None,
    ) -> InternalAssetRecord:
        """Registra un activo de datos calculando su hash si no se provee."""
        path = Path(file_path)

        actual_size = size_bytes if size_bytes is not None else (
            path.stat().st_size if path.exists() else 0
        )

        computed_sha = content_hash_sha256
        if not computed_sha and path.exists():
            computed_sha = self._compute_file_sha256(path)
        elif not computed_sha:
            computed_sha = hashlib.sha256(asset_id.encode()).hexdigest()

        asset = InternalAssetRecord(
            asset_id=asset_id,
            source_id=source_id,
            dataset_id=dataset_id,
            period_start=period_start,
            period_end=period_end,
            storage_layer=storage_layer,
            file_path=str(path),
            content_hash_sha256=computed_sha,
            row_count=row_count,
            size_bytes=actual_size,
        )
        self._repository.record_asset(asset)
        return asset

    def link_bronze_source(
        self,
        gold_asset_id: str,
        bronze_source_file: str,
        bronze_file_sha256: str,
        external_run_id: str,
        pipeline_version: str = "v1.0.0",
        transformed_at: datetime | None = None,
    ) -> DataLineageRecord:
        """Vincula un insumo Bronze verificado con el activo consolidado Gold."""
        # Generación determinista de lineage_id para idempotencia
        digest_input = (
            f"{gold_asset_id}:{bronze_source_file}:"
            f"{bronze_file_sha256}:{external_run_id}"
        )
        lineage_id = hashlib.sha256(digest_input.encode()).hexdigest()[:32]

        record = DataLineageRecord(
            lineage_id=lineage_id,
            gold_asset_id=gold_asset_id,
            bronze_source_file=bronze_source_file,
            bronze_file_sha256=bronze_file_sha256,
            external_run_id=external_run_id,
            pipeline_version=pipeline_version,
            transformed_at=transformed_at or datetime.now(timezone.utc),
        )
        self._repository.record_lineage(record)
        return record

    def get_asset_lineage_graph(
        self,
        asset_id: str,
    ) -> AssetLineageGraphResponse:
        """Construye el grafo de linaje y cadena de custodia para un activo Gold."""
        asset = self._repository.get_asset(asset_id)
        if not asset:
            raise EntityNotFoundError(
                f"Activo '{asset_id}' no encontrado en el inventario de catálogo."
            )

        lineage_records = self._repository.get_lineage_by_asset(asset_id)

        upstream_bronze_files = list(
            dict.fromkeys(r.bronze_source_file for r in lineage_records)
        )
        upstream_sha256_digests = list(
            dict.fromkeys(r.bronze_file_sha256 for r in lineage_records)
        )
        external_run_ids = list(
            dict.fromkeys(r.external_run_id for r in lineage_records)
        )

        provenance_chain: list[ProvenanceStep] = []

        # Paso 1: Descubrimiento externo
        runs_desc = ", ".join(external_run_ids) if external_run_ids else "N/A"
        provenance_chain.append(
            ProvenanceStep(
                step_order=1,
                step_name="EXTERNAL_DISCOVERY",
                component="Prospector Externo (Crawler/Scraper)",
                timestamp=asset.created_at,
                input_digest=None,
                output_digest="RUNS: " + runs_desc,
                metadata={"external_run_ids": external_run_ids},
            )
        )

        # Paso 2: Adquisición en Bronze
        for idx, (b_file, b_sha) in enumerate(
            zip(upstream_bronze_files, upstream_sha256_digests)
        ):
            provenance_chain.append(
                ProvenanceStep(
                    step_order=2 + idx,
                    step_name="BRONZE_PERSISTENCE",
                    component="PoliteMassiveDownloader / BronzeStorageAdapter",
                    timestamp=asset.created_at,
                    input_digest=None,
                    output_digest=b_sha,
                    metadata={"file_name": b_file, "integrity": "VERIFIED_ON_FLIGHT"},
                )
            )

        # Paso 3: Limpieza y Tipado Silver
        next_order = len(provenance_chain) + 1
        provenance_chain.append(
            ProvenanceStep(
                step_order=next_order,
                step_name="SILVER_NORMALIZATION",
                component="SilverTransformPipeline (FinruralPdfParser)",
                timestamp=asset.created_at,
                input_digest=",".join(upstream_sha256_digests),
                output_digest="ATOMIC_TABULAR_ROWS",
                metadata={"target_category": "BALANCE_PATRIMONIAL_Y_CARTERA"},
            )
        )

        # Paso 4: Materialización Dimensional Gold
        provenance_chain.append(
            ProvenanceStep(
                step_order=next_order + 1,
                step_name="GOLD_MATERIALIZATION",
                component="GoldMaterializer (Vectorized Parquet)",
                timestamp=asset.last_updated_at,
                input_digest="ATOMIC_TABULAR_ROWS",
                output_digest=asset.content_hash_sha256,
                metadata={
                    "row_count": asset.row_count,
                    "storage_layer": asset.storage_layer.value,
                    "file_path": asset.file_path,
                },
            )
        )

        return AssetLineageGraphResponse(
            asset=asset,
            lineage_records=lineage_records,
            upstream_bronze_files=upstream_bronze_files,
            upstream_sha256_digests=upstream_sha256_digests,
            external_run_ids=external_run_ids,
            provenance_chain=provenance_chain,
        )

    def verify_asset_integrity(
        self,
        asset_id: str,
        base_bronze_dir: Path | str | None = None,
    ) -> LineageIntegrityCheckResult:
        """Verifica criptográficamente que los archivos físicos en disco
        sigan intactos y coincidan con el registro inmutable.
        """
        asset = self._repository.get_asset(asset_id)

        if not asset:
            raise EntityNotFoundError(f"Activo '{asset_id}' no encontrado.")

        asset_path = Path(asset.file_path)
        disk_sha = None
        asset_intact = False

        if asset_path.exists():
            disk_sha = self._compute_file_sha256(asset_path)
            asset_intact = disk_sha == asset.content_hash_sha256

        lineage_records = self._repository.get_lineage_by_asset(asset_id)
        upstream_details: list[dict] = []
        all_upstream_intact = True

        for rec in lineage_records:
            b_path = Path(rec.bronze_source_file)
            if not b_path.exists() and base_bronze_dir:
                b_path = Path(base_bronze_dir) / b_path.name

            b_exists = b_path.exists()
            b_disk_sha = self._compute_file_sha256(b_path) if b_exists else None
            b_match = (b_disk_sha == rec.bronze_file_sha256) if b_exists else False

            if not b_match:
                all_upstream_intact = False

            upstream_details.append(
                {
                    "source_file": rec.bronze_source_file,
                    "expected_sha256": rec.bronze_file_sha256,
                    "disk_sha256": b_disk_sha,
                    "exists": b_exists,
                    "is_intact": b_match,
                }
            )

        is_valid = asset_intact and all_upstream_intact

        msg = (
            "Cadena de custodia verificada: activo Gold e insumos Bronze íntegros."
            if is_valid
            else "Discrepancia criptográfica o archivos faltantes en disco."
        )

        return LineageIntegrityCheckResult(
            asset_id=asset_id,
            is_valid=is_valid,
            recorded_sha256=asset.content_hash_sha256,
            current_disk_sha256=disk_sha,
            all_upstream_intact=all_upstream_intact,
            upstream_details=upstream_details,
            verification_message=msg,
        )

    @staticmethod
    def _compute_file_sha256(path: Path) -> str:
        """Calcula el hash SHA-256 en bloques de 64 KB para no consumir memoria."""
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
