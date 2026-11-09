"""Pipeline de procesamiento y normalización hacia Capa Silver (HUI-12)."""

from pathlib import Path
from prospector_interno.application.pipelines.finrural_parser import (
    FinruralPdfParser,
)
from prospector_interno.domain.contracts.silver_spec import (
    SilverFinancialRecord,
)
from prospector_interno.domain.models import BronzeArtifact


class SilverTransformPipeline:
    """Pipeline orquestador para procesar artefactos de Bronze a Silver."""

    _CACHE: dict[tuple[str, str, int], list[SilverFinancialRecord]] = {}

    def __init__(self) -> None:
        self._finrural_parser = FinruralPdfParser()

    def process_artifact(
        self,
        artifact: BronzeArtifact,
    ) -> list[SilverFinancialRecord]:
        """Extrae y normaliza los registros tabulares de un artefacto Bronze."""
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

        # Enrutamiento según el dominio y tipo de documento
        if "finrural" in artifact.domain:
            res = self._finrural_parser.parse_cuadro_patrimonial(
                pdf_path=path,
                source_file_sha256=artifact.sha256_hash,
            )
            self._CACHE[cache_key] = res
            return list(res)

        return []

    def process_artifacts(
        self,
        artifacts: list[BronzeArtifact],
    ) -> list[SilverFinancialRecord]:
        """Procesa un lote de artefactos de Capa Bronze hacia Silver."""
        all_records: list[SilverFinancialRecord] = []
        for artifact in artifacts:
            try:
                records = self.process_artifact(artifact)
                all_records.extend(records)
            except Exception:
                # Continuar con el resto del lote ante fallos individuales
                continue
        return all_records
