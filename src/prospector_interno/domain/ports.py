"""Puertos del dominio (Interfaces abstractas) para Arquitectura Hexagonal."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from prospector_interno.domain.contracts.lineage_spec import (
    DataLineageRecord,
    InternalAssetRecord,
)
from prospector_interno.domain.models import (
    BronzeArtifact,
    DatasetSpecification,
    DiscoveredResourceItem,
    ReconciliationDiffReport,
    SourceDefinition,
    SyncPlan,
)


class BronzeStoragePort(ABC):

    """Puerto para el almacenamiento inmutable en Capa Bronze."""

    @abstractmethod
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
        """Escribe un stream en un archivo temporal y lo renombra atómicamente."""

    @abstractmethod
    def find_by_checksum(self, checksum: str) -> BronzeArtifact | None:
        """Busca si un artefacto idéntico ya existe por su hash SHA-256."""

    @abstractmethod
    def find_by_period(
        self,
        domain: str,
        dataset_code: str,
        period: str,
    ) -> BronzeArtifact | None:
        """Busca si ya existe una descarga previa válida para el periodo."""

    @abstractmethod
    def find_by_url(self, url: str) -> BronzeArtifact | None:
        """Busca si un artefacto ya existe por su URL original."""

    @abstractmethod
    def find_by_resource_key(self, resource_key: str) -> BronzeArtifact | None:
        """Busca si un artefacto ya existe por su clave de recurso."""


class GatekeeperStatePort(ABC):
    """Puerto para la gestión de locks en vuelo y caché de deduplicación."""

    @abstractmethod
    def try_acquire_lock(
        self,
        idempotency_key: str,
        task_id: str,
        ttl_seconds: int = 300,
    ) -> bool:
        """Intenta adquirir el lock para una tarea en vuelo de forma atómica."""

    @abstractmethod
    def release_lock(self, idempotency_key: str, task_id: str) -> None:
        """Libera el lock de una tarea una vez concluida."""

    @abstractmethod
    def get_in_flight_task(self, idempotency_key: str) -> str | None:
        """Obtiene el task_id actualmente procesando la clave si existe."""


class CatalogRepositoryPort(ABC):
    """Puerto para el catálogo de fuentes y especificaciones de datasets."""

    @abstractmethod
    def get_source_by_id(self, source_id: int) -> SourceDefinition | None:
        """Recupera una fuente emisora por su identificador único."""

    @abstractmethod
    def get_source_by_code(self, canonical_code: str) -> SourceDefinition | None:
        """Recupera una fuente por su código canónico (ej. 'BO_BCB')."""

    @abstractmethod
    def list_sources(self) -> list[SourceDefinition]:
        """Lista todas las fuentes registradas en el catálogo."""

    @abstractmethod
    def save_source(self, source: SourceDefinition) -> None:
        """Registra o actualiza una fuente en el catálogo."""

    @abstractmethod
    def get_dataset(self, dataset_code: str) -> DatasetSpecification | None:
        """Recupera la especificación técnica de un dataset."""

    @abstractmethod
    def save_dataset(self, dataset: DatasetSpecification) -> None:
        """Registra o actualiza la especificación de un dataset."""


class RateLimiterPort(ABC):
    """Puerto para el control de frecuencia y evitación de bloqueos HTTP."""

    @abstractmethod
    def acquire_delay(self, domain: str) -> float:
        """Calcula y consume un ticket, retornando la pausa obligatoria."""


class HttpDownloaderPort(ABC):
    """Puerto para clientes de descarga HTTP resilientes y streaming."""

    @abstractmethod
    def download_to_stream(
        self,
        url: str,
        domain: str,
    ) -> tuple[BinaryIO, int, str | None]:
        """Descarga una URL por streaming y retorna el buffer con metadatos."""


class ExternalCatalogClientPort(ABC):
    """Puerto para el cliente de consulta REST hacia el Prospector Externo."""

    @abstractmethod
    def fetch_resources(
        self,
        source_id: str,
        base_url: str | None = None,
    ) -> list[DiscoveredResourceItem]:
        """Obtiene la lista de recursos descubiertos para una fuente vía HTTP REST."""

    @abstractmethod
    def get_latest_run_id(
        self,
        source_id: str | None = None,
        base_url: str | None = None,
    ) -> str | None:
        """Consulta el identificador de la última corrida exitosa."""

    @abstractmethod
    def check_health(
        self,
        base_url: str | None = None,
    ) -> bool:
        """Verifica la conectividad y disponibilidad de la API externa."""


class ReconciliationEnginePort(ABC):
    """Puerto para el motor de diff y conciliación con el Prospector Externo."""

    @abstractmethod
    def compute_diff(
        self,
        source_id: str,
        external_map_path: Path | None = None,
        external_api_url: str | None = None,
    ) -> ReconciliationDiffReport:
        """Compara inventario externo contra almacenamiento local con DuckDB."""


class SyncOrchestratorPort(ABC):
    """Puerto para la orquestación y ejecución de planes de sincronización."""

    @abstractmethod
    def create_plan(
        self,
        diff_report: ReconciliationDiffReport,
    ) -> SyncPlan:
        """Construye un SyncPlan priorizado a partir del reporte de diff."""

    @abstractmethod
    def execute_plan(self, plan: SyncPlan) -> SyncPlan:
        """Ejecuta las descargas del plan depositándolas en Capa Bronze."""


class CircuitBreakerPort(ABC):
    """Puerto para el disyuntor adaptativo por dominio (HUI-09)."""

    @abstractmethod
    def can_execute(self, domain: str) -> bool:
        """Determina si un dominio puede admitir nuevas peticiones HTTP."""

    @abstractmethod
    def record_success(self, domain: str) -> None:
        """Registra una respuesta exitosa cerrando el circuito."""

    @abstractmethod
    def record_failure(
        self,
        domain: str,
        status_code: int,
        retry_after_seconds: float | None = None,
    ) -> None:
        """Registra un fallo abriendo el circuito e iniciando enfriamiento."""

    @abstractmethod
    def get_status(self, domain: str):
        """Devuelve los metadatos del circuito para el dominio."""

    @abstractmethod
    def reset(self, domain: str | None = None) -> None:
        """Restablece el estado del circuito a CLOSED."""


class ResumableDownloaderPort(ABC):
    """Puerto para descargas streaming reanudables con checksum en vuelo (HUI-08)."""

    @abstractmethod
    def download_file(
        self,
        url: str,
        domain: str,
        destination_path: Path,
        expected_checksum: str | None = None,
    ):
        """Descarga un archivo en bloques reanudables y valida su hash SHA-256."""


class LineageRepositoryPort(ABC):
    """Puerto para el registro y consulta inmutable de activos y linaje (HUI-17)."""

    @abstractmethod
    def record_asset(self, asset: InternalAssetRecord) -> None:
        """Registra o actualiza un activo de datos en el inventario."""

    @abstractmethod
    def get_asset(self, asset_id: str) -> InternalAssetRecord | None:
        """Obtiene un activo de datos por su identificador único."""

    @abstractmethod
    def list_assets(
        self,
        storage_layer: str | None = None,
        source_id: str | None = None,
    ) -> list[InternalAssetRecord]:
        """Lista activos catalogados con filtros opcionales."""

    @abstractmethod
    def record_lineage(self, lineage: DataLineageRecord) -> None:
        """Inserta un enlace inmutable de linaje entre Gold y Bronze."""

    @abstractmethod
    def get_lineage_by_asset(
        self,
        gold_asset_id: str,
    ) -> list[DataLineageRecord]:
        """Recupera todos los registros de linaje de un activo Gold."""

    @abstractmethod
    def get_lineage_by_checksum(
        self,
        bronze_file_sha256: str,
    ) -> list[DataLineageRecord]:
        """Recupera los vínculos de linaje asociados a un archivo Bronze por SHA-256."""
