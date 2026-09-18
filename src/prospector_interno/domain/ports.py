"""Puertos del dominio (Interfaces abstractas) para Arquitectura Hexagonal."""

from abc import ABC, abstractmethod
from typing import BinaryIO

from prospector_interno.domain.models import (
    BronzeArtifact,
    DatasetSpecification,
    SourceDefinition,
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
