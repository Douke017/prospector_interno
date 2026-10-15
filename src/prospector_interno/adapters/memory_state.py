"""Adaptadores en memoria para estado del Gatekeeper y Catálogo."""

import threading
import time

from prospector_interno.domain.models import (
    DatasetSpecification,
    SourceDefinition,
)
from prospector_interno.domain.ports import (
    CatalogRepositoryPort,
    GatekeeperStatePort,
)


class InMemoryGatekeeperState(GatekeeperStatePort):
    """Gestor de locks en vuelo atómico y thread-safe."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # {idempotency_key: (task_id, expiry_timestamp)}
        self._locks: dict[str, tuple[str, float]] = {}

    def try_acquire_lock(
        self,
        idempotency_key: str,
        task_id: str,
        ttl_seconds: int = 300,
    ) -> bool:
        """Intenta adquirir el lock atómicamente."""
        now = time.monotonic()
        with self._lock:
            # Limpiar si ya expiró el TTL
            if idempotency_key in self._locks:
                existing_id, expiry = self._locks[idempotency_key]
                if now < expiry:
                    # Aún activo por otro worker
                    if existing_id == task_id:
                        return True
                    return False

            self._locks[idempotency_key] = (task_id, now + ttl_seconds)
            return True

    def release_lock(self, idempotency_key: str, task_id: str) -> None:
        """Libera el lock si coincide con el task_id poseedor."""
        with self._lock:
            if idempotency_key in self._locks:
                existing_id, _ = self._locks[idempotency_key]
                if existing_id == task_id:
                    del self._locks[idempotency_key]

    def get_in_flight_task(self, idempotency_key: str) -> str | None:
        """Retorna el ID de la tarea poseedora si el lock sigue vigente."""
        now = time.monotonic()
        with self._lock:
            if idempotency_key in self._locks:
                task_id, expiry = self._locks[idempotency_key]
                if now < expiry:
                    return task_id
                # Si expiró, se purga de forma perezosa
                del self._locks[idempotency_key]
            return None


class InMemoryCatalogRepository(CatalogRepositoryPort):
    """Catálogo canónico en memoria para fuentes y datasets de Portal Nexus."""

    def __init__(self) -> None:
        self._sources_by_id: dict[int, SourceDefinition] = {}
        self._sources_by_code: dict[str, SourceDefinition] = {}
        self._datasets_by_code: dict[str, DatasetSpecification] = {}

    def get_source_by_id(self, source_id: int) -> SourceDefinition | None:
        return self._sources_by_id.get(source_id)

    def get_source_by_code(self, canonical_code: str) -> SourceDefinition | None:
        return self._sources_by_code.get(canonical_code.upper())

    def list_sources(self) -> list[SourceDefinition]:
        return list(self._sources_by_id.values())

    def save_source(self, source: SourceDefinition) -> None:
        self._sources_by_id[source.source_id] = source
        self._sources_by_code[source.canonical_code.upper()] = source

    def get_dataset(self, dataset_code: str) -> DatasetSpecification | None:
        return self._datasets_by_code.get(dataset_code.upper())

    def save_dataset(self, dataset: DatasetSpecification) -> None:
        self._datasets_by_code[dataset.dataset_code.upper()] = dataset

    def list_datasets(
        self,
        source_id: int | None = None,
    ) -> list[DatasetSpecification]:
        if source_id is not None:
            return [
                ds for ds in self._datasets_by_code.values()
                if ds.source_id == source_id
            ]
        return list(self._datasets_by_code.values())

    def get_dataset_by_legacy_code(
        self,
        legacy_file_code: str,
    ) -> DatasetSpecification | None:
        return next(
            (
                ds for ds in self._datasets_by_code.values()
                if ds.legacy_file_code.upper() == legacy_file_code.upper()
            ),
            None,
        )
