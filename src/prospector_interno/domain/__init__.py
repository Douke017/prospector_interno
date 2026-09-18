"""Submódulo de Dominio canónico de Prospector Interno."""

from prospector_interno.domain.exceptions import (
    AtomicRenameError,
    CacheHitSignal,
    ChecksumMismatchError,
    DomainError,
    EntityNotFoundError,
    GatekeeperError,
    InFlightCollisionError,
    InvalidStateTransitionError,
    LegacySeederError,
    ProspectorError,
    RateLimitExceededError,
    SchedulerError,
    StorageError,
)
from prospector_interno.domain.models import (
    BronzeArtifact,
    DatasetSpecification,
    IngestionRequest,
    Periodicity,
    PreFlightEvaluation,
    PreFlightVerdict,
    SourceDefinition,
)
from prospector_interno.domain.ports import (
    BronzeStoragePort,
    CatalogRepositoryPort,
    GatekeeperStatePort,
    RateLimiterPort,
)

__all__ = [
    "AtomicRenameError",
    "BronzeArtifact",
    "BronzeStoragePort",
    "CacheHitSignal",
    "CatalogRepositoryPort",
    "ChecksumMismatchError",
    "DatasetSpecification",
    "DomainError",
    "EntityNotFoundError",
    "GatekeeperError",
    "GatekeeperStatePort",
    "InFlightCollisionError",
    "IngestionRequest",
    "InvalidStateTransitionError",
    "LegacySeederError",
    "Periodicity",
    "PreFlightEvaluation",
    "PreFlightVerdict",
    "ProspectorError",
    "RateLimitExceededError",
    "RateLimiterPort",
    "SchedulerError",
    "SourceDefinition",
    "StorageError",
]
