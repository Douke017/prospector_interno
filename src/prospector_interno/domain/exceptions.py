"""Jerarquía canónica de excepciones de dominio para Prospector Interno."""


class ProspectorError(Exception):
    """Excepción base para todos los errores del sistema Prospector Interno."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DomainError(ProspectorError):
    """Errores originados en las reglas invariantes de dominio."""


class EntityNotFoundError(DomainError):
    """Lanzada cuando una entidad requerida no existe en el catálogo."""


class InvalidStateTransitionError(DomainError):
    """Lanzada ante una transición de estado ilegal en el ciclo de vida."""


class GatekeeperError(ProspectorError):
    """Errores y señales operativas del Pre-flight Gatekeeper."""


class CacheHitSignal(GatekeeperError):
    """Señal emitida cuando el dato ya existe validado en Capa Bronze o Gold."""

    def __init__(self, message: str, artifact_path: str, checksum: str) -> None:
        super().__init__(message)
        self.artifact_path = artifact_path
        self.checksum = checksum


class InFlightCollisionError(GatekeeperError):
    """Emitida cuando una solicitud idéntica ya está siendo procesada."""

    def __init__(self, message: str, task_key: str, in_flight_id: str) -> None:
        super().__init__(message)
        self.task_key = task_key
        self.in_flight_id = in_flight_id


class StorageError(ProspectorError):
    """Errores en operaciones de I/O y persistencia."""


class AtomicRenameError(StorageError):
    """Fallo en la operación atómica de renombrado de archivo temporal."""


class ChecksumMismatchError(StorageError):
    """El checksum SHA-256 verificado en vuelo no coincide con el esperado."""

    def __init__(
        self,
        message: str,
        expected: str,
        calculated: str,
    ) -> None:
        super().__init__(message)
        self.expected = expected
        self.calculated = calculated


class SchedulerError(ProspectorError):
    """Errores en el despachador de tareas y control de tasa."""


class RateLimitExceededError(SchedulerError):
    """La tasa de peticiones para un dominio ha sobrepasado el umbral seguro."""


class LegacySeederError(ProspectorError):
    """Errores durante la extracción y normalización de la base legacy."""
