"""Especificaciones componibles para el Inventario de Activos (HUI-15).

Implementa el Specification Pattern para permitir consultas fluidas y dinámicas
sobre el inventario de activos de datos (por institución, fechas, capa y volumen),
soportando evaluación en memoria y compilación optimizada a SQL para SQLite.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any

from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
    StorageLayer,
)


class BaseAssetSpecification(ABC):
    """Clase base abstracta para el patrón de especificación sobre activos."""

    @abstractmethod
    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        """Evalúa si un activo en memoria cumple la regla de la especificación."""

    @abstractmethod
    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        """Compila la especificación a una tupla (cláusula_sql, parámetros)."""

    def and_(self, other: BaseAssetSpecification) -> BaseAssetSpecification:
        """Combina lógicamente con otra especificación mediante AND."""
        return AndAssetSpecification(self, other)

    def or_(self, other: BaseAssetSpecification) -> BaseAssetSpecification:
        """Combina lógicamente con otra especificación mediante OR."""
        return OrAssetSpecification(self, other)

    def not_(self) -> BaseAssetSpecification:
        """Niega la especificación actual."""
        return NotAssetSpecification(self)

    def __and__(self, other: BaseAssetSpecification) -> BaseAssetSpecification:
        return self.and_(other)

    def __or__(self, other: BaseAssetSpecification) -> BaseAssetSpecification:
        return self.or_(other)

    def __invert__(self) -> BaseAssetSpecification:
        return self.not_()

    # Métodos fluidos de encadenamiento continuo
    def by_source(self, source_id: str) -> BaseAssetSpecification:
        """Encadena un filtro por identificador de fuente emisora."""
        return self.and_(SourceSpecification(source_id))

    def by_dataset(self, dataset_id: str) -> BaseAssetSpecification:
        """Encadena un filtro por identificador de dataset."""
        return self.and_(DatasetSpecification(dataset_id))

    def by_layer(self, layer: StorageLayer | str) -> BaseAssetSpecification:
        """Encadena un filtro por capa de almacenamiento Medallion."""
        return self.and_(LayerSpecification(layer))

    def in_range(self, start_date: str, end_date: str) -> BaseAssetSpecification:
        """Encadena un filtro por rango de fechas ISO YYYY-MM-DD."""
        return self.and_(PeriodRangeSpecification(start_date, end_date))

    def with_min_rows(self, min_rows: int) -> BaseAssetSpecification:
        """Encadena un filtro por cantidad mínima de filas tabulares."""
        return self.and_(MinRowsSpecification(min_rows))


class AllAssetsSpecification(BaseAssetSpecification):
    """Especificación neutra que admite todos los activos catalogados."""

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return True

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "1=1", []


class SourceSpecification(BaseAssetSpecification):
    """Filtra activos pertenecientes a una fuente emisora específica."""

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id.lower().strip()

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return asset.source_id.lower() == self.source_id

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "LOWER(source_id) = ?", [self.source_id]


class DatasetSpecification(BaseAssetSpecification):
    """Filtra activos por el código del conjunto de datos (ej. mart_cartera)."""

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id.lower().strip()

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return asset.dataset_id.lower() == self.dataset_id

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "LOWER(dataset_id) = ?", [self.dataset_id]


class LayerSpecification(BaseAssetSpecification):
    """Filtra activos por la capa Medallion (BRONZE, SILVER, GOLD)."""

    def __init__(self, layer: StorageLayer | str) -> None:
        if isinstance(layer, StorageLayer):
            self.layer = layer
        else:
            self.layer = StorageLayer(layer.upper())

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return asset.storage_layer == self.layer

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "storage_layer = ?", [self.layer.value]


class PeriodRangeSpecification(BaseAssetSpecification):
    """Filtra activos comprendidos dentro de un rango temporal (YYYY-MM-DD)."""

    def __init__(self, start_date: str, end_date: str) -> None:
        self.start_date = start_date.strip()
        self.end_date = end_date.strip()

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return (
            asset.period_start >= self.start_date
            and asset.period_end <= self.end_date
        )

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "period_start >= ? AND period_end <= ?", [
            self.start_date,
            self.end_date,
        ]


class MinRowsSpecification(BaseAssetSpecification):
    """Filtra activos que contengan al menos un número mínimo de filas."""

    def __init__(self, min_rows: int) -> None:
        self.min_rows = max(0, min_rows)

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        if asset.row_count is None:
            return False
        return asset.row_count >= self.min_rows

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        return "row_count >= ?", [self.min_rows]


class AndAssetSpecification(BaseAssetSpecification):
    """Operador de conjunción lógica (AND) entre dos especificaciones."""

    def __init__(
        self,
        left: BaseAssetSpecification,
        right: BaseAssetSpecification,
    ) -> None:
        self.left = left
        self.right = right

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return self.left.is_satisfied_by(asset) and self.right.is_satisfied_by(asset)

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        left_sql, left_params = self.left.to_sql_criteria()
        right_sql, right_params = self.right.to_sql_criteria()
        return f"({left_sql} AND {right_sql})", left_params + right_params


class OrAssetSpecification(BaseAssetSpecification):
    """Operador de disyunción lógica (OR) entre dos especificaciones."""

    def __init__(
        self,
        left: BaseAssetSpecification,
        right: BaseAssetSpecification,
    ) -> None:
        self.left = left
        self.right = right

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return self.left.is_satisfied_by(asset) or self.right.is_satisfied_by(asset)

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        left_sql, left_params = self.left.to_sql_criteria()
        right_sql, right_params = self.right.to_sql_criteria()
        return f"({left_sql} OR {right_sql})", left_params + right_params


class NotAssetSpecification(BaseAssetSpecification):
    """Operador de negación lógica (NOT) sobre una especificación."""

    def __init__(self, spec: BaseAssetSpecification) -> None:
        self.spec = spec

    def is_satisfied_by(self, asset: InternalAssetRecord) -> bool:
        return not self.spec.is_satisfied_by(asset)

    def to_sql_criteria(self) -> tuple[str, list[Any]]:
        inner_sql, inner_params = self.spec.to_sql_criteria()
        return f"(NOT ({inner_sql}))", inner_params


class AssetSpecification:
    """Fábrica de alto nivel para construcción fluida de especificaciones."""

    @staticmethod
    def all() -> BaseAssetSpecification:
        """Crea una especificación que selecciona todos los activos."""
        return AllAssetsSpecification()

    @staticmethod
    def by_source(source_id: str) -> BaseAssetSpecification:
        """Crea una especificación filtrada por código de fuente."""
        return SourceSpecification(source_id)

    @staticmethod
    def by_dataset(dataset_id: str) -> BaseAssetSpecification:
        """Crea una especificación filtrada por código de dataset."""
        return DatasetSpecification(dataset_id)

    @staticmethod
    def by_layer(layer: StorageLayer | str) -> BaseAssetSpecification:
        """Crea una especificación filtrada por capa de almacenamiento."""
        return LayerSpecification(layer)

    @staticmethod
    def in_range(start_date: str, end_date: str) -> BaseAssetSpecification:
        """Crea una especificación filtrada por rango de periodos."""
        return PeriodRangeSpecification(start_date, end_date)

    @staticmethod
    def with_min_rows(min_rows: int) -> BaseAssetSpecification:
        """Crea una especificación filtrada por mínimo de filas tabulares."""
        return MinRowsSpecification(min_rows)
