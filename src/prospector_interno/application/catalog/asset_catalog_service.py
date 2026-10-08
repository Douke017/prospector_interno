"""Servicio de Aplicación para el Inventario Técnico de Activos (HUI-15).

Coordina las búsquedas componibles bajo el Specification Pattern, consultas
por identificador único y resúmenes ejecutivos del inventario de datos.
"""

from typing import Any

from prospector_interno.domain.catalog.specifications import (
    AssetSpecification,
    BaseAssetSpecification,
)
from prospector_interno.domain.contracts.lineage_spec import (
    InternalAssetRecord,
)
from prospector_interno.domain.exceptions import EntityNotFoundError
from prospector_interno.domain.ports import LineageRepositoryPort


class AssetCatalogService:
    """Servicio de gestión y consulta del inventario técnico de activos."""

    def __init__(self, repository: LineageRepositoryPort) -> None:
        self._repository = repository

    def find_assets(
        self,
        spec: BaseAssetSpecification | None = None,
    ) -> list[InternalAssetRecord]:
        """Recupera activos aplicando los filtros de la especificación."""
        active_spec = spec or AssetSpecification.all()
        return self._repository.find_by_specification(active_spec)

    def get_asset_by_id(self, asset_id: str) -> InternalAssetRecord:
        """Obtiene un activo de datos o lanza EntityNotFoundError si no existe."""
        asset = self._repository.get_asset(asset_id)
        if asset is None:
            raise EntityNotFoundError(
                f"Activo '{asset_id}' no encontrado en el inventario técnico."
            )
        return asset

    def count_assets(

        self,
        spec: BaseAssetSpecification | None = None,
    ) -> int:
        """Cuenta la cantidad de activos que satisfacen una especificación."""
        return len(self.find_assets(spec))

    def get_inventory_summary(self) -> dict[str, Any]:
        """Calcula métricas consolidadas del inventario físico catalogado."""
        all_assets = self.find_assets(AssetSpecification.all())
        total_count = len(all_assets)
        total_size_bytes = sum(a.size_bytes for a in all_assets)
        total_rows = sum(a.row_count or 0 for a in all_assets)

        layers_distribution: dict[str, int] = {}
        sources_distribution: dict[str, int] = {}

        min_period: str | None = None
        max_period: str | None = None

        for asset in all_assets:
            layer = asset.storage_layer.value
            layers_distribution[layer] = layers_distribution.get(layer, 0) + 1

            source = asset.source_id
            sources_distribution[source] = sources_distribution.get(source, 0) + 1

            if min_period is None or asset.period_start < min_period:
                min_period = asset.period_start
            if max_period is None or asset.period_end > max_period:
                max_period = asset.period_end

        return {
            "total_assets": total_count,
            "total_size_bytes": total_size_bytes,
            "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
            "total_row_count": total_rows,
            "layers_distribution": layers_distribution,
            "sources_distribution": sources_distribution,
            "earliest_period": min_period,
            "latest_period": max_period,
        }
