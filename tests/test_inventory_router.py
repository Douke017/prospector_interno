"""Pruebas unitarias para el router de inventario y curaduría de URLs (HUI-03, HUI-15)."""

from fastapi.testclient import TestClient
import pytest

pytestmark = pytest.mark.legacy_pg

from prospector_interno.adapters.api.main import app

client = TestClient(app)


def test_get_inventory_stats() -> None:
    """Verifica que el endpoint de estadísticas de inventario devuelva métricas válidas."""
    response = client.get("/api/v1/inventory/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total_urls" in data
    assert "total_downloaded" in data
    assert "total_processed" in data
    assert "download_rate_pct" in data
    assert "processing_rate_pct" in data
    assert isinstance(data["by_source"], list)


def test_list_inventory_urls() -> None:
    """Verifica listado paginado y filtros de inventario de URLs."""
    response = client.get("/api/v1/inventory/urls?page=1&limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert data["page"] == 1
    assert data["limit"] == 10
    assert len(data["items"]) <= 10
    if data["items"]:
        item = data["items"][0]
        assert "id" in item
        assert "canonical_url" in item
        assert "normalized_url_sha256" in item


def test_patch_inventory_url_toggles() -> None:
    """Verifica la conmutación independiente de is_downloaded e is_processed."""
    # Obtener el primer elemento
    res_list = client.get("/api/v1/inventory/urls?limit=1")
    assert res_list.status_code == 200
    items = res_list.json()["items"]
    if not items:
        pytest.skip("No hay items en el inventario para probar PATCH")

    target_id = items[0]["id"]
    initial_downloaded = items[0]["is_downloaded"]
    initial_processed = items[0]["is_processed"]

    # 1. Conmutar is_processed
    new_proc = not initial_processed
    patch_proc = client.patch(
        f"/api/v1/inventory/urls/{target_id}",
        json={"is_processed": new_proc},
    )
    assert patch_proc.status_code == 200
    assert patch_proc.json()["is_processed"] == new_proc

    # 2. Conmutar is_downloaded
    new_dl = not initial_downloaded
    patch_dl = client.patch(
        f"/api/v1/inventory/urls/{target_id}",
        json={"is_downloaded": new_dl},
    )
    assert patch_dl.status_code == 200
    assert patch_dl.json()["is_downloaded"] == new_dl

    # 3. Payload vacío debe retornar 422
    patch_empty = client.patch(
        f"/api/v1/inventory/urls/{target_id}",
        json={},
    )
    assert patch_empty.status_code == 422

    # 4. Restaurar estado original
    client.patch(
        f"/api/v1/inventory/urls/{target_id}",
        json={"is_processed": initial_processed, "is_downloaded": initial_downloaded},
    )


def test_patch_inventory_url_not_found() -> None:
    """Verifica 404 para ID inexistente."""
    response = client.patch(
        "/api/v1/inventory/urls/99999999",
        json={"is_processed": True},
    )
    assert response.status_code == 404
