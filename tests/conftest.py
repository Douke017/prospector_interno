"""Configuración común y fixtures para las pruebas de Prospector Interno."""

import csv
import json
from pathlib import Path
import pytest

from prospector_interno.adapters.api import dependencies as deps
from prospector_interno.adapters.api.routers.catalog_router import (
    set_lineage_tracker,
)
from prospector_interno.adapters.api.routers.downloads_router import (
    set_download_tracking,
)
from prospector_interno.adapters.api.routers.gatekeeper_router import (
    set_derivation_filter,
    set_download_state_filter,
    set_gatekeeper,
)
from prospector_interno.adapters.memory_state import (
    InMemoryCatalogRepository,
    InMemoryGatekeeperState,
)
from prospector_interno.adapters.storage.local_bronze import LocalBronzeStorage
from prospector_interno.application.gatekeeper import PreFlightGatekeeper
from prospector_interno.domain.models import IngestionRequest


@pytest.fixture(autouse=True)
def restore_router_singletons():
    """Garantiza aislamiento entre suites restaurando los singletons al finalizar cada test."""
    yield
    set_download_tracking(deps.download_tracking)
    set_download_state_filter(deps.download_state_filter)
    set_derivation_filter(deps.derivation_filter)
    set_gatekeeper(deps.gatekeeper)
    set_lineage_tracker(deps.lineage_tracker)


def build_bbv_map_fixture(tmp_path: Path, count: int = 235) -> Path:
    """Genera un mapa canónico BBV en tmp_path cuando el archivo runtime no está presente."""
    candidates = [
        Path("../prospector_externo/output/bbv/mapa_bbv.json"),
        Path("prospector_externo/output/bbv/mapa_bbv.json"),
    ]
    for cand in candidates:
        if cand.is_file():
            return cand

    out = tmp_path / "mapa_bbv.json"
    resources = [
        {
            "resource_key": f"bbv_fixture_res_{idx:03d}",
            "url": f"https://www.bbv.com.bo/fixture/estados_financieros_{idx:03d}.pdf",
            "title": f"Balance General BBV {idx:03d}",
            "file_extension": ".pdf",
            "content_type": "application/pdf",
            "content_length_bytes": 48120 + idx,
            "period_label": f"2025-{(idx % 12) + 1:02d}",
            "change_status": "UNCHANGED",
        }
        for idx in range(1, count + 1)
    ]
    payload = {
        "source": {"id": "bbv"},
        "run": {"run_id": "run_bbv_fixture_001"},
        "datasets": [{"resources": resources}],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return out


def build_finrural_map_fixture(tmp_path: Path, count: int = 130) -> Path:
    """Genera un mapa canónico FINRURAL en tmp_path cuando el archivo runtime no está presente."""
    candidates = [
        Path("../prospector_externo/output/finrural/mapa_finrural.json"),
        Path("prospector_externo/output/finrural/mapa_finrural.json"),
    ]
    for cand in candidates:
        if cand.is_file():
            return cand

    out = tmp_path / "mapa_finrural.json"
    resources = [
        {
            "resource_key": f"finrural_fixture_res_{idx:03d}",
            "url": f"https://www.finrural.org.bo/fixture/boletin_ifd_{idx:03d}.pdf",
            "title": f"Boletín Financiero FINRURAL {idx:03d}",
            "file_extension": ".pdf",
            "content_type": "application/pdf",
            "content_length_bytes": 112000 + idx,
            "period_label": "2026-01" if idx == 1 else f"2030-{(idx % 12) + 1:02d}",
            "change_status": "UNCHANGED",
        }
        for idx in range(1, count + 1)
    ]
    payload = {
        "source": {"id": "finrural"},
        "run": {"run_id": "run_finrural_fixture_001"},
        "datasets": [{"resources": resources}],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return out


def build_bcb_canonical_csv_fixture(tmp_path: Path, count: int = 164) -> Path:
    """Genera el CSV canónico BCB en tmp_path si el archivo externo no existe."""
    candidates = [
        Path("../archivos_prospector_interno/bcb_mapa_interno_canonico_v1.csv"),
        Path("archivos_prospector_interno/bcb_mapa_interno_canonico_v1.csv"),
        Path("/home/douke017/Datax/archivos_prospector_interno/bcb_mapa_interno_canonico_v1.csv"),
    ]
    for cand in candidates:
        if cand.is_file():
            return cand

    out = tmp_path / "bcb_mapa_interno_canonico_v1.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "report_code",
                "file_code",
                "main_url",
                "file_name",
                "file_updated_to",
                "relacion_fabrica_vieja",
            ]
        )
        for idx in range(1, count + 1):
            writer.writerow(
                [
                    f"R_BCB_{idx:03d}",
                    f"D_BO_BCB_{idx:03d}",
                    f"https://www.bcb.gob.bo/webdocs/estadisticas/cuadro_{idx:03d}.xlsx",
                    f"Cuadro Estadístico Monetario {idx:03d}",
                    f"2025-{(idx % 12) + 1:02d}",
                    "ANTECEDENTE_VIEJO_IDENTIFICADO" if idx % 2 == 0 else "NUEVO",
                ]
            )
    return out


def build_bcb_projection_fixture(tmp_path: Path, count: int = 527) -> Path:
    """Genera datax_projection.json de BCB en tmp_path si el archivo runtime no existe."""
    candidates = [
        Path("mapas_externo_last/bcb/datax_projection.json"),
        Path("../mapas_externo_last/bcb/datax_projection.json"),
    ]
    for cand in candidates:
        if cand.is_file():
            return cand

    out_dir = tmp_path / "mapas_externo_last" / "bcb"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "datax_projection.json"
    representations = [
        {
            "resource_key": f"bcb_proj_res_{idx:03d}",
            "url": f"https://www.bcb.gob.bo/webdocs/proyeccion/serie_{idx:03d}.xlsx",
            "format": "xlsx",
            "title": f"Estadística Semanal BCB {idx:03d}",
            "period_label": f"2026-{(idx % 12) + 1:02d}",
            "change_status": "UNCHANGED",
        }
        for idx in range(1, count + 1)
    ]
    payload = {
        "source_id": "bcb",
        "run_id": "run_20260919_052909_fa1124",
        "families": [
            {
                "family_id": "fam_bcb_semanal",
                "periods": [
                    {
                        "period": "2026-09",
                        "representations": representations,
                    }
                ],
            }
        ],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return out


@pytest.fixture
def bbv_map_path(tmp_path: Path) -> Path:
    """Ruta resuelta o fixture determinista para mapa_bbv.json."""
    return build_bbv_map_fixture(tmp_path)


@pytest.fixture
def finrural_map_path(tmp_path: Path) -> Path:
    """Ruta resuelta o fixture determinista para mapa_finrural.json."""
    return build_finrural_map_fixture(tmp_path)


@pytest.fixture
def bcb_canonical_csv_path(tmp_path: Path) -> Path:
    """Ruta resuelta o fixture determinista para bcb_mapa_interno_canonico_v1.csv."""
    return build_bcb_canonical_csv_fixture(tmp_path)


@pytest.fixture
def bcb_projection_path(tmp_path: Path) -> Path:
    """Ruta resuelta o fixture determinista para datax_projection.json."""
    return build_bcb_projection_fixture(tmp_path)


@pytest.fixture
def temp_bronze_dir(tmp_path: Path) -> Path:
    """Provee un directorio temporal limpio para la Capa Bronze."""
    bronze_dir = tmp_path / "bronze_store"
    bronze_dir.mkdir()
    return bronze_dir


@pytest.fixture
def bronze_storage(temp_bronze_dir: Path) -> LocalBronzeStorage:
    """Instancia configurada de almacenamiento Bronze local."""
    return LocalBronzeStorage(base_path=temp_bronze_dir)


@pytest.fixture
def state_registry() -> InMemoryGatekeeperState:
    """Registro en memoria de locks y tareas en vuelo."""
    return InMemoryGatekeeperState()


@pytest.fixture
def catalog_repo() -> InMemoryCatalogRepository:
    """Catálogo canónico en memoria."""
    return InMemoryCatalogRepository()


@pytest.fixture
def gatekeeper(
    bronze_storage: LocalBronzeStorage,
    state_registry: InMemoryGatekeeperState,
) -> PreFlightGatekeeper:
    """Instancia del Pre-flight Gatekeeper lista para pruebas."""
    return PreFlightGatekeeper(
        storage=bronze_storage,
        state_registry=state_registry,
    )


@pytest.fixture
def sample_request() -> IngestionRequest:
    """Solicitud de ingesta canónica de prueba."""
    return IngestionRequest(
        request_id="req-test-001",
        dataset_code="DS_BO_BCB_INFLATION",
        domain="bcb.gob.bo",
        target_url="https://www.bcb.gob.bo/datos/inflacion.xlsx",
        period="2026-08",
        force_refresh=False,
    )
