"""Punto de entrada de la aplicación FastAPI (Documentación Swagger / OpenAPI).

Expone los contratos de integración para que el equipo de Portal Nexus pueda
consultar el catálogo activo, evaluar pre-vuelo e interrogar mapas externos.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from prospector_interno.adapters.api.routers import (
    catalog_router,
    gatekeeper_router,
    reconciliation_router,
)

app = FastAPI(
    title="DATAX Prospector Interno & Portal Nexus API",
    description=(
        "**API Oficial del Prospector Interno (DATAX Equipo 2)**\n\n"
        "Provee servicios de alto rendimiento para el **Portal Nexus**:\n"
        "- **Catálogo Activo:** Búsqueda <5ms en PostgreSQL MV.\n"
        "- **Pre-flight Gatekeeper:** Deduplicación y prevención de redundancia.\n"
        "- **DuckDB:** Interrogación vectorizada sobre los 3 JSONs externos."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "1. Catálogo Portal Nexus",
            "description": "Consultas a la Vista Materializada de PostgreSQL.",
        },
        {
            "name": "2. Pre-flight Gatekeeper",
            "description": "Detección de cache hit en Bronze/Gold y locks.",
        },
        {
            "name": "3. Conciliación & DuckDB Diff",
            "description": "Cruce y validación de URLs contra los 3 JSONs externos.",
        },
        {
            "name": "4. Sistema & Salud",
            "description": "Monitoreo de estado operativo del servicio.",
        },
    ],
)

# Configuración CORS abierta para integración con Portal Nexus
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registro de routers modulares
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(gatekeeper_router, prefix="/api/v1")
app.include_router(reconciliation_router, prefix="/api/v1")


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    """Redirige automáticamente la raíz hacia la documentación Swagger UI."""
    return RedirectResponse(url="/docs")


@app.get(
    "/health",
    tags=["4. Sistema & Salud"],
    summary="Verificación de salud del servicio (Liveness probe)",
)
def health_check() -> dict[str, str]:
    """Retorna el estado operativo del servicio."""
    return {
        "status": "healthy",
        "service": "prospector-interno",
        "version": "1.0.0",
    }
