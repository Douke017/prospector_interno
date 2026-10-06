# Prospector Interno (DATAX Equipo 2) — Target: Portal Nexus

Plataforma de adquisición masiva de datos económicos, ingesta resiliente, conciliación federada, repositorio analítico Medallion y catálogo semántico de alto desempeño.

---

## 1. Arquitectura Hexagonal y Monolito Modular

El sistema está diseñado bajo el patrón **Puertos y Adaptadores (Arquitectura Hexagonal)** y principios de diseño guiado por el dominio (DDD), con total apego a [PEP 8](https://peps.python.org/pep-0008/) y tipado estricto moderno:

```text
src/prospector_interno/
├── domain/                      # Entidades canónicas Nexus, Value Objects, Excepciones y Puertos
│   ├── models.py                # SourceDefinition, DatasetSpecification, IngestionRequest, BronzeArtifact
│   ├── ports.py                 # Interfaces abstractas: Storage, GatekeeperState, Catalog, ExternalCatalogClient
│   ├── exceptions.py            # Jerarquía formal de excepciones del sistema
│   └── contracts/               # Contratos Spec-Driven Development (Pydantic v2 estricto)
│       ├── silver_spec.py       # SilverFinancialRecord, StatementType, AccountCategory
│       ├── gold_spec.py         # GoldMartCarteraRecord (15 métricas y ratios contables)
│       ├── analytics_spec.py    # AnalyticsQueryRequest, AnalyticsQueryResponse
│       ├── semantic_spec.py     # HUI-16: FinancialMetricDefinition, SemanticDictionaryResponse
│       └── views_spec.py        # HUI-22: PredefinedViewMetadata, PredefinedViewResult
├── application/                 # Orquestación de Casos de Uso del Negocio
│   ├── gatekeeper.py            # HUI-01 & HUI-04: Pre-flight Gatekeeper (Cache Hit + In-Flight Lock)
│   ├── scheduler.py             # HUI-05 & HUI-06: Round-Robin multi-dominio + Token Bucket con Jitter
│   ├── bridge/                  # HUI-18 & HUI-19: Motor de Conciliación y Sync Orchestrator
│   │   ├── diff_engine.py       # DuckDBDiffEngine (cálculo de discrepancias vectorizado en RAM)
│   │   └── sync_orchestrator.py # Generación y ejecución de SyncPlan priorizado hacia Bronze
│   ├── catalog/                 # HUI-16: Gobernanza y catálogo semántico
│   │   └── semantic_service.py  # SemanticDictionaryService (13 métricas oficiales y fórmulas LaTeX)
│   ├── analytics/               # HUI-22: Servicio de vistas analíticas predefinidas
│   │   └── views_service.py     # PredefinedViewsService (consultas de negocio sobre DuckDB)
│   └── pipelines/               # HUI-12 & HUI-13: Pipelines Medallion
│       ├── finrural_parser.py   # Extractor espacial de PDF (pdftotext -layout) y balance contable
│       ├── silver_pipeline.py   # Orquestador Bronze -> Silver
│       └── gold_materializer.py # Materializador dimensional a Parquet particionado con DuckDB
└── adapters/                    # Implementaciones de Infraestructura y Drivers
    ├── storage/                 # HUI-11: Almacenamiento inmutable en Capa Bronze con renombrado atómico
    ├── memory_state.py          # Estado en memoria thread-safe para locks y catálogo
    ├── legacy/                  # Adaptador SQL declarativo para extraer el catálogo del backup PostgreSQL
    ├── external/                # HUI-18: Cliente HTTP REST desacoplado para el Prospector Externo
    ├── analytics/               # HUI-14: Adaptador analítico DuckDB OLAP en memoria sobre Parquet
    └── api/                     # HUI-21: Endpoints FastAPI y Documentación Swagger interactiva
        ├── main.py              # Aplicación central, middleware CORS y openapi_tags
        ├── schemas.py           # Esquemas Pydantic de entrada/salida para la API REST
        └── routers/             # Routers modulares: catalog, gatekeeper, reconciliation, bridge, analytics, dictionary
```

---

## 2. Capacidades Entregadas por Sprint

### Sprint 1: Resiliencia, Ingesta Segura y Capa Bronze
1. **Pre-flight Gatekeeper (`HUI-01` & `HUI-04`)**:
   - Detección de cache hit en almacenamiento antes de realizar peticiones HTTP.
   - Idempotencia con leases/locks en vuelo para evitar descargas concurrentes idénticas.
2. **Polite Scheduler (`HUI-05` & `HUI-06`)**:
   - Despacho intercalado multi-dominio (*Round-Robin*) para evitar saturar un mismo emisor.
   - Algoritmo *Token Bucket* por dominio con inyección de *Jitter Gaussiano* para simular variabilidad humana y eludir bloqueos WAF.
3. **Capa Bronze Inmutable (`HUI-11`)**:
   - Transmisión en bloques (*streaming*) a archivo temporal `.tmp.{uuid}.dat`.
   - Cálculo criptográfico del checksum SHA-256 en vuelo sin relectura de disco.
   - Renombrado atómico protegido mediante el kernel del sistema operativo (`os.replace`).
4. **Adaptador de Ingesta Legacy (Nexus Seeder)**:
   - Extracción directa mediante SQL sobre PostgreSQL (`platform_db`).
   - Normalización de las 74 fuentes institucionales reales hacia el modelo canónico `SourceDefinition`.

### Sprint 2: Reconciliation Bridge REST Desacoplado
1. **Motor de Conciliación DuckDB (`HUI-18`)**:
   - Cruce vectorizado en memoria entre el inventario local y los recursos descubiertos por el Prospector Externo.
   - Clasificación atómica en: confirmados (existentes con igual hash), nuevos para descarga y modificados (cambio de hash/URL).
2. **Generador de Planes de Sincronización Priorizados (`HUI-19`)**:
   - Creación de `SyncPlan` priorizando los periodos más recientes y orden temporal descendente.
   - Modo `dry-run` para previsualizar tareas y modo de ejecución que descarga hacia Bronze respetando el rate limiter.
3. **Desacoplamiento HTTP REST Puro**:
   - Comunicación de red limpia vía `HttpExternalCatalogClient` hacia el Prospector Externo, sin acoplamiento a rutas relativas en disco.

### Sprint 3: Núcleo Medallion, Capa Semántica & Autoservicio Analítico
1. **Pipeline de Normalización Capa Silver (`HUI-12`)**:
   - Parser especializado `FinruralPdfParser` con `pdftotext -layout` de la página 1 (*Cuadro Nº 1: Situación Patrimonial*).
   - Resolución de fechas en español (`31 de enero de 2026` $\rightarrow$ `2026-01`), balance contable ($\pm 2$ BOB) y mapeo de las 8 IFDs.
   - Generación de contratos inmutables `SilverFinancialRecord` (64 registros atómicos por boletín).
2. **Materialización Dimensional en Capa Gold (`HUI-13`)**:
   - `GoldMaterializer` pivota y calcula métricas clave: `cartera_en_mora`, `ratio_mora`, `ratio_cobertura`.
   - Particionado físico nativo Parquet en disco vía DuckDB: `gold/mart_cartera/gestion={YYYY}/mes={MM}/*.parquet`.
3. **Motor Analítico OLAP y API REST de Autoservicio (`HUI-14` & `HUI-21`)**:
   - `DuckDBAnalyticsAdapter`: Consultas vectorizadas sobre Parquet con `read_parquet(...)`, `SET max_memory = '4GB'` y sanitización estricta de columnas.
   - Endpoints FastAPI:
     - `POST /api/v1/analytics/query`: Consultas OLAP en <5ms sobre los datamarts consolidados.
     - `GET /api/v1/analytics/dimensions`: Consulta de dimensiones disponibles (instituciones, gestiones, meses).
     - `POST /api/v1/analytics/materialize`: Disparador manual del pipeline Bronze $\rightarrow$ Silver $\rightarrow$ Gold.
4. **Diccionario Semántico de Métricas Financieras (`HUI-16`)**:
   - Catálogo oficial con 13 métricas financieras formalizadas (`cartera_bruta`, `ratio_mora`, `ratio_cobertura`, `roa`, `roe`, etc.).
   - Fórmulas en LaTeX, unidades de medida y reglas de negocio accesibles en `GET /api/v1/dictionary/metrics`.
5. **Biblioteca de Vistas Analíticas Precalculadas (`HUI-22`)**:
   - Consultas de negocio estándar listas para visualización en Portal Nexus:
     - `evolucion-mora-temporal`: Serie histórica temporal mensual.
     - `ranking-solvencia-cobertura`: Clasificación jerárquica de solvencia.
     - `comparativo-entidades-resumen`: Fotografía comparativa transversal de entidades.
   - Endpoints en `GET /api/v1/analytics/views` y `POST /api/v1/analytics/views/{view_id}/execute`.

---

## 3. Pruebas Automatizadas y Calidad de Código

Para ejecutar la suite de pruebas con reporte de cobertura y verificación de estilo:

```bash
# Ejecutar suite completa con medición de cobertura (>90%)
.venv/bin/pytest --cov=prospector_interno --cov-report=term-missing

# Verificación estricta de estilo PEP 8 (0 errores)
.venv/bin/flake8 src tests --max-line-length=88
```

### Métricas Actuales del Proyecto:
- **Pruebas Automatizadas:** 91 tests pasando (0 fallos).
- **Cobertura de Código Global:** **91%** (requerimiento > 85%).
- **Auditoría PEP 8:** 0 errores / advertencias con `flake8 --max-line-length=88`.
