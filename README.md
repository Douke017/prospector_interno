# Prospector Interno (DATAX Equipo 2) — Target: Portal Nexus

Plataforma de adquisición masiva de datos económicos, ingesta resiliente y repositorio analítico de alto desempeño.

---

## 1. Arquitectura Hexagonal y Monolito Modular

El sistema está diseñado bajo el patrón **Puertos y Adaptadores (Arquitectura Hexagonal)** y principios de diseño guiado por el dominio (DDD), con total apego a [PEP 8](https://peps.python.org/pep-0008/) y tipado estricto moderno:

```
src/prospector_interno/
├── domain/                  # Entidades canónicas Nexus, Value Objects, Excepciones y Puertos
│   ├── models.py            # SourceDefinition, DatasetSpecification, IngestionRequest, BronzeArtifact
│   ├── ports.py             # Interfaces abstractas: Storage, GatekeeperState, Catalog, RateLimiter
│   └── exceptions.py        # Jerarquía formal de excepciones del sistema
├── application/             # Orquestación de Casos de Uso del Negocio
│   ├── gatekeeper.py        # HUI-01 & HUI-04: Pre-flight Gatekeeper (Cache Hit + In-Flight Lock)
│   └── scheduler.py         # HUI-05 & HUI-06: Round-Robin multi-dominio + Token Bucket con Jitter
└── adapters/                # Implementaciones de Infraestructura y Drivers
    ├── storage/             # HUI-11: Almacenamiento inmutable en Capa Bronze con renombrado atómico
    ├── memory_state.py      # Estado en memoria thread-safe para locks y catálogo
    └── legacy/              # Adaptador SQL declarativo para extraer el catálogo del backup PostgreSQL
```

---

## 2. Capacidades de Sprint 1 Entregadas

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

---

## 3. Pruebas Automatizadas y Calidad de Código

Para ejecutar la suite de pruebas con reporte de cobertura y verificación de estilo:

```bash
# Ejecutar pruebas unitarias e integración con cobertura (>95%)
.venv/bin/pytest --cov=prospector_interno --cov-report=term-missing

# Verificación estricta de estilo PEP 8
.venv/bin/flake8 src tests --max-line-length=88
```
