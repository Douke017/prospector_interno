-- =============================================================================
-- DATAX — Esquema Base Standalone (000) para PostgreSQL 16 Dedicado
-- Objetivo: Inicializar una instancia PostgreSQL 16 limpia y aislada para
-- Prospector Interno sin depender de tablas legacy (`source`/`file`/`download`)
-- ni credenciales embebidas. Se ejecuta antes de 002_raw_data_prospector_evolution.sql.
-- =============================================================================

BEGIN;

-- 1. Inventario unificado y curaduría de URLs
CREATE TABLE IF NOT EXISTS public.url_inventory_map (
    id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    file_code TEXT,
    file_name TEXT,
    canonical_url TEXT NOT NULL,
    normalized_url_sha256 TEXT NOT NULL UNIQUE,
    is_discovered_web BOOLEAN NOT NULL DEFAULT FALSE,
    is_downloaded BOOLEAN NOT NULL DEFAULT FALSE,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    sha256_hash TEXT,
    storage_path TEXT,
    gold_target TEXT,
    status TEXT NOT NULL DEFAULT 'CATALOGADO_BDD',
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_url_inv_source ON public.url_inventory_map(UPPER(source_id));
CREATE INDEX IF NOT EXISTS idx_url_inv_status ON public.url_inventory_map(status);
CREATE INDEX IF NOT EXISTS idx_url_inv_dl_proc ON public.url_inventory_map(is_downloaded, is_processed);

-- 2. Seguimiento y cola multi-worker de descargas
CREATE TABLE IF NOT EXISTS public.download_tracking (
    download_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    local_path TEXT,
    status TEXT NOT NULL,
    sha256_checksum TEXT,
    bytes_downloaded BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT,
    task_id TEXT,
    subscribers TEXT NOT NULL DEFAULT '[]',
    started_at TEXT,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_pg_download_url ON public.download_tracking(canonical_url);
CREATE INDEX IF NOT EXISTS idx_pg_download_status ON public.download_tracking(status);
CREATE INDEX IF NOT EXISTS idx_pg_download_source ON public.download_tracking(UPPER(source_id));
CREATE INDEX IF NOT EXISTS idx_pg_download_checksum ON public.download_tracking(sha256_checksum);

-- 3. Activos custodiados y linaje criptográfico SHA-256
CREATE TABLE IF NOT EXISTS public.internal_assets (
    asset_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    file_path TEXT NOT NULL,
    content_hash_sha256 TEXT NOT NULL,
    row_count BIGINT,
    size_bytes BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    last_updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_assets_layer ON public.internal_assets(storage_layer);
CREATE INDEX IF NOT EXISTS idx_pg_assets_dataset ON public.internal_assets(dataset_id);

CREATE TABLE IF NOT EXISTS public.data_lineage (
    lineage_id TEXT PRIMARY KEY,
    gold_asset_id TEXT NOT NULL REFERENCES public.internal_assets(asset_id) ON DELETE CASCADE,
    bronze_source_file TEXT NOT NULL,
    bronze_file_sha256 TEXT NOT NULL,
    external_run_id TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    transformed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_lineage_gold ON public.data_lineage(gold_asset_id);
CREATE INDEX IF NOT EXISTS idx_pg_lineage_sha ON public.data_lineage(bronze_file_sha256);

-- 4. Catálogo canónico de fuentes y datasets
CREATE TABLE IF NOT EXISTS public.canonical_sources (
    source_id INTEGER PRIMARY KEY,
    canonical_code TEXT NOT NULL UNIQUE,
    official_name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    homepage TEXT,
    country_iso TEXT NOT NULL DEFAULT 'BOL',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS public.canonical_datasets (
    dataset_code TEXT PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES public.canonical_sources(source_id) ON DELETE CASCADE,
    legacy_file_code TEXT NOT NULL,
    name TEXT NOT NULL,
    periodicity TEXT NOT NULL,
    url_template TEXT NOT NULL,
    expected_format TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- 5. Diccionario técnico y relaciones entre repositorios
CREATE TABLE IF NOT EXISTS public.semantic_dictionary (
    id SERIAL PRIMARY KEY,
    datamart_name VARCHAR(64) NOT NULL,
    column_name VARCHAR(64) NOT NULL,
    business_label VARCHAR(120) NOT NULL,
    data_type VARCHAR(40) NOT NULL,
    unit VARCHAR(40) NOT NULL,
    formula TEXT,
    description TEXT NOT NULL,
    is_primary_metric BOOLEAN NOT NULL DEFAULT FALSE,
    display_order INTEGER NOT NULL DEFAULT 0,
    UNIQUE (datamart_name, column_name)
);

CREATE TABLE IF NOT EXISTS public.asset_relationships (
    id SERIAL PRIMARY KEY,
    from_datamart VARCHAR(64) NOT NULL,
    from_column TEXT NOT NULL,
    to_datamart VARCHAR(64) NOT NULL,
    to_column TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    business_rule TEXT NOT NULL,
    UNIQUE (from_datamart, from_column, to_datamart, to_column)
);

-- 6. Gobierno de llaves API y bitácora de acceso externo
CREATE TABLE IF NOT EXISTS public.api_keys (
    key_id VARCHAR(64) PRIMARY KEY,
    label VARCHAR(120) NOT NULL,
    key_prefix VARCHAR(24) NOT NULL,
    key_sha256 CHAR(64) NOT NULL UNIQUE,
    allowed_datamarts TEXT NOT NULL DEFAULT '*',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    usage_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.access_logs (
    log_id SERIAL PRIMARY KEY,
    key_id VARCHAR(64),
    key_label VARCHAR(120),
    datamart_name VARCHAR(64) NOT NULL,
    export_format VARCHAR(24) NOT NULL,
    rows_returned INTEGER NOT NULL DEFAULT 0,
    client_agent TEXT,
    accessed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
