-- =============================================================================
-- DATAX — Migración Idempotente 001: Evolución Productiva de platform_db
-- Objetivo: Aplicar ADD COLUMN / DROP COLUMN sin mutar filas históricas y
-- centralizar todas las tablas operativas del Prospector Interno en PostgreSQL.
-- =============================================================================

BEGIN;

-- 0. Rol opcional de aplicación (idempotente)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'prospector_datos') THEN
        CREATE ROLE prospector_datos WITH LOGIN PASSWORD 'Datax26$*/';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE platform_db TO prospector_datos;
GRANT ALL ON SCHEMA public TO prospector_datos;

-- 1. Actualizar función de trigger log_file_alert() ANTES de eliminar navigation_path
-- para garantizar compatibilidad total con INSERT/UPDATE sobre public.file.
CREATE OR REPLACE FUNCTION public.log_file_alert()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_n_days INTEGER;
    v_diff_days INTEGER;
    v_status_color VARCHAR(50);
BEGIN
    IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE' AND OLD.updated_to IS DISTINCT FROM NEW.updated_to) THEN
        SELECT COALESCE(
            (d.delay * CASE
                WHEN d.frecuency_delay ILIKE 'diari%' THEN 1
                WHEN d.frecuency_delay ILIKE 'semanal%' THEN 7
                WHEN d.frecuency_delay ILIKE 'mensual%' THEN 30
                WHEN d.frecuency_delay ILIKE 'trimestral%' THEN 90
                WHEN d.frecuency_delay ILIKE 'semestral%' THEN 180
                WHEN d.frecuency_delay ILIKE 'anual%' THEN 365
                ELSE 1 END),
            (CASE
                WHEN NEW.publication_frequency ILIKE 'diari%' THEN 1
                WHEN NEW.publication_frequency ILIKE 'semanal%' THEN 7
                WHEN NEW.publication_frequency ILIKE 'mensual%' THEN 30
                WHEN NEW.publication_frequency ILIKE 'trimestral%' THEN 90
                WHEN NEW.publication_frequency ILIKE 'semestral%' THEN 180
                WHEN NEW.publication_frequency ILIKE 'anual%' THEN 365
                ELSE 0 END)
        ) INTO v_n_days
        FROM (SELECT 1) AS dummy
        LEFT JOIN delay d ON d.id_file = NEW.id_file;

        IF NEW.updated_to IS NOT NULL AND NEW.updated_to ~ '^\d{4}-\d{2}-\d{2}' THEN
            v_diff_days := (CURRENT_DATE - NEW.updated_to::DATE);
        ELSE
            v_diff_days := NULL;
        END IF;

        v_status_color := CASE
            WHEN v_diff_days IS NULL THEN 'gray'
            WHEN v_diff_days <= (v_n_days - 1) THEN 'green'
            WHEN v_diff_days = v_n_days THEN 'orange'
            WHEN v_diff_days = v_n_days + 1 THEN 'yellow'
            ELSE 'red'
        END;

        INSERT INTO alerts (
            short_name, db_code, file_code, report_code,
            name_data_base, name_file, publication_frequency,
            navigation_path, updated_to, main_url, natural_delay
        )
        SELECT
            s.short_name,
            db.db_code,
            NEW.code,
            r.code,
            db.name,
            NEW.name,
            NEW.publication_frequency,
            COALESCE(NEW.path, ''),
            CASE WHEN NEW.updated_to ~ '^\d{4}-\d{2}-\d{2}' THEN NEW.updated_to::DATE ELSE NULL END,
            NEW.main_url,
            v_status_color
        FROM source s
        LEFT JOIN report r ON r.id_file = NEW.id_file
        LEFT JOIN data_base_report dbr ON r.code = dbr.report_code
        LEFT JOIN data_base db ON dbr.db_code = db.db_code
        WHERE s.id_source = NEW.id_source;
    END IF;
    RETURN NEW;
END;
$function$;

-- 2. Evolución de public.source (ADD COLUMN)
ALTER TABLE public.source ADD COLUMN IF NOT EXISTS health_state VARCHAR(20) DEFAULT 'ACTIVE';
ALTER TABLE public.source ADD COLUMN IF NOT EXISTS rate_limit_delay_seconds DOUBLE PRECISION DEFAULT 1.0;
ALTER TABLE public.source ADD COLUMN IF NOT EXISTS workflow_strategy VARCHAR(30) DEFAULT 'html';

-- 3. Evolución de public.file (ADD COLUMN + DROP COLUMN de columnas manuales obsoletas)
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS is_downloaded BOOLEAN DEFAULT FALSE;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS is_processed BOOLEAN DEFAULT FALSE;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS processing_mode VARCHAR(20) DEFAULT 'AUTO';
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS is_verified BOOLEAN DEFAULT FALSE;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS verified_by VARCHAR(100);
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS processing_notes TEXT;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS normalized_url_sha256 VARCHAR(64);
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS contingency_urls TEXT;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS last_http_status SMALLINT;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS structural_signature VARCHAR(64);
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS gold_table_target VARCHAR(64);
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS priority_level SMALLINT DEFAULT 2;
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS data_quality_score NUMERIC(5,2);
ALTER TABLE public.file ADD COLUMN IF NOT EXISTS row_count INTEGER;

ALTER TABLE public.file DROP COLUMN IF EXISTS is_false_positive;
ALTER TABLE public.file DROP COLUMN IF EXISTS color;
ALTER TABLE public.file DROP COLUMN IF EXISTS task;
ALTER TABLE public.file DROP COLUMN IF EXISTS accion;
ALTER TABLE public.file DROP COLUMN IF EXISTS general_status;
ALTER TABLE public.file DROP COLUMN IF EXISTS navigation_path;
ALTER TABLE public.file DROP COLUMN IF EXISTS section_path;

-- 4. Evolución de public.download (ADD COLUMN de custodia SHA-256)
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS file_sha256 CHAR(64);
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS source_resource_key VARCHAR(64);
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS content_length_bytes BIGINT;
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS storage_path_bronze VARCHAR(255);
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS is_downloaded BOOLEAN DEFAULT TRUE;
ALTER TABLE public.download ADD COLUMN IF NOT EXISTS is_processed BOOLEAN DEFAULT FALSE;

-- 5. Tabla unificada de Inventario y Curaduría de URLs (url_inventory_map)
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

ALTER TABLE public.url_inventory_map ALTER COLUMN source_id TYPE TEXT;
ALTER TABLE public.url_inventory_map ALTER COLUMN file_code TYPE TEXT;
ALTER TABLE public.url_inventory_map ALTER COLUMN normalized_url_sha256 TYPE TEXT;
ALTER TABLE public.url_inventory_map ALTER COLUMN sha256_hash TYPE TEXT;
ALTER TABLE public.url_inventory_map ALTER COLUMN gold_target TYPE TEXT;
ALTER TABLE public.url_inventory_map ALTER COLUMN status TYPE TEXT;

CREATE INDEX IF NOT EXISTS idx_url_inv_source ON public.url_inventory_map(UPPER(source_id));
CREATE INDEX IF NOT EXISTS idx_url_inv_status ON public.url_inventory_map(status);
CREATE INDEX IF NOT EXISTS idx_url_inv_dl_proc ON public.url_inventory_map(is_downloaded, is_processed);

-- 6. Tabla en vivo de Seguimiento y Cola de Descargas (download_tracking - Obj. 6)
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

-- 7. Tablas de Activos Consolidados y Linaje Criptográfico (Obj. 2 y 3)
CREATE TABLE IF NOT EXISTS public.internal_assets (
    asset_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    file_path TEXT NOT NULL,
    content_hash_sha256 TEXT NOT NULL,
    row_count INTEGER,
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

CREATE TABLE IF NOT EXISTS public.canonical_sources (
    source_id INTEGER PRIMARY KEY,
    canonical_code TEXT NOT NULL UNIQUE,
    official_name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    homepage TEXT,
    country_iso TEXT NOT NULL,
    is_active BOOLEAN NOT NULL,
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
    is_active BOOLEAN NOT NULL
);

-- 8. Diccionario Técnico Dinámico y Relaciones entre Repositorios (Obj. 3)
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

ALTER TABLE public.asset_relationships ALTER COLUMN from_column TYPE TEXT;
ALTER TABLE public.asset_relationships ALTER COLUMN to_column TYPE TEXT;
ALTER TABLE public.asset_relationships ALTER COLUMN relationship_type TYPE TEXT;

-- 9. Autoservicio Externo (Excel / Power BI) con API Keys y Bitácora (Obj. 4)
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

-- 10. Vista Materializada de Catálogo Activo para respuestas < 5ms
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_nexus_active_catalog AS
WITH deduplicated_file AS (
    SELECT DISTINCT ON (file.code)
        file.id_file,
        file.id_source,
        file.code,
        file.name,
        file.specific_url,
        file.main_url,
        file.publication_frequency,
        file.type,
        file.creation_date,
        file.state
    FROM public.file
    WHERE file.code IS NOT NULL AND TRIM(BOTH FROM file.code) <> ''
    ORDER BY file.code, file.id_file DESC
)
SELECT
    s.id_source,
    s.short_name AS source_code,
    s.name AS source_name,
    CASE
        WHEN TRIM(BOTH FROM s.homepage) = ANY (ARRAY['vacio', 'no-data', '']) THEN NULL
        ELSE TRIM(BOTH FROM s.homepage)
    END AS source_homepage,
    f.id_file,
    f.code AS legacy_file_code,
    'DS_BO_' || upper(regexp_replace(s.short_name, '[^A-Za-z0-9]+', '_', 'g'))
        || '_' || upper(regexp_replace(f.code, '[^A-Za-z0-9]+', '_', 'g')) AS canonical_dataset_code,
    f.name AS dataset_name,
    f.publication_frequency,
    COALESCE(NULLIF(TRIM(BOTH FROM f.specific_url), ''), NULLIF(TRIM(BOTH FROM f.main_url), '')) AS target_url,
    f.type AS format_type,
    f.creation_date,
    max(d.download_date) AS last_download_date,
    max(d.download_hash) AS last_known_hash,
    count(d.id_download) AS total_historical_downloads
FROM public.source s
JOIN deduplicated_file f ON s.id_source = f.id_source
LEFT JOIN public.download d ON f.id_file = d.id_file
GROUP BY
    s.id_source, s.short_name, s.name, s.homepage,
    f.id_file, f.code, f.name, f.publication_frequency,
    f.specific_url, f.main_url, f.type, f.creation_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_nexus_dataset_code
    ON public.mv_nexus_active_catalog(canonical_dataset_code);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO prospector_datos;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO prospector_datos;

COMMIT;
