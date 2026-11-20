-- =============================================================================
-- DATAX — Migración Idempotente 002: Prospector Interno Datos en Bruto (v2.0)
-- Objetivo: Soportar locks distribuidos con TTL, cola multi-worker SKIP LOCKED,
-- perfilado técnico automático, índice de columnas, deduplicación en doble capa,
-- auditoría inmutable y gobierno de llaves API con expiración y alcance.
-- Sin credenciales embebidas; tipos nativos TIMESTAMPTZ, JSONB y BIGINT.
-- =============================================================================

BEGIN;

-- 1. Locks distribuidos con TTL para Pre-Flight Gatekeeper (seguro multi-worker)
CREATE TABLE IF NOT EXISTS public.gatekeeper_locks (
    idempotency_key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    worker_id TEXT,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gk_locks_expires_at
    ON public.gatekeeper_locks(expires_at);

-- 2. Perfilado técnico estructural de archivos en bruto (sin alterar el binario)
CREATE TABLE IF NOT EXISTS public.asset_profiles (
    asset_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    file_format TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256_hash CHAR(64),
    detected_encoding TEXT,
    csv_delimiter TEXT,
    sheet_names JSONB NOT NULL DEFAULT '[]'::jsonb,
    row_count BIGINT NOT NULL DEFAULT 0,
    column_count INTEGER NOT NULL DEFAULT 0,
    columns_summary JSONB NOT NULL DEFAULT '[]'::jsonb,
    sample_rows JSONB NOT NULL DEFAULT '[]'::jsonb,
    archive_entries JSONB NOT NULL DEFAULT '[]'::jsonb,
    profiled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_asset_profiles_source
    ON public.asset_profiles(UPPER(source_id));
CREATE INDEX IF NOT EXISTS idx_asset_profiles_sha256
    ON public.asset_profiles(sha256_hash);

-- 3. Catálogo indexado de columnas para búsqueda global por nombre de columna
CREATE TABLE IF NOT EXISTS public.asset_columns_catalog (
    id BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    dataset_id TEXT,
    sheet_name TEXT NOT NULL DEFAULT '',
    column_name TEXT NOT NULL,
    inferred_type TEXT NOT NULL,
    null_pct DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    distinct_count BIGINT NOT NULL DEFAULT 0,
    sample_values JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (asset_id, sheet_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_asset_cols_name_lower
    ON public.asset_columns_catalog(LOWER(column_name));
CREATE INDEX IF NOT EXISTS idx_asset_cols_source
    ON public.asset_columns_catalog(UPPER(source_id));
CREATE INDEX IF NOT EXISTS idx_asset_cols_asset
    ON public.asset_columns_catalog(asset_id);

-- 4. Auditoría inmutable de colisiones evitadas (deduplicación en doble capa)
CREATE TABLE IF NOT EXISTS public.deduplication_audit (
    audit_id TEXT PRIMARY KEY,
    request_id TEXT,
    source_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    criterion TEXT NOT NULL,
    reused_asset_id TEXT,
    reused_sha256 CHAR(64),
    bytes_saved BIGINT NOT NULL DEFAULT 0,
    detail TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dedup_audit_detected
    ON public.deduplication_audit(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_dedup_audit_criterion
    ON public.deduplication_audit(criterion);
CREATE INDEX IF NOT EXISTS idx_dedup_audit_source
    ON public.deduplication_audit(UPPER(source_id));

-- 5. Alias de trazabilidad cuando distintas URLs producen el mismo SHA-256 binario
CREATE TABLE IF NOT EXISTS public.asset_aliases (
    alias_id TEXT PRIMARY KEY,
    canonical_url TEXT NOT NULL,
    normalized_url_sha256 CHAR(64) NOT NULL,
    source_id TEXT NOT NULL,
    target_asset_id TEXT NOT NULL,
    target_sha256 CHAR(64) NOT NULL,
    etag TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (normalized_url_sha256, target_sha256)
);

CREATE INDEX IF NOT EXISTS idx_asset_aliases_url_hash
    ON public.asset_aliases(normalized_url_sha256);
CREATE INDEX IF NOT EXISTS idx_asset_aliases_target_sha
    ON public.asset_aliases(target_sha256);

-- 6. Auditoría de overrides de estado en curaduría de inventario
CREATE TABLE IF NOT EXISTS public.inventory_status_audit (
    audit_id TEXT PRIMARY KEY,
    url_id INTEGER,
    canonical_url TEXT NOT NULL,
    source_id TEXT NOT NULL,
    previous_is_downloaded BOOLEAN,
    new_is_downloaded BOOLEAN,
    previous_is_processed BOOLEAN,
    new_is_processed BOOLEAN,
    actor TEXT NOT NULL DEFAULT 'MANUAL_OPERATOR',
    override_reason TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inv_audit_occurred
    ON public.inventory_status_audit(occurred_at DESC);

-- 7. Evolución aditiva de public.download_tracking para workers SKIP LOCKED
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 5;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS speed_bps DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS max_retries INTEGER NOT NULL DEFAULT 3;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS subscribers_jsonb JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS worker_id TEXT;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS resource_key TEXT;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS dataset_code TEXT;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS period_label TEXT;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS etag TEXT;
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE public.download_tracking ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_pg_dl_claim
    ON public.download_tracking(status, priority, created_at);

-- 8. Evolución aditiva de public.internal_assets (esquema, compresión, integridad y BIGINT)
ALTER TABLE public.internal_assets ALTER COLUMN row_count TYPE BIGINT;
ALTER TABLE public.internal_assets ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE public.internal_assets ADD COLUMN IF NOT EXISTS schema_hash CHAR(64);
ALTER TABLE public.internal_assets ADD COLUMN IF NOT EXISTS compression_ratio DOUBLE PRECISION;
ALTER TABLE public.internal_assets ADD COLUMN IF NOT EXISTS integrity_status TEXT NOT NULL DEFAULT 'VERIFIED';

CREATE INDEX IF NOT EXISTS idx_pg_assets_sha
    ON public.internal_assets(content_hash_sha256);

-- 9. Gobierno de llaves API con expiración, alcance por capas y revocación
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS allowed_layers TEXT[] NOT NULL DEFAULT ARRAY['RAW', 'GOLD', 'SQL']::TEXT[];
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

-- 10. Auditoría extendida de accesos permitidos y denegados en access_logs
ALTER TABLE public.access_logs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ALLOWED';
ALTER TABLE public.access_logs ADD COLUMN IF NOT EXISTS denial_reason TEXT;
ALTER TABLE public.access_logs ADD COLUMN IF NOT EXISTS layer TEXT;

-- Otorgar permisos al rol operativo solo si ya existe (sin crear roles con contraseñas)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'prospector_datos') THEN
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO prospector_datos;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO prospector_datos;
    END IF;
END
$$;

COMMIT;
