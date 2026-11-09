"""Ejecutor idempotente de migraciones y semillas iniciales en PostgreSQL.

Aplica la evolución DDL sobre la réplica PostgreSQL de `platform_db` y sincroniza
el inventario curado de URLs, el catálogo canónico, el diccionario técnico
dinámico y las relaciones de los 4 repositorios analíticos consolidados.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Any

import psycopg
from psycopg.rows import dict_row


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
SQL_MIGRATION_FILE = Path(__file__).resolve().parent / "001_evolve_prospector_schema.sql"


SEMANTIC_DICTIONARY_SEEDS: list[tuple[Any, ...]] = [
    # 1. mart_cartera (FINRURAL — Cartera, Mora y Previsiones)
    (
        "mart_cartera", "institucion_id", "Código de Entidad", "VARCHAR",
        "Identificador", None,
        "Sigla oficial de la Institución Financiera de Desarrollo afiliada a FINRURAL.",
        False, 1,
    ),
    (
        "mart_cartera", "institucion_nombre", "Denominación Institucional", "VARCHAR",
        "Texto", None,
        "Razón social completa de la entidad financiera supervisada o asociada.",
        False, 2,
    ),
    (
        "mart_cartera", "gestion", "Gestión Contable", "INTEGER",
        "Año", None,
        "Año fiscal del corte mensual reportado.",
        False, 3,
    ),
    (
        "mart_cartera", "mes", "Mes de Corte", "INTEGER",
        "Mes (1-12)", None,
        "Mes calendario de cierre contable del estado financiero.",
        False, 4,
    ),
    (
        "mart_cartera", "cartera_bruta", "Cartera Bruta Total", "NUMERIC(18,2)",
        "Bs.", "cartera_vigente + cartera_vencida + cartera_en_ejecucion",
        "Saldo total de colocaciones crediticias directas antes de previsiones.",
        True, 5,
    ),
    (
        "mart_cartera", "cartera_vigente", "Cartera Vigente", "NUMERIC(18,2)",
        "Bs.", None,
        "Saldo de créditos al día en el cumplimiento de su cronograma de pagos.",
        False, 6,
    ),
    (
        "mart_cartera", "cartera_en_mora", "Cartera en Mora Total", "NUMERIC(18,2)",
        "Bs.", "cartera_vencida + cartera_en_ejecucion",
        "Saldo de operaciones crediticias vencidas y en cobro judicial.",
        True, 7,
    ),
    (
        "mart_cartera", "prevision_cartera", "Previsión para Incobrabilidad", "NUMERIC(18,2)",
        "Bs.", "|Previsión Específica| + |Previsión Genérica|",
        "Fondo constituido para absorber pérdidas esperadas de cartera crediticia.",
        False, 8,
    ),
    (
        "mart_cartera", "total_activo", "Total del Activo", "NUMERIC(18,2)",
        "Bs.", None,
        "Recursos económicos totales administrados por la entidad al cierre.",
        False, 9,
    ),
    (
        "mart_cartera", "total_patrimonio", "Total del Patrimonio", "NUMERIC(18,2)",
        "Bs.", "total_activo - total_pasivo",
        "Capital institucional, reservas patrimoniales y resultados acumulados.",
        False, 10,
    ),
    (
        "mart_cartera", "ratio_mora", "Índice de Morosidad (PES)", "NUMERIC(8,4)",
        "%", "(cartera_en_mora / cartera_bruta) * 100",
        "Proporción de la cartera bruta que se encuentra vencida o en ejecución.",
        True, 11,
    ),
    (
        "mart_cartera", "ratio_cobertura", "Cobertura de Cartera en Mora", "NUMERIC(8,4)",
        "%", "(prevision_cartera / cartera_en_mora) * 100",
        "Respaldo de previsiones constituidas frente al saldo total en mora.",
        True, 12,
    ),
    # 2. mart_tasas_bcb (BCB — Indicadores Monetarios, Liquidez e Inflación)
    (
        "mart_tasas_bcb", "institucion_id", "Código de Autoridad Monetaria", "VARCHAR",
        "Identificador", None,
        "Identificador institucional del Banco Central de Bolivia.",
        False, 1,
    ),
    (
        "mart_tasas_bcb", "institucion_nombre", "Entidad Emisora", "VARCHAR",
        "Texto", None,
        "Denominación oficial de la autoridad monetaria nacional.",
        False, 2,
    ),
    (
        "mart_tasas_bcb", "gestion", "Gestión Monetaria", "INTEGER",
        "Año", None,
        "Año calendario de observación de las series monetarias.",
        False, 3,
    ),
    (
        "mart_tasas_bcb", "mes", "Mes de Observación", "INTEGER",
        "Mes (1-12)", None,
        "Mes de corte del boletín estadístico monetario del BCB.",
        False, 4,
    ),
    (
        "mart_tasas_bcb", "base_monetaria_bs", "Base Monetaria", "NUMERIC(18,2)",
        "Bs.", "Billetes y Monedas en Poder del Público + Reservas Bancarias",
        "Pasivo monetario primario del Banco Central de Bolivia expresado en bolivianos.",
        True, 5,
    ),
    (
        "mart_tasas_bcb", "emision_monetaria_bs", "Emisión Monetaria", "NUMERIC(18,2)",
        "Bs.", None,
        "Total de billetes y monedas emitidos por el ente emisor.",
        False, 6,
    ),
    (
        "mart_tasas_bcb", "reservas_bancarias_bs", "Reservas Bancarias", "NUMERIC(18,2)",
        "Bs.", "Encaje Legal MN + Encaje Legal ME + UFV",
        "Fondos de encaje legal y liquidez depositados por las entidades en el BCB.",
        False, 7,
    ),
    (
        "mart_tasas_bcb", "depositos_sistema_bs", "Depósitos del Público MN", "NUMERIC(18,2)",
        "Bs.", "Vista + Caja de Ahorro + Plazo Fijo + Otros (MN)",
        "Saldo consolidado de captaciones en moneda nacional del sistema financiero.",
        True, 8,
    ),
    (
        "mart_tasas_bcb", "cartera_sistema_bs", "Crédito al Sector Privado MN", "NUMERIC(18,2)",
        "Bs.", None,
        "Financiamiento total concedido por el sistema financiero al sector privado en MN.",
        True, 9,
    ),
    (
        "mart_tasas_bcb", "reservas_internacionales_usd", "Reservas Internacionales Netas (RIN)",
        "NUMERIC(18,2)", "Millones USD", "Oro + Divisas + DEG + Posición FMI - Obligaciones",
        "Activos externos netos de reserva administrados por el Banco Central.",
        True, 10,
    ),
    (
        "mart_tasas_bcb", "inflacion_mensual_pct", "Inflación Mensual Total", "NUMERIC(8,4)",
        "%", "Variación mensual del IPC general",
        "Variación porcentual mensual del nivel general de precios al consumidor.",
        True, 11,
    ),
    (
        "mart_tasas_bcb", "bolivianizacion_depositos_pct", "Bolivianización de Captaciones",
        "NUMERIC(8,4)", "%", "(Depósitos MN / Depósitos Totales) * 100",
        "Participación de la moneda nacional en el ahorro financiero total.",
        False, 12,
    ),
    (
        "mart_tasas_bcb", "ufv_cierre", "Cotización UFV de Cierre", "NUMERIC(10,5)",
        "Bs./UFV", None,
        "Valor oficial de la Unidad de Fomento de Vivienda al último día del mes.",
        False, 13,
    ),
    # 3. mart_estadisticas_bbv (BBV — Estados Financieros e Inversiones Bursátiles)
    (
        "mart_estadisticas_bbv", "institucion_id", "Código Bursátil", "VARCHAR",
        "Identificador", None,
        "Clave institucional del participante del mercado de valores.",
        False, 1,
    ),
    (
        "mart_estadisticas_bbv", "institucion_nombre", "Entidad del Mercado de Valores", "VARCHAR",
        "Texto", None,
        "Razón social oficial reportada en los estados financieros de la BBV.",
        False, 2,
    ),
    (
        "mart_estadisticas_bbv", "gestion", "Gestión Bursátil", "INTEGER",
        "Año", None,
        "Año del corte contable y operativo en la Bolsa Boliviana de Valores.",
        False, 3,
    ),
    (
        "mart_estadisticas_bbv", "mes", "Mes de Corte", "INTEGER",
        "Mes (1-12)", None,
        "Mes calendario del estado financiero bursátil.",
        False, 4,
    ),
    (
        "mart_estadisticas_bbv", "disponible_bs", "Disponibilidades", "NUMERIC(18,2)",
        "Bs.", None,
        "Efectivo y saldos líquidos inmediatos en bancos e instituciones de liquidación.",
        False, 5,
    ),
    (
        "mart_estadisticas_bbv", "inversiones_bursatiles_bs", "Inversiones Bursátiles",
        "NUMERIC(18,2)", "Bs.", "Deuda + Derecho Patrimonial + Operaciones de Reporto",
        "Portafolio de títulos valores negociables y operaciones de reporto vigentes.",
        True, 6,
    ),
    (
        "mart_estadisticas_bbv", "activo_corriente_bs", "Total Activo Corriente", "NUMERIC(18,2)",
        "Bs.", None,
        "Suma de disponibilidades, inversiones temporarias y cuentas por cobrar de corto plazo.",
        False, 7,
    ),
    (
        "mart_estadisticas_bbv", "inversiones_permanentes_bs", "Inversiones Permanentes",
        "NUMERIC(18,2)", "Bs.", None,
        "Inversiones financieras mantenidas a largo plazo en el mercado de capitales.",
        False, 8,
    ),
    (
        "mart_estadisticas_bbv", "total_activo", "Total del Activo", "NUMERIC(18,2)",
        "Bs.", "activo_corriente_bs + Activo No Corriente",
        "Patrimonio bruto total administrado por la entidad bursátil.",
        True, 9,
    ),
    (
        "mart_estadisticas_bbv", "pasivo_corriente_bs", "Total Pasivo Corriente", "NUMERIC(18,2)",
        "Bs.", None,
        "Obligaciones bursátiles, cuentas por pagar y provisiones exigibles a corto plazo.",
        False, 10,
    ),
    (
        "mart_estadisticas_bbv", "total_pasivo", "Total del Pasivo", "NUMERIC(18,2)",
        "Bs.", None,
        "Obligaciones totales con terceros al cierre del período.",
        False, 11,
    ),
    (
        "mart_estadisticas_bbv", "total_patrimonio", "Total del Patrimonio", "NUMERIC(18,2)",
        "Bs.", "total_activo - total_pasivo",
        "Capital pagado, reservas de ley y resultados acumulados.",
        True, 12,
    ),
    (
        "mart_estadisticas_bbv", "ratio_liquidez", "Razón de Liquidez Corriente", "NUMERIC(8,4)",
        "Veces (x)", "activo_corriente_bs / pasivo_corriente_bs",
        "Capacidad de cobertura de obligaciones bursátiles de corto plazo con activos líquidos.",
        True, 13,
    ),
    (
        "mart_estadisticas_bbv", "ratio_endeudamiento", "Nivel de Endeudamiento", "NUMERIC(8,4)",
        "%", "(total_pasivo / total_activo) * 100",
        "Proporción del activo total financiada mediante pasivos con terceros.",
        False, 14,
    ),
    # 4. mart_indicadores_asfi (ASFI — Indicadores Prudenciales y de Solvencia)
    (
        "mart_indicadores_asfi", "institucion_id", "Sigla Supervisada", "VARCHAR",
        "Identificador", None,
        "Código institucional homologado de la entidad de intermediación financiera.",
        False, 1,
    ),
    (
        "mart_indicadores_asfi", "institucion_nombre", "Entidad Supervisada", "VARCHAR",
        "Texto", None,
        "Nombre oficial de la entidad financiera sujeta a regulación prudencial.",
        False, 2,
    ),
    (
        "mart_indicadores_asfi", "gestion", "Gestión de Supervisión", "INTEGER",
        "Año", None,
        "Año del reporte prudencial mensual.",
        False, 3,
    ),
    (
        "mart_indicadores_asfi", "mes", "Mes de Supervisión", "INTEGER",
        "Mes (1-12)", None,
        "Mes de corte de los indicadores prudenciales.",
        False, 4,
    ),
    (
        "mart_indicadores_asfi", "total_activo", "Activo Total", "NUMERIC(18,2)",
        "Bs.", None,
        "Volumen total de activos e intermediación de la entidad supervisada.",
        True, 5,
    ),
    (
        "mart_indicadores_asfi", "total_pasivo", "Pasivo Total y Captaciones", "NUMERIC(18,2)",
        "Bs.", None,
        "Total de obligaciones con el público, bancos y financiadores externos.",
        False, 6,
    ),
    (
        "mart_indicadores_asfi", "total_patrimonio", "Patrimonio Neto Contable", "NUMERIC(18,2)",
        "Bs.", "total_activo - total_pasivo",
        "Respaldo patrimonial propio de la entidad regulada.",
        True, 7,
    ),
    (
        "mart_indicadores_asfi", "coeficiente_solvencia", "Solvencia Patrimonial sobre Activo",
        "NUMERIC(8,4)", "%", "(total_patrimonio / total_activo) * 100",
        "Proporción de solvencia estructural propia frente al volumen total de activos.",
        True, 8,
    ),
    (
        "mart_indicadores_asfi", "peso_cartera_activo", "Concentración Crediticia del Activo",
        "NUMERIC(8,4)", "%", "(cartera_bruta / total_activo) * 100",
        "Grado de canalización de los activos totales hacia colocaciones crediticias.",
        False, 9,
    ),
    (
        "mart_indicadores_asfi", "ratio_mora", "Índice de Morosidad Regulatoria",
        "NUMERIC(8,4)", "%", "(cartera_en_mora / cartera_bruta) * 100",
        "Porcentaje de cartera pesada sujeta a seguimiento prudencial.",
        True, 10,
    ),
    (
        "mart_indicadores_asfi", "ratio_cobertura", "Cobertura Prudencial de Previsiones",
        "NUMERIC(8,4)", "%", "(prevision_cartera / cartera_en_mora) * 100",
        "Nivel de cobertura de cartera pesada mediante previsiones constituidas.",
        False, 11,
    ),
    (
        "mart_indicadores_asfi", "apalancamiento_veces", "Multiplicador de Apalancamiento",
        "NUMERIC(8,4)", "Veces (x)", "total_activo / total_patrimonio",
        "Número de veces que el activo total multiplica el patrimonio neto institucional.",
        True, 12,
    ),
]


ASSET_RELATIONSHIPS_SEEDS: list[tuple[str, str, str, str, str, str]] = [
    (
        "mart_cartera", "gestion, mes",
        "mart_tasas_bcb", "gestion, mes",
        "N:1 (Serie Temporal Mensual)",
        "Cruza la evolución mensual de cartera y mora por entidad frente a la Base Monetaria, crédito privado e inflación publicados por el BCB.",
    ),
    (
        "mart_cartera", "institucion_id, gestion, mes",
        "mart_indicadores_asfi", "institucion_id, gestion, mes",
        "1:1 (Entidad y Corte Contable)",
        "Vincula los saldos detallados de colocaciones y previsiones con los indicadores prudenciales de solvencia y apalancamiento por institución.",
    ),
    (
        "mart_tasas_bcb", "gestion, mes",
        "mart_estadisticas_bbv", "gestion, mes",
        "1:N (Entorno Monetario y Mercado de Valores)",
        "Relaciona la liquidez agregada del sistema (Base Monetaria y RIN) con las inversiones bursátiles y liquidez de la Bolsa Boliviana de Valores.",
    ),
    (
        "mart_indicadores_asfi", "gestion, mes",
        "mart_estadisticas_bbv", "gestion, mes",
        "N:N (Comparativo Sectorial)",
        "Permite contrastar mensualmente el apalancamiento y solvencia de intermediación financiera frente a la posición patrimonial del mercado bursátil.",
    ),
]


def get_migration_pg_dsn() -> str:
    """Construye la cadena de conexión a la réplica PostgreSQL."""
    host = os.environ.get("PROSPECTOR_PG_HOST", "127.0.0.1")
    port = os.environ.get("PROSPECTOR_PG_PORT", "5434")
    dbname = os.environ.get("PROSPECTOR_PG_DB", "platform_db")
    user = os.environ.get("PROSPECTOR_PG_USER", "postgres")
    password = os.environ.get("PROSPECTOR_PG_PASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def consolidate_bronze_directories() -> dict[str, dict[str, Any]]:
    """Unifica artefactos físicos Bronze dispersos hacia prospector_interno/data/bronze."""
    canonical_bronze = PROJECT_ROOT / "data" / "bronze"
    canonical_bronze.mkdir(parents=True, exist_ok=True)
    alt_bronze = WORKSPACE_ROOT / "data" / "bronze"

    merged_by_sha: dict[str, dict[str, Any]] = {}

    for manifest_dir in (canonical_bronze, alt_bronze):
        manifest_file = manifest_dir / "bronze_manifest.json"
        if not manifest_file.is_file():
            continue
        try:
            payload = json.loads(manifest_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for item in payload.get("artifacts", []):
            sha = item.get("sha256_hash")
            rel_path = item.get("file_path", "")
            if not sha or not rel_path:
                continue
            rel_sub = rel_path.replace("data/bronze/", "", 1)
            target_file = canonical_bronze / rel_sub
            src_candidates = [
                canonical_bronze / rel_sub,
                alt_bronze / rel_sub,
            ]
            if not target_file.is_file():
                for cand in src_candidates:
                    if cand.is_file():
                        target_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(cand, target_file)
                        break
            item_copy = dict(item)
            item_copy["file_path"] = f"data/bronze/{rel_sub}"
            if sha not in merged_by_sha or target_file.is_file():
                merged_by_sha[sha] = item_copy

    manifest_path = canonical_bronze / "bronze_manifest.json"
    tmp_path = manifest_path.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps({"artifacts": list(merged_by_sha.values())}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(manifest_path)
    return merged_by_sha


def run_migrations() -> dict[str, int]:
    """Ejecuta el SQL DDL y puebla idempotentemente las tablas maestras."""
    bronze_artifacts = consolidate_bronze_directories()
    sql_script = SQL_MIGRATION_FILE.read_text(encoding="utf-8")

    with psycopg.connect(get_migration_pg_dsn(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_script)

            # 1. Sincronizar canonical_sources y canonical_datasets desde source / file
            cur.execute(
                """
                INSERT INTO public.canonical_sources (
                    source_id, canonical_code, official_name, short_name,
                    homepage, country_iso, is_active, notes
                )
                SELECT
                    s.id_source,
                    'BO_' || UPPER(REGEXP_REPLACE(s.short_name, '[^A-Za-z0-9]+', '_', 'g')) || '_' || s.id_source,
                    COALESCE(NULLIF(TRIM(s.name), ''), s.short_name),
                    s.short_name,
                    CASE WHEN TRIM(s.homepage) IN ('vacio', 'no-data', '') THEN NULL ELSE TRIM(s.homepage) END,
                    'BOL',
                    TRUE,
                    s.observations
                FROM public.source s
                WHERE s.short_name IS NOT NULL AND TRIM(s.short_name) <> ''
                ON CONFLICT (source_id) DO UPDATE SET
                    official_name = EXCLUDED.official_name,
                    homepage = EXCLUDED.homepage;
                """
            )

            # 2. Importar inventario curado desde snapshot SQLite si existe
            snapshot_db = PROJECT_ROOT / "data" / "snapshot" / "platform_prod_snapshot.db"
            imported_urls = 0
            if snapshot_db.is_file():
                with sqlite3.connect(str(snapshot_db)) as sq_conn:
                    sq_conn.row_factory = sqlite3.Row
                    rows = sq_conn.execute("SELECT * FROM url_inventory_map").fetchall()
                deduped_by_hash: dict[str, tuple[Any, ...]] = {}
                for r in rows:
                    canon_url = str(r["canonical_url"] or "").strip()
                    norm_hash = str(r["normalized_url_sha256"] or "").strip()
                    if not norm_hash:
                        basis = canon_url or f"legacy://{r['source_id']}/{r['file_code'] or r['id']}"
                        norm_hash = hashlib.sha256(basis.encode("utf-8")).hexdigest()
                    gold_tgt = r["gold_target"] or _infer_gold_target(str(r["source_id"] or ""))
                    deduped_by_hash[norm_hash] = (
                        r["source_id"] or "GENERAL",
                        r["file_code"],
                        r["file_name"],
                        canon_url,
                        norm_hash,
                        bool(r["is_discovered_web"]),
                        bool(r["is_downloaded"]),
                        bool(r["is_processed"]),
                        r["sha256_hash"],
                        r["storage_path"],
                        gold_tgt,
                        r["status"] or "CATALOGADO_BDD",
                        r["updated_at"],
                    )
                batch_params = list(deduped_by_hash.values())
                if batch_params:
                    cur.executemany(
                        """
                        INSERT INTO public.url_inventory_map (
                            source_id, file_code, file_name, canonical_url,
                            normalized_url_sha256, is_discovered_web, is_downloaded,
                            is_processed, sha256_hash, storage_path, gold_target,
                            status, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (normalized_url_sha256) DO UPDATE SET
                            is_downloaded = public.url_inventory_map.is_downloaded OR EXCLUDED.is_downloaded,
                            is_processed = public.url_inventory_map.is_processed OR EXCLUDED.is_processed,
                            sha256_hash = COALESCE(public.url_inventory_map.sha256_hash, EXCLUDED.sha256_hash),
                            storage_path = COALESCE(public.url_inventory_map.storage_path, EXCLUDED.storage_path),
                            gold_target = COALESCE(public.url_inventory_map.gold_target, EXCLUDED.gold_target);
                        """,
                        batch_params,
                    )
                    imported_urls = len(batch_params)

            # 3. Asegurar que todos los registros de public.file con URL estén en url_inventory_map
            cur.execute(
                """
                SELECT f.id_file, f.code, f.name, s.short_name,
                       COALESCE(NULLIF(TRIM(f.specific_url), ''), NULLIF(TRIM(f.main_url), '')) AS target_url
                FROM public.file f
                JOIN public.source s ON s.id_source = f.id_source
                WHERE COALESCE(NULLIF(TRIM(f.specific_url), ''), NULLIF(TRIM(f.main_url), '')) IS NOT NULL;
                """
            )
            pg_files = cur.fetchall()
            file_inv_params = []
            for pf in pg_files:
                url = str(pf["target_url"]).strip()
                if not url.startswith(("http://", "https://")):
                    continue
                url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                src_short = str(pf["short_name"] or "GENERAL").strip()
                gold_tgt = _infer_gold_target(src_short)
                file_inv_params.append(
                    (
                        src_short,
                        pf["code"],
                        pf["name"],
                        url,
                        url_hash,
                        False,
                        False,
                        False,
                        None,
                        None,
                        gold_tgt,
                        "CATALOGADO_BDD",
                        None,
                    )
                )
            if file_inv_params:
                cur.executemany(
                    """
                    INSERT INTO public.url_inventory_map (
                        source_id, file_code, file_name, canonical_url,
                        normalized_url_sha256, is_discovered_web, is_downloaded,
                        is_processed, sha256_hash, storage_path, gold_target,
                        status, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (normalized_url_sha256) DO NOTHING;
                    """,
                    file_inv_params,
                )

            # 4. Poblar diccionario semántico y relaciones entre los 4 Datamarts
            cur.execute("DELETE FROM public.semantic_dictionary;")
            cur.execute("DELETE FROM public.asset_relationships;")
            cur.executemany(
                """
                INSERT INTO public.semantic_dictionary (
                    datamart_name, column_name, business_label, data_type,
                    unit, formula, description, is_primary_metric, display_order
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (datamart_name, column_name) DO UPDATE SET
                    business_label = EXCLUDED.business_label,
                    data_type = EXCLUDED.data_type,
                    unit = EXCLUDED.unit,
                    formula = EXCLUDED.formula,
                    description = EXCLUDED.description,
                    is_primary_metric = EXCLUDED.is_primary_metric,
                    display_order = EXCLUDED.display_order;
                """,
                SEMANTIC_DICTIONARY_SEEDS,
            )

            cur.executemany(
                """
                INSERT INTO public.asset_relationships (
                    from_datamart, from_column, to_datamart, to_column,
                    relationship_type, business_rule
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (from_datamart, from_column, to_datamart, to_column) DO UPDATE SET
                    relationship_type = EXCLUDED.relationship_type,
                    business_rule = EXCLUDED.business_rule;
                """,
                ASSET_RELATIONSHIPS_SEEDS,
            )

            # 5. Refrescar vista materializada
            cur.execute("REFRESH MATERIALIZED VIEW public.mv_nexus_active_catalog;")

            cur.execute("SELECT COUNT(*) AS cnt FROM public.url_inventory_map;")
            total_inv = int((cur.fetchone() or {}).get("cnt", 0))
            cur.execute("SELECT COUNT(*) AS cnt FROM public.semantic_dictionary;")
            total_dict = int((cur.fetchone() or {}).get("cnt", 0))
            cur.execute("SELECT COUNT(*) AS cnt FROM public.asset_relationships;")
            total_rels = int((cur.fetchone() or {}).get("cnt", 0))

        conn.commit()

    return {
        "bronze_artifacts_unified": len(bronze_artifacts),
        "imported_snapshot_urls": imported_urls,
        "url_inventory_total": total_inv,
        "dictionary_entries": total_dict,
        "asset_relationships": total_rels,
    }


def _infer_gold_target(source_code: str) -> str | None:
    upper = source_code.upper()
    if "FINRURAL" in upper:
        return "mart_cartera"
    if "BCB" in upper:
        return "mart_tasas_bcb"
    if "BBV" in upper:
        return "mart_estadisticas_bbv"
    if "ASFI" in upper:
        return "mart_indicadores_asfi"
    return None


if __name__ == "__main__":
    summary = run_migrations()
    print(json.dumps(summary, indent=2, ensure_ascii=False))
