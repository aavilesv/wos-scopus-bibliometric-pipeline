# ============================================================
# main.py
#   - Script Principal (Orquestador) del Pipeline Bibliométrico
#   - Coordina la carga, limpieza, deduplicación y reporte.
# ============================================================
from __future__ import annotations

import time
import pandas as pd

# --- Importaciones de Infraestructura y Utilidades ---
import config  # Archivo de configuración con rutas y constantes
from logging_utils import setup_logger  # Sistema de logs (reemplaza print)
# Funciones para validar archivos de entrada antes de empezar
from file_validation import build_default_paths, scan_inputs, validate_or_stop, PipelinePaths
# Nueva Validación de Calidad
from data_quality import validate_quality

# --- Importaciones de Lógica de Negocio (Módulos) ---
from loaders import load_merge_scopus, load_merge_wos  # Carga y limpieza inicial
from deduplication import cross_deduplicate  # Lógica de deduplicación (paralela)
from normalization import normalize_wos_to_scopus_schema, apply_post_merge_normalization  # Normalización de datos
from scimago_utils import load_scimago_if_exists, build_scimago_map, apply_scimago_canonical_titles  # Utilidades SCImago
from sjr_analysis import enrich_with_scimago  # Cruce final con métricas SCImago
from doi_validation import validate_dois_online # Validación de DOIs Online
from reporting import (  # Generación de reportes y gráficas
    save_outputs,
    build_report_tables,
    save_report_excel,
    plot_distribution,
    plot_raw_trends
)

# Configuración del Logger Global
# Se guardarán logs en la carpeta definida en config.BASE_DIR / "logs"
logger = setup_logger("bibliometric_pipeline", log_dir=config.BASE_DIR / "logs")


def main() -> None:
    # Inicio del cronómetro para medir tiempo total de ejecución
    start_time = time.time()
    
    # Mensajes de inicio en el log
    logger.info("=" * 60)
    logger.info("Bibliometric Review Pipeline")
    logger.info("Execution started...")
    logger.info("=" * 60)
    
    # --------------------------------------------------------
    # 1) Definición de Rutas y Validación de Inputs
    # --------------------------------------------------------
    # build_default_paths: Detecta automáticamente las carpetas FILES/SCOPUS, FILES/WOS, etc.
    paths = build_default_paths()
    # scan_inputs: Busca archivos dentro de esas carpetas
    inv = scan_inputs(paths)

    # validate_or_stop: Verifica que existan archivos. Si no, detiene el programa.
    if not validate_or_stop(paths, inv):
        logger.error("Validation failed. Stopping.")
        return

    # Flags para saber qué datos tenemos disponibles
    has_scopus = len(inv.scopus_files) > 0
    has_wos = len(inv.wos_files) > 0
    
    logger.info(f"Input detected: Scopus Files={len(inv.scopus_files)}, WoS Files={len(inv.wos_files)}")

    # --------------------------------------------------------
    # 2) Carga de Datos Auxiliares (SCImago)
    # --------------------------------------------------------
    logger.info("Loading SCImago data...")
    # Carga el archivo CSV de SCImago si existe
    scimago_df = load_scimago_if_exists(paths)
    # Crea un mapa rápido (diccionario) de ISSN -> Título Canónico para normalizar nombres
    scimago_map = build_scimago_map(scimago_df) if scimago_df is not None else {}
    
    if scimago_df is not None:
        logger.info(f"SCImago loaded: {len(scimago_df)} rows")
    else:
        logger.warning("SCImago file not found or empty.")

    # --------------------------------------------------------
    # 3) Carga y Merge (Unificación) de Fuentes
    # --------------------------------------------------------
    scopus_df = pd.DataFrame()
    wos_df = pd.DataFrame()
    original_scopus = 0  # Contadores para estadísticas
    original_wos = 0
    scopus_stats = {}
    wos_stats = {}

    if has_scopus:
        logger.info("Processing Scopus files...")
        scopus_df, original_scopus = load_merge_scopus(inv.scopus_files)
        scopus_internal_dups = original_scopus - len(scopus_df)
        
        # --- VALIDACIÓN DE CALIDAD SCOPUS ---
        scopus_after_internal = len(scopus_df)
        scopus_df, scopus_stats = validate_quality(scopus_df, "Scopus")
        scopus_stats['internal_duplicates'] = scopus_internal_dups
        
        # [Assertion] Stage_n - Removed_n = Stage_n+1
        sc_quality_removed = (scopus_stats.get('removed_doi', 0) + scopus_stats.get('removed_title', 0) + 
                              scopus_stats.get('removed_authors', 0) + scopus_stats.get('removed_abstract', 0))
        assert scopus_after_internal - sc_quality_removed == len(scopus_df), "Scopus Quality conservation failed"
        
        logger.info(f"Scopus merged: {len(scopus_df)} unique records (from {original_scopus} raw)")

    if has_wos:
        logger.info("Processing WoS files...")
        wos_df, original_wos = load_merge_wos(inv.wos_files)
        wos_internal_dups = original_wos - len(wos_df)
        
        # --- VALIDACIÓN DE CALIDAD WOS ---
        wos_after_internal = len(wos_df)
        wos_df, wos_stats = validate_quality(wos_df, "WoS")
        wos_stats['internal_duplicates'] = wos_internal_dups
        
        # [Assertion] Stage_n - Removed_n = Stage_n+1
        wos_quality_removed = (wos_stats.get('removed_doi', 0) + wos_stats.get('removed_title', 0) + 
                               wos_stats.get('removed_authors', 0) + wos_stats.get('removed_abstract', 0))
        assert wos_after_internal - wos_quality_removed == len(wos_df), "WoS Quality conservation failed"
        
        logger.info(f"WoS merged: {len(wos_df)} unique records (from {original_wos} raw)")

    # --------------------------------------------------------
    # 4) Deduplicación Cruzada (Cross-Deduplication)
    # --------------------------------------------------------
    duplicated_titles = set()
    # Solo ejecutamos si tenemos ambas fuentes con datos
    if has_scopus and has_wos and (not scopus_df.empty) and (not wos_df.empty):
        logger.info(f"Starting Cross-Deduplication (Threshold: {config.FUZZY_THRESHOLD})...")
        # cross_deduplicate: Retorna la lista de títulos de WoS que ya existen en Scopus
        duplicated_titles = cross_deduplicate(
            scopus_df=scopus_df,
            wos_df=wos_df,
            threshold=config.FUZZY_THRESHOLD  # Umbral desde config (ej. 85)
        )
        logger.info(f"Duplicates identified: {len(duplicated_titles)}")

    # --------------------------------------------------------
    # 5) Marcar Duplicados (Columna 'In_Both')
    # --------------------------------------------------------
    # Agregamos una bandera (1 o 0) indicando si el artículo está en ambas fuentes
    if has_wos and not wos_df.empty:
        wos_df["In_Both"] = wos_df["processed_title"].isin(duplicated_titles).astype(int)

    if has_scopus and not scopus_df.empty:
        scopus_df["In_Both"] = scopus_df["processed_title"].isin(duplicated_titles).astype(int)

    # --------------------------------------------------------
    # 6) Normalización de WoS al Esquema de Scopus
    # --------------------------------------------------------
    wos_non_repeated = pd.DataFrame()
    wos_norm = pd.DataFrame()

    if has_wos and not wos_df.empty:
        logger.info("Normalizing WoS schema to Scopus...")
        wos_before_cross = len(wos_df)
        wos_non_repeated, wos_norm = normalize_wos_to_scopus_schema(wos_df)
        
        # [Assertion] Ensure exactly the duplicate titles flag removed the right amount of WoS records
        # Note: If there were any duplicates, they should equal the difference
        assert wos_before_cross - len(wos_non_repeated) == len(wos_df[wos_df['In_Both'] == 1]), "Cross Deduplication conservation failed"

    # --------------------------------------------------------
    # 7) Combinación Final (Merge)
    # --------------------------------------------------------
    parts = []
    if has_scopus and not scopus_df.empty:
        parts.append(scopus_df)
    if has_wos and not wos_norm.empty:
        parts.append(wos_norm) # Solo agregamos la versión normalizada y SIN duplicados de WoS

    # pd.concat une los dataframes verticalmente
    scopus_to_merge_len = len(scopus_df) if has_scopus else 0
    wos_to_merge_len = len(wos_norm) if has_wos else 0
    combined_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    
    # [Assertion] Merge conservation
    assert len(combined_df) == scopus_to_merge_len + wos_to_merge_len, "Merge conservation failed"
    logger.info(f"Combined dataset: {len(combined_df)} rows")

    # --------------------------------------------------------
    # 7.5) Validación Online de DOIs (Cruce con Crossref)
    # --------------------------------------------------------
    # Valida la autenticidad del campo DOI (si lo hay) y agrega las columnas:
    # doi_verified, doi_validation_status, doi_metadata_found
    doi_val_counts = {}
    if not combined_df.empty:
        logger.info("Applying Online DOI Validation using Crossref...")
        # Llama a nuestro nuevo módulo que usa caché y consultas online
        combined_df, doi_val_counts = validate_dois_online(combined_df, source_name="Combined")
        logger.info(f"Online DOI validation completed. Results: {doi_val_counts}")

    # --------------------------------------------------------
    # 8) Estandarización de Títulos de Revistas (Canonical Titles)
    # --------------------------------------------------------
    if not combined_df.empty and scimago_map:
        # Usa SCImago para corregir nombres de revistas variantes (ej. "J. Finance" -> "Journal of Finance")
        combined_df = apply_scimago_canonical_titles(combined_df, scimago_map)

    # --------------------------------------------------------
    # 9) Normalización Post-Merge
    # --------------------------------------------------------
    logger.info(f"Applying post-merge normalization (Years: {config.YEAR_START}-{config.YEAR_FINAL})...")
    before_year_filter = len(combined_df)
    
    combined_df, norm_stats = apply_post_merge_normalization(
        combined_df=combined_df,
        scimago_map=scimago_map,
        year_start=config.YEAR_START,
        year_end=config.YEAR_FINAL,
    )
    removed_out_of_years = norm_stats.get("removed_out_of_years", 0)
    
    # [Assertion] Post-Merge filtering conservation
    assert before_year_filter - removed_out_of_years == len(combined_df), "Year filtering conservation failed"
    
    # --------------------------------------------------------
    # 10) Enriquecimiento con Métricas (SCImago SJR, Quartiles)
    # --------------------------------------------------------
    logger.info("Enriching with SCImago metrics...")
    before_enrichment = len(combined_df)
    combined_df = enrich_with_scimago(
        combined_df=combined_df,
        scimago_df=scimago_df,
        fuzzy_threshold=config.SCIMAGO_FUZZY_THRESHOLD
    )
    
    # [Assertion] Enrichment doesn't drop records
    assert len(combined_df) == before_enrichment, "SCImago enrichment unexpectedly dropped/added records"

    # --------------------------------------------------------
    # 11) Guardado de Archivos y Limpieza Final
    # --------------------------------------------------------
    # Última pasada de deduplicación exacta por si acaso
    removed_post_merge = 0
    if not combined_df.empty and {"Title", "Year"}.issubset(combined_df.columns):
        before_final = len(combined_df)
        combined_df["Title"] = combined_df["Title"].astype(str).str.strip()
        combined_df["Year"] = pd.to_numeric(combined_df["Year"], errors="coerce")
        combined_df = combined_df.drop_duplicates(
            subset=["Title", "Year"],
            keep="first"
        ).reset_index(drop=True)
        after_final = len(combined_df)
        removed_post_merge = before_final - after_final
        if removed_post_merge > 0:
            logger.info(f"Final cleanup (exact Title+Year): Removed {removed_post_merge} duplicates.")

    logger.info(f"Saving results to: {paths.results_dir}")
    # Guarda los CSVs finales
    save_outputs(combined_df, duplicated_titles, paths.results_dir)

    # --------------------------------------------------------
    # 12) Reportes Excel
    # --------------------------------------------------------
    logger.info("Generating reports...")
    # Construye tablas resumen para el Excel
    report_tables = build_report_tables(
        original_scopus_count=original_scopus,
        original_wos_count=original_wos,
        scopus_df=scopus_df if not scopus_df.empty else None,
        wos_df=wos_df if not wos_df.empty else None,
        wos_non_repeated=wos_non_repeated if not wos_non_repeated.empty else None,
        df_wos_renombrado=wos_norm if not wos_norm.empty else None,
        combined_df=combined_df if not combined_df.empty else None,
        duplicated_titles=duplicated_titles,
        year_start=config.YEAR_START,
        year_end=config.YEAR_FINAL,
        scopus_stats=scopus_stats,
        wos_stats=wos_stats,
        removed_post_merge=removed_post_merge + removed_out_of_years,
        doi_val_counts=doi_val_counts
    )
    save_report_excel(report_tables, paths.results_dir)

    # --------------------------------------------------------
    # 13) Gráficos
    # --------------------------------------------------------
    # Datos para el gráfico de distribución (qué se quedó y qué se eliminó)
    final_scopus = len(scopus_df) if not scopus_df.empty else 0
    final_wos_count = len(wos_norm) if not wos_norm.empty else 0
    removed_wos = original_wos - (len(wos_non_repeated) if not wos_non_repeated.empty else 0)
    removed_scopus = original_scopus - final_scopus

    # Genera gráfico de pastel o barras de distribución
    plot_distribution(
        final_wos=final_wos_count,
        final_scopus=final_scopus,
        removed_wos=removed_wos,
        removed_scopus=removed_scopus,
        results_dir=paths.results_dir,
        filename="distribution_post_dedup.png",
        show=False  # No mostrar ventana emergente
    )

    # Genera gráficos de tendencias temporales
    plot_raw_trends(
        raw_counts_by_year=report_tables.get("raw_counts_by_year", pd.DataFrame()),
        raw_citations_by_year=report_tables.get("raw_citations_by_year", pd.DataFrame()),
        results_dir=paths.results_dir,
        show=False
    )

    # --------------------------------------------------------
    # Fin del Programa
    # --------------------------------------------------------
    end_time = time.time()
    elapsed = end_time - start_time
    minutes = int(elapsed // 60)
    seconds = elapsed % 60

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully")
    logger.info(f"Total execution time: {minutes} min {seconds:.2f} sec")
    logger.info("=" * 60)


# Punto de entrada estándar de Python ("Main guard")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Si ocurre un error no controlado, lo registramos como crítico antes de salir
        logger.critical(f"Critical Error: {e}", exc_info=True)
        raise
