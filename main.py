# ============================================================
# main.py  (CORREGIDO + CONSISTENTE con tu script original)
# ============================================================
from __future__ import annotations

import pandas as pd

from file_validation import build_default_paths, scan_inputs, validate_or_stop
from ui_messages import info, error

from loaders import load_merge_scopus, load_merge_wos
from deduplication import cross_deduplicate

# IMPORTANTE:
# normalize_wos_to_scopus_schema debe devolver (wos_non_repeated, wos_norm)
from normalization import normalize_wos_to_scopus_schema, apply_post_merge_normalization

from scimago_utils import load_scimago_if_exists, build_scimago_map, apply_scimago_canonical_titles

from reporting import (
    save_outputs,
    build_report_tables,
    save_report_excel,
    plot_distribution,
    plot_raw_trends
)

# ============================================================
# CONFIG
# ============================================================
YEAR_START = 2015
YEAR_FINAL = 2025
FUZZY_THRESHOLD = 85


def main() -> None:
    # 1) rutas + validación (early exit)
    paths = build_default_paths()
    inv = scan_inputs(paths)

    if not validate_or_stop(paths, inv):
        return

    has_scopus = len(inv.scopus_files) > 0
    has_wos = len(inv.wos_files) > 0

    # 2) SCImago
    scimago_df = load_scimago_if_exists(paths)
    scimago_map = build_scimago_map(scimago_df) if scimago_df is not None else {}

    # 3) merge por fuente + dedup interno
    scopus_df = pd.DataFrame()
    wos_df = pd.DataFrame()
    original_scopus = 0
    original_wos = 0

    if has_scopus:
        scopus_df, original_scopus = load_merge_scopus(inv.scopus_files)

    if has_wos:
        wos_df, original_wos = load_merge_wos(inv.wos_files)

    # 4) deduplicación cruzada
    duplicated_titles = set()
    if has_scopus and has_wos and (not scopus_df.empty) and (not wos_df.empty):
        duplicated_titles = cross_deduplicate(
            scopus_df=scopus_df,
            wos_df=wos_df,
            threshold=FUZZY_THRESHOLD
        )

    # 5) marcar duplicados (In_Both)
    if has_wos and not wos_df.empty:
        wos_df["In_Both"] = wos_df["processed_title"].isin(duplicated_titles).astype(int)

    if has_scopus and not scopus_df.empty:
        scopus_df["In_Both"] = scopus_df["processed_title"].isin(duplicated_titles).astype(int)

    # 6) Normalizar WoS → esquema Scopus
    #    (y producir wos_non_repeated para métricas removed_wos como tu script original)
    wos_non_repeated = pd.DataFrame()
    wos_norm = pd.DataFrame()

    if has_wos and not wos_df.empty:
        # ESTA FUNCIÓN DEBE SER tuple: (wos_non_repeated, wos_norm)
        wos_non_repeated, wos_norm = normalize_wos_to_scopus_schema(wos_df)

    # 7) Combinar
    parts = []
    if has_scopus and not scopus_df.empty:
        parts.append(scopus_df)
    if has_wos and not wos_norm.empty:
        parts.append(wos_norm)

    combined_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # 8) SCImago canonical titles (si hay mapa)
    if not combined_df.empty and scimago_map:
        combined_df = apply_scimago_canonical_titles(combined_df, scimago_map)

    # 9) normalización post-merge (años, ISSN, afiliaciones, OA, citas, drop processed_title, etc.)
    combined_df = apply_post_merge_normalization(
        combined_df=combined_df,
        scimago_map=scimago_map,
        year_start=YEAR_START,
        year_end=YEAR_FINAL,
    )

    # 10) Guardar outputs principales
    save_outputs(combined_df, duplicated_titles, paths.results_dir)

    # ============================================================
    # 11) Reporte Excel + gráficos (mostrar + guardar)
    #     (esto reemplaza tus prints por tablas en Excel)
    # ============================================================
    report_tables = build_report_tables(
        original_scopus_count=original_scopus,
        original_wos_count=original_wos,
        scopus_df=scopus_df if not scopus_df.empty else None,
        wos_df=wos_df if not wos_df.empty else None,
        wos_non_repeated=wos_non_repeated if not wos_non_repeated.empty else None,
        df_wos_renombrado=wos_norm if not wos_norm.empty else None,
        combined_df=combined_df if not combined_df.empty else None,
        duplicated_titles=duplicated_titles,
        year_start=YEAR_START,
        year_end=YEAR_FINAL
    )
    save_report_excel(report_tables, paths.results_dir)

    # 12) Gráfico distribución (Kept/Removed) con tu lógica REAL:
    # removed_wos = original_wos - len(wos_non_repeated)
    # removed_scopus = original_scopus - len(scopus_df)  (dedup interno)
    final_scopus = len(scopus_df) if not scopus_df.empty else 0
    final_wos = len(wos_norm) if not wos_norm.empty else 0

    removed_wos = original_wos - (len(wos_non_repeated) if not wos_non_repeated.empty else 0)
    removed_scopus = original_scopus - final_scopus

    plot_distribution(
        final_wos=final_wos,
        final_scopus=final_scopus,
        removed_wos=removed_wos,
        removed_scopus=removed_scopus,
        results_dir=paths.results_dir,
        filename="distribution_post_dedup.png",
        show=True
    )

    # 13) RAW trends (antes de dedup) mostrar + guardar
    plot_raw_trends(
        raw_counts_by_year=report_tables.get("raw_counts_by_year", pd.DataFrame()),
        raw_citations_by_year=report_tables.get("raw_citations_by_year", pd.DataFrame()),
        results_dir=paths.results_dir,
        show=True
    )

    info(
        "Proceso completado",
        "El pipeline finalizó correctamente.\n\n"
        f"Resultados:\n{paths.results_dir}\n\n"
        "Se generaron:\n"
        "- datawos_scopus.csv\n"
        "- datawos_scopus_repeatedstitles.csv\n"
        "- report.xlsx\n"
        "- distribution_post_dedup.png\n"
        "- raw_articles_by_year.png\n"
        "- raw_citations_by_year.png"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error("Error crítico", f"El proceso se detuvo por un error inesperado:\n\n{e}")
        raise
