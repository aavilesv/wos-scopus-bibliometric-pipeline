# ============================================================
# reporting.py
#   - Guarda outputs principales (CSV)
#   - Genera y guarda reportes (Excel) de métricas/tablas
#   - Genera gráficos: mostrar + guardar PNG
# ============================================================
from __future__ import annotations

from pathlib import Path
from typing import Set, Optional, Dict, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from ui_messages import info, warn


# ------------------------------------------------------------
# CSV con tildes OK en Excel (UTF-8 con BOM)
# ------------------------------------------------------------
def _save_csv_utf8sig(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


# ------------------------------------------------------------
# Excel reporte (solo tablas/resúmenes)
# ------------------------------------------------------------
def _save_report_excel(sheets: Dict[str, pd.DataFrame], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if df is None:
                df = pd.DataFrame()
            df.to_excel(writer, sheet_name=name[:31], index=False)

    # Formato simple (si falla, igual queda el xlsx)
    try:
        from openpyxl import load_workbook
        from openpyxl.styles import Font, Alignment
        from openpyxl.utils import get_column_letter

        wb = load_workbook(path)
        header_font = Font(bold=True)
        header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell_align = Alignment(horizontal="left", vertical="top", wrap_text=True)

        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

            for cell in ws[1]:
                cell.font = header_font
                cell.alignment = header_align

            max_cap = 60
            for col_idx, col in enumerate(ws.iter_cols(min_row=1, max_row=min(ws.max_row, 80)), start=1):
                lengths = []
                for cell in col:
                    val = "" if cell.value is None else str(cell.value)
                    lengths.append(len(val))
                    if cell.row > 1:
                        cell.alignment = cell_align

                width = min((max(lengths) + 2) if lengths else 12, max_cap)
                ws.column_dimensions[get_column_letter(col_idx)].width = max(12, width)

        wb.save(path)
    except Exception as e:
        warn("Formato Excel", f"No se pudo aplicar formato avanzado al reporte.\nDetalle: {e}")


# ------------------------------------------------------------
# 1) Guardado de outputs principales
# ------------------------------------------------------------
def save_outputs(
    combined_df: pd.DataFrame,
    duplicated_titles: Set[str],
    results_dir: Path
) -> Tuple[Path, Path]:
    """
    Guarda:
      - datawos_scopus.csv
      - datawos_scopus_repeatedstitles.csv
    Retorna rutas guardadas.
    """
    results_dir.mkdir(parents=True, exist_ok=True)

    out_main = results_dir / "datawos_scopus.csv"
    out_dups = results_dir / "datawos_scopus_repeatedstitles.csv"

    if combined_df is None:
        combined_df = pd.DataFrame()

    dups_df = pd.DataFrame(sorted(list(duplicated_titles)), columns=["Título Repetido"])

    _save_csv_utf8sig(combined_df, out_main)
    _save_csv_utf8sig(dups_df, out_dups)

    info(
        "Archivos guardados",
        "Se guardaron los outputs principales:\n\n"
        f"- {out_main}\n"
        f"- {out_dups}"
    )

    return out_main, out_dups


# ------------------------------------------------------------
# 2) Construcción de tablas de reporte (lo que antes era print)
# ------------------------------------------------------------
def build_report_tables(
    *,
    original_scopus_count: int,
    original_wos_count: int,
    scopus_df: Optional[pd.DataFrame],
    wos_df: Optional[pd.DataFrame],
    wos_non_repeated: Optional[pd.DataFrame],
    df_wos_renombrado: Optional[pd.DataFrame],
    combined_df: Optional[pd.DataFrame],
    duplicated_titles: Set[str],
    year_start: int,
    year_end: int,
) -> Dict[str, pd.DataFrame]:
    """
    Devuelve un dict {sheet_name: df} para exportar a Excel.
    Replica la lógica de tus prints, pero en tablas.
    """

    # --- Conteos base ---
    total_loaded = int(original_scopus_count + original_wos_count)
    omitted_papers = 0
    after_omission_total = total_loaded

    scopus_unique_count = int(len(scopus_df)) if scopus_df is not None else 0

    # En tu script: removed_wos = original_wos_count - len(wos_non_repeated)
    wos_non_rep_count = int(len(wos_non_repeated)) if wos_non_repeated is not None else 0
    removed_wos = int(original_wos_count - wos_non_rep_count) if original_wos_count else 0

    removed_scopus = int(original_scopus_count - scopus_unique_count) if original_scopus_count else 0

    duplicated_papers_found = int(len(duplicated_titles))

    final_total = int(len(combined_df)) if combined_df is not None else 0
    final_wos_count = int(len(df_wos_renombrado)) if df_wos_renombrado is not None else 0
    final_scopus_count = scopus_unique_count

    percentage_wos_loaded = (original_wos_count / total_loaded * 100) if total_loaded else 0
    percentage_scopus_loaded = (original_scopus_count / total_loaded * 100) if total_loaded else 0

    final_wos_percentage = (final_wos_count / final_total * 100) if final_total else 0
    final_scopus_percentage = (final_scopus_count / final_total * 100) if final_total else 0

    removed_wos_percentage = (removed_wos / original_wos_count * 100) if original_wos_count else 0
    removed_scopus_percentage = (removed_scopus / original_scopus_count * 100) if original_scopus_count else 0
    duplicated_percentage = (duplicated_papers_found / total_loaded * 100) if total_loaded else 0

    stats_summary = pd.DataFrame(
        [
            ("Loaded papers", total_loaded),
            ("Omitted papers by document type", omitted_papers),
            ("Total after omission", after_omission_total),
            ("Loaded WoS", original_wos_count),
            ("Loaded WoS (%)", round(percentage_wos_loaded, 1)),
            ("Loaded Scopus", original_scopus_count),
            ("Loaded Scopus (%)", round(percentage_scopus_loaded, 1)),
            ("Duplicated papers found", duplicated_papers_found),
            ("Duplicated papers found (%)", round(duplicated_percentage, 1)),
            ("Removed duplicated WoS", removed_wos),
            ("Removed duplicated WoS (%)", round(removed_wos_percentage, 1)),
            ("Removed duplicated Scopus", removed_scopus),
            ("Removed duplicated Scopus (%)", round(removed_scopus_percentage, 1)),
            ("Total after duplicates removal", final_total),
            ("Final WoS", final_wos_count),
            ("Final WoS (%)", round(final_wos_percentage, 1)),
            ("Final Scopus", final_scopus_count),
            ("Final Scopus (%)", round(final_scopus_percentage, 1)),
        ],
        columns=["Metric", "Value"]
    )

    # --- Distribución kept/removed por fuente (para gráfico y tabla) ---
    dedup_distribution = pd.DataFrame(
        {
            "Source": ["WoS", "Scopus"],
            "Kept": [final_wos_count, final_scopus_count],
            "Removed": [removed_wos, removed_scopus],
        }
    )
    dedup_distribution["Total"] = dedup_distribution["Kept"] + dedup_distribution["Removed"]
    dedup_distribution["Kept (%)"] = np.where(
        dedup_distribution["Total"] > 0,
        (dedup_distribution["Kept"] / dedup_distribution["Total"] * 100).round(1),
        0.0
    )
    dedup_distribution["Removed (%)"] = np.where(
        dedup_distribution["Total"] > 0,
        (dedup_distribution["Removed"] / dedup_distribution["Total"] * 100).round(1),
        0.0
    )

    # --- Tablas RAW por año (antes de dedup) ---
    raw_counts = pd.DataFrame()
    raw_citations = pd.DataFrame()

    try:
        if scopus_df is not None and "Year" in scopus_df.columns:
            sc_year = pd.to_numeric(scopus_df["Year"], errors="coerce")
            mask_sc = sc_year.between(year_start, year_end)
            scopus_yearly = scopus_df.loc[mask_sc].assign(Year=sc_year[mask_sc]).groupby("Year").size()
        else:
            scopus_yearly = pd.Series(dtype=int)

        # wos_df tiene Publication Year en tu export original
        if wos_df is not None and "Publication Year" in wos_df.columns:
            wo_year = pd.to_numeric(wos_df["Publication Year"], errors="coerce")
            mask_wo = wo_year.between(year_start, year_end)
            wos_yearly = wos_df.loc[mask_wo].assign(Year=wo_year[mask_wo]).groupby("Year").size()
        else:
            wos_yearly = pd.Series(dtype=int)

        raw_counts = pd.DataFrame({"WoS": wos_yearly, "Scopus": scopus_yearly}).fillna(0).astype(int)
        raw_counts = raw_counts.reset_index().rename(columns={"index": "Year"})
        raw_counts["Total Articles Raw"] = raw_counts["WoS"] + raw_counts["Scopus"]

        # Citas RAW
        if scopus_df is not None and "Cited by" in scopus_df.columns and "Year" in scopus_df.columns:
            sc_year = pd.to_numeric(scopus_df["Year"], errors="coerce")
            mask_sc = sc_year.between(year_start, year_end)
            sc_cites = pd.to_numeric(scopus_df.loc[mask_sc, "Cited by"], errors="coerce").fillna(0)
            scopus_cites = pd.DataFrame({"Year": sc_year[mask_sc], "Cited by": sc_cites}).groupby("Year")["Cited by"].sum()
        else:
            scopus_cites = pd.Series(dtype=int)

        if wos_df is not None and "Cited Reference Count" in wos_df.columns and "Publication Year" in wos_df.columns:
            wo_year = pd.to_numeric(wos_df["Publication Year"], errors="coerce")
            mask_wo = wo_year.between(year_start, year_end)
            wo_cites = pd.to_numeric(wos_df.loc[mask_wo, "Cited Reference Count"], errors="coerce").fillna(0)
            wos_cites = pd.DataFrame({"Year": wo_year[mask_wo], "Cited Reference Count": wo_cites}).groupby("Year")["Cited Reference Count"].sum()
        else:
            wos_cites = pd.Series(dtype=int)

        raw_citations = pd.DataFrame({"WoS Citations": wos_cites, "Scopus Citations": scopus_cites}).fillna(0).astype(int)
        raw_citations = raw_citations.reset_index().rename(columns={"index": "Year"})
        raw_citations["Total Citations Raw"] = raw_citations["WoS Citations"] + raw_citations["Scopus Citations"]

    except Exception as e:
        warn("Reporte RAW por año", f"No se pudieron construir tablas RAW por año.\nDetalle: {e}")

    # --- Tabla Document Type por año (si existe) ---
    doc_types_by_year = pd.DataFrame()
    try:
        if combined_df is not None and {"Year", "Document Type"}.issubset(set(combined_df.columns)):
            df = combined_df[["Year", "Document Type"]].copy()
            df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
            df = df.dropna(subset=["Year"])
            df = df[df["Year"].between(year_start, year_end)]

            df["Document Type"] = df["Document Type"].astype(str).str.split(";")
            df["Document Type"] = df["Document Type"].apply(
                lambda x: [i.strip() for i in x if i.strip()] if isinstance(x, list) else []
            )
            exploded = df.explode("Document Type")
            exploded = exploded[exploded["Document Type"].astype(str).str.strip() != ""]

            pivot = exploded.groupby(["Year", "Document Type"]).size().unstack(fill_value=0)
            doc_types_by_year = pivot.reset_index()
    except Exception as e:
        warn("Reporte Document Type", f"No se pudo construir tabla de Document Type por año.\nDetalle: {e}")

    return {
        "stats_summary": stats_summary,
        "dedup_distribution": dedup_distribution,
        "raw_counts_by_year": raw_counts,
        "raw_citations_by_year": raw_citations,
        "doc_types_by_year": doc_types_by_year,
    }


def save_report_excel(report_tables: Dict[str, pd.DataFrame], results_dir: Path) -> Path:
    """
    Guarda un solo Excel de reporte con varias hojas.
    """
    results_dir.mkdir(parents=True, exist_ok=True)
    out = results_dir / "report.xlsx"
    _save_report_excel(report_tables, out)

    info("Reporte Excel", f"Se guardó el reporte en Excel:\n{out}")
    return out


# ------------------------------------------------------------
# 3) Gráfico distribución kept/removed (mostrar + guardar)
# ------------------------------------------------------------
def plot_distribution(
    final_wos: int,
    final_scopus: int,
    removed_wos: int,
    removed_scopus: int,
    results_dir: Optional[Path] = None,
    filename: str = "distribution_post_dedup.png",
    show: bool = True,
    dpi: int = 300,
) -> Optional[Path]:
    sources = ["WoS", "Scopus"]
    kept = [final_wos, final_scopus]
    removed = [removed_wos, removed_scopus]

    totals = np.array(kept) + np.array(removed)
    order = np.argsort(totals)[::-1]

    sources = [sources[i] for i in order]
    kept = [kept[i] for i in order]
    removed = [removed[i] for i in order]
    totals = (np.array(kept) + np.array(removed)).astype(float)

    pct_kept = np.where(totals > 0, np.array(kept) / totals * 100, 0.0)
    pct_removed = np.where(totals > 0, np.array(removed) / totals * 100, 0.0)

    fig, ax = plt.subplots(figsize=(8, 4))
    bars_kept = ax.barh(sources, kept, label="Kept")
    bars_removed = ax.barh(sources, removed, left=kept, label="Removed")

    # etiquetas (como tu script)
    for i, (b1, b2) in enumerate(zip(bars_kept, bars_removed)):
        w1 = b1.get_width()
        ax.text(
            w1 / 2 if w1 else 0,
            b1.get_y() + b1.get_height() / 2,
            f"{kept[i]}\n({pct_kept[i]:.1f}%)",
            va="center",
            ha="center" if w1 else "left",
        )

        w2 = b2.get_width()
        if w2 > 0:
            ax.text(
                kept[i] + w2 / 2,
                b2.get_y() + b2.get_height() / 2,
                f"{removed[i]}\n({pct_removed[i]:.1f}%)",
                va="center",
                ha="center",
            )

    ax.invert_yaxis()
    ax.set_title(
        "Post-deduplication Distribution of Bibliometric Records\nfrom Scopus and Web of Science",
        weight="bold",
        pad=12,
    )
    ax.set_xlabel("Number of Articles")
    ax.legend(loc="lower right")
    ax.grid(axis="x", linestyle="--", alpha=0.5)
    plt.tight_layout()

    saved_path = None
    if results_dir is not None:
        results_dir.mkdir(parents=True, exist_ok=True)
        saved_path = results_dir / filename
        fig.savefig(saved_path, dpi=dpi, bbox_inches="tight")
        info("Gráfico guardado", f"El gráfico se guardó en:\n{saved_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)

    return saved_path


# ------------------------------------------------------------
# 4) Gráficos RAW por año (mostrar + guardar)
# ------------------------------------------------------------
def plot_raw_trends(
    raw_counts_by_year: pd.DataFrame,
    raw_citations_by_year: pd.DataFrame,
    results_dir: Optional[Path] = None,
    show: bool = True,
    dpi: int = 300,
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Guarda (opcional) y muestra:
      - raw_articles_by_year.png
      - raw_citations_by_year.png
    """
    saved_counts = None
    saved_cites = None

    # --- Artículos por año ---
    if raw_counts_by_year is not None and not raw_counts_by_year.empty and "Year" in raw_counts_by_year.columns:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(raw_counts_by_year["Year"], raw_counts_by_year.get("WoS", 0), marker="o", label="WoS Articles")
        ax.plot(raw_counts_by_year["Year"], raw_counts_by_year.get("Scopus", 0), marker="s", label="Scopus Articles")
        ax.plot(raw_counts_by_year["Year"], raw_counts_by_year.get("Total Articles Raw", 0), marker="^", label="Total Articles")
        ax.set_title("Annual evolution of articles (RAW data before deduplication)", weight="bold", pad=12)
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of articles")
        ax.legend(loc="upper left")
        ax.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()

        if results_dir is not None:
            results_dir.mkdir(parents=True, exist_ok=True)
            saved_counts = results_dir / "raw_articles_by_year.png"
            fig.savefig(saved_counts, dpi=dpi, bbox_inches="tight")
            info("Gráfico guardado", f"RAW artículos por año:\n{saved_counts}")

        if show:
            plt.show()
        else:
            plt.close(fig)

    # --- Citas por año ---
    if raw_citations_by_year is not None and not raw_citations_by_year.empty and "Year" in raw_citations_by_year.columns:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(raw_citations_by_year["Year"], raw_citations_by_year.get("WoS Citations", 0), marker="o", label="WoS Citations")
        ax.plot(raw_citations_by_year["Year"], raw_citations_by_year.get("Scopus Citations", 0), marker="s", label="Scopus Citations")
        ax.plot(raw_citations_by_year["Year"], raw_citations_by_year.get("Total Citations Raw", 0), marker="^", label="Total Citations")
        ax.set_title("Annual evolution of citations (RAW data before deduplication)", weight="bold", pad=12)
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of citations")
        ax.legend(loc="upper left")
        ax.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()

        if results_dir is not None:
            results_dir.mkdir(parents=True, exist_ok=True)
            saved_cites = results_dir / "raw_citations_by_year.png"
            fig.savefig(saved_cites, dpi=dpi, bbox_inches="tight")
            info("Gráfico guardado", f"RAW citas por año:\n{saved_cites}")

        if show:
            plt.show()
        else:
            plt.close(fig)

    return saved_counts, saved_cites
