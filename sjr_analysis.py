# ============================================================
# sjr_analysis.py
#   - Enriquecimiento del dataset final con SCImago
#   - Matching por ISSN + fuzzy Source title
#   - Consolida columnas SCImago y elimina columnas técnicas
#   - NO guarda archivos (solo devuelve DataFrame)
# ============================================================

from __future__ import annotations
import re
import pandas as pd
from rapidfuzz import process, fuzz


# ------------------------------------------------------------
# Utilidades internas
# ------------------------------------------------------------
def _clean_categories(cat_str: str) -> str:
    """Limpia categorías SCImago eliminando paréntesis y normalizando separadores."""
    if not isinstance(cat_str, str):
        return ""
    no_par = re.sub(r"\([^)]*\)", "", cat_str)
    parts = [p.strip() for p in no_par.split(";") if p.strip()]
    return "; ".join(parts)


def _best_title_match(
    title: str,
    choices: list[str],
    threshold: int = 90
) -> str | None:
    """Devuelve el mejor match fuzzy de título si supera el umbral."""
    if not isinstance(title, str) or not title.strip():
        return None

    result = process.extractOne(
        title.lower(),
        choices,
        scorer=fuzz.WRatio
    )
    if result:
        best, score, _ = result
        if score >= threshold:
            return best
    return None


def _deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas duplicadas manteniendo la primera ocurrencia.
    Previene InvalidIndexError tras merges múltiples.
    """
    return df.loc[:, ~df.columns.duplicated()]


# ------------------------------------------------------------
# Enriquecimiento principal
# ------------------------------------------------------------
def enrich_with_scimago(
    combined_df: pd.DataFrame,
    scimago_df: pd.DataFrame,
    fuzzy_threshold: int = 90
) -> pd.DataFrame:
    """
    Enrich final Scopus+WoS dataset with SCImago metadata.

    Matching order:
      1) ISSN + Year
      2) Fuzzy Source title + Year (fallback)
    """

    if combined_df is None or combined_df.empty:
        return combined_df

    if scimago_df is None or scimago_df.empty:
        return combined_df

    # --------------------------------------------------------
    # Preparación SCImago
    # --------------------------------------------------------
    scimago = scimago_df.copy()

    # Normalizar nombres y títulos
    scimago = scimago.rename(columns={"Title": "Source title"})
    scimago["Source title"] = (
        scimago["Source title"]
        .astype(str)
        .str.replace(r"\([^)]*\)", "", regex=True)
        .str.strip()
    )

    if "Categories" in scimago.columns:
        scimago["Categories"] = scimago["Categories"].apply(_clean_categories)

    # Expandir ISSN (uno por fila)
    scimago_exp = scimago.assign(
        Issn=scimago["Issn"].astype(str).str.split(",")
    ).explode("Issn")
    scimago_exp["Issn"] = scimago_exp["Issn"].astype(str).str.strip()

    # --------------------------------------------------------
    # 1) Merge por ISSN + Year
    # --------------------------------------------------------
    by_issn = pd.merge(
        combined_df,
        scimago_exp,
        how="left",
        left_on=["ISSN", "Year"],
        right_on=["Issn", "Year"],
        suffixes=("", "_scimago")
    )

    # --------------------------------------------------------
    # 2) Fallback fuzzy por título (sin ISSN)
    # --------------------------------------------------------
    no_match = by_issn[by_issn["Issn"].isna()].copy()

    scimago_titles = (
        scimago_exp["Source title"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    no_match["Source title"] = no_match["Source title"].apply(
        lambda x: _best_title_match(x, scimago_titles, fuzzy_threshold)
    )

    by_title = pd.merge(
        no_match.dropna(subset=["Source title"]),
        scimago_exp,
        how="left",
        on=["Source title", "Year"],
        suffixes=("", "_scimago")
    )

    # --------------------------------------------------------
    # Conciliación segura (evita columnas duplicadas)
    # --------------------------------------------------------
    matched_issn = _deduplicate_columns(
        by_issn[~by_issn["Issn"].isna()]
    )
    matched_title = _deduplicate_columns(
        by_title[~by_title["Issn"].isna()]
    )

    matched = pd.concat(
        [matched_issn, matched_title],
        ignore_index=True
    )

    unmatched = combined_df.loc[
        ~combined_df.index.isin(matched.index)
    ]

    enriched = pd.concat(
        [matched, unmatched],
        ignore_index=True
    )

    enriched = _deduplicate_columns(enriched)

    # --------------------------------------------------------
    # Consolidar columnas SCImago (_scimago → finales)
    # --------------------------------------------------------
    SCIMAGO_MERGE_MAP = {
        "SJR": "SJR_scimago",
        "SJR Best Quartile": "SJR Best Quartile_scimago",
        "H index": "H index_scimago",
        "Country": "Country_scimago",
        "Region": "Region_scimago",
        "Publisher": "Publisher_scimago",
        "Categories": "Categories_scimago",
        "Areas": "Areas_scimago",
    }

    for final_col, sc_col in SCIMAGO_MERGE_MAP.items():
        if sc_col in enriched.columns:
            if final_col not in enriched.columns:
                enriched[final_col] = None

            enriched[final_col] = enriched[final_col].combine_first(
                enriched[sc_col]
            )

    # --------------------------------------------------------
    # Eliminar columnas técnicas de merge (_scimago)
    # --------------------------------------------------------
    cols_to_drop = [c for c in enriched.columns if c.endswith("_scimago")]
    if cols_to_drop:
        enriched.drop(columns=cols_to_drop, inplace=True)

    return enriched
