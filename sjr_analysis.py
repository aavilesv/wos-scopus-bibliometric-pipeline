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
      1) ISSN + Year (clipped to available SCImago years)
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
    # Ajuste de Años (para coincidir con el rango de SCImago)
    # --------------------------------------------------------
    # Si SCImago solo tiene de 2013 a 2024, artículos de 2005 se cruzan con 2013.
    min_year = scimago_exp["Year"].min()
    max_year = scimago_exp["Year"].max()
    
    temp_df = combined_df.copy()
    temp_df["Match_Year"] = pd.to_numeric(temp_df["Year"], errors="coerce").fillna(max_year).astype(float).clip(lower=min_year, upper=max_year).astype(int)
    
    # Normalizar ISSN para cruce
    temp_df["Match_ISSN"] = temp_df["ISSN"].astype(str).str.replace("-", "", regex=False).str.strip()
    scimago_exp["Match_ISSN"] = scimago_exp["Issn"].str.replace("-", "", regex=False).str.strip()

    # Preparar DataFrames únicos para evitar explosion de filas (dropped/added records bug)
    scimago_issn = scimago_exp.drop_duplicates(subset=["Match_ISSN", "Year"]).copy()
    scimago_title = scimago_exp.drop_duplicates(subset=["Source title", "Year"]).copy()

    # --------------------------------------------------------
    # 1) Merge por ISSN + Match_Year
    # --------------------------------------------------------
    merged = pd.merge(
        temp_df,
        scimago_issn,
        how="left",
        left_on=["Match_ISSN", "Match_Year"],
        right_on=["Match_ISSN", "Year"],
        suffixes=("", "_scimago")
    )

    # --------------------------------------------------------
    # 2) Fallback fuzzy por título + Match_Year
    # --------------------------------------------------------
    unmatched_mask = merged["Issn"].isna()

    if unmatched_mask.any():
        scimago_titles = scimago_title["Source title"].dropna().astype(str).unique().tolist()
        
        titles_to_match = merged.loc[unmatched_mask, "Source title"]
        matched_titles = titles_to_match.apply(lambda x: _best_title_match(x, scimago_titles, fuzzy_threshold))
        merged.loc[unmatched_mask, "Fuzzy_Title"] = matched_titles
        
        fuzzy_merged = pd.merge(
            merged[unmatched_mask],
            scimago_title,
            how="left",
            left_on=["Fuzzy_Title", "Match_Year"],
            right_on=["Source title", "Year"],
            suffixes=("", "_scimago_fz")
        )
        
        # Renombrar columnas para evitar sufijos innecesarios en el scimago_title original
        for col in scimago_title.columns:
            if col in ["Match_ISSN", "Issn", "Year", "Source title"]:
                continue
            base_col = f"{col}_scimago" if col in combined_df.columns else col
            fz_col = f"{col}_scimago_fz" if col in combined_df.columns else col
            
            if base_col in merged.columns and fz_col in fuzzy_merged.columns:
                merged.loc[unmatched_mask, base_col] = fuzzy_merged[fz_col].values

    # --------------------------------------------------------
    # Consolidar columnas SCImago (_scimago → finales)
    # --------------------------------------------------------
    SCIMAGO_MERGE_MAP = [
        "SJR", "SJR Best Quartile", "H index", "Country", "Region", 
        "Publisher", "Categories", "Areas"
    ]
    
    for final_col in SCIMAGO_MERGE_MAP:
        sc_col = f"{final_col}_scimago"
        
        if final_col not in combined_df.columns:
            # Si la columna no existía en el combined_df original, la agregamos desde sc_col
            # si existió un conflicto, de lo contrario la columna ya está en merged con nombre final_col
            if sc_col in merged.columns:
                merged[final_col] = merged[sc_col]
        else:
            # Si existía, actualizamos valores nulos con los traídos de SCImago
            if sc_col in merged.columns:
                merged[final_col] = merged[final_col].combine_first(merged[sc_col])

    # --------------------------------------------------------
    # Eliminar columnas técnicas intermedios
    # --------------------------------------------------------
    cols_to_drop = [c for c in merged.columns if c.endswith("_scimago") or c.endswith("_scimago_fz")]
    cols_to_drop.extend(["Match_Year", "Match_ISSN", "Fuzzy_Title", "Issn", "Year_scimago"])
    
    # Remove 'Year' column brought from scimago merge if it overrode something, but _scimago took care of that
    # merged still has 'Year' which was the original combined_df Year.
    enriched = merged.drop(columns=[c for c in cols_to_drop if c in merged.columns])
    
    return enriched
