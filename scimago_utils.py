# ============================================================
# scimago_utils.py
#   - Lee SCImago
#   - Construye mapa ISSN -> Title canónico
#   - Asigna Source title canónico (tu lógica)
# ============================================================
from __future__ import annotations

import re
from typing import Dict, Optional

import pandas as pd
from rapidfuzz import fuzz, process

from ui_messages import warn


def load_scimago_if_exists(paths) -> Optional[pd.DataFrame]:
    """
    paths: PipelinePaths (de file_validation)
    Lee SCImago si existe, sep=';'
    """
    if not paths.scimago_file.exists():
        return None

    try:
        return pd.read_csv(paths.scimago_file, sep=";")
    except Exception as e:
        warn("SCImago no se pudo leer", f"No se pudo leer SCImago.\nSe continúa sin SCImago.\n\nDetalle: {e}")
        return None


def build_scimago_map(scimago_df: pd.DataFrame) -> Dict[str, str]:
    """
    Expande Issn separadas por coma, y arma dict Issn -> Title (first).
    """
    if scimago_df is None or scimago_df.empty:
        return {}

    if "Issn" not in scimago_df.columns or "Title" not in scimago_df.columns:
        warn("SCImago inválido", "SCImago no tiene columnas 'Issn' y 'Title'. Se omitirá.")
        return {}

    exp = scimago_df.assign(Issn=scimago_df["Issn"].astype(str).str.split(",")).explode("Issn")
    exp["Issn"] = exp["Issn"].astype(str).str.strip()
    return exp.groupby("Issn")["Title"].first().to_dict()


def safe_text(x) -> str:
    if isinstance(x, str):
        return x
    if pd.isna(x):
        return ""
    return str(x)


def assign_canonical_title_row(row: pd.Series, scimago_map: Dict[str, str]) -> str:
    issn = safe_text(row.get("ISSN", "")).strip()
    src = safe_text(row.get("Source", "")).strip().lower()
    orig = safe_text(row.get("Source title", "")).strip()

    # 1) Si hay ISSN y existe en SCImago y NO es Scopus -> usar SCImago
    if issn and (issn in scimago_map) and (src != "scopus"):
        cand = safe_text(scimago_map.get(issn, "")).strip()
        if cand:
            return re.sub(r"\([^)]*\)", "", cand).strip()

    # 2) Si NO hay ISSN, fuzzy vs títulos SCImago (NO Scopus)
    if (not issn) and (src != "scopus") and orig and scimago_map:
        best = process.extractOne(orig, list(scimago_map.values()), scorer=fuzz.token_sort_ratio)
        if best:
            best_title, score, _ = best
            if isinstance(score, (int, float)) and score > 90 and isinstance(best_title, str):
                return re.sub(r"\([^)]*\)", "", best_title).strip()

    # 3) Default: el original sin paréntesis
    return re.sub(r"\([^)]*\)", "", orig).strip() if orig else orig


def apply_scimago_canonical_titles(df: pd.DataFrame, scimago_map: Dict[str, str]) -> pd.DataFrame:
    """
    Aplica Source title canónico si hay scimago_map.
    """
    if df is None or df.empty or not scimago_map:
        return df

    if "Source title" not in df.columns:
        return df

    # asegurar tipos
    for col in ["ISSN", "Source", "Source title"]:
        if col in df.columns:
            df[col] = df[col].astype(object).where(~df[col].isna(), None)

    if "Source" in df.columns:
        df["Source"] = df["Source"].fillna("unknown")

    df["Source title"] = df.apply(lambda r: assign_canonical_title_row(r, scimago_map), axis=1)
    return df

