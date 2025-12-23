# ============================================================
# deduplication.py
#   - Deduplicación cruzada WoS–Scopus
#   - 1) DOI match
#   - 2) Fuzzy title + año ±1
# ============================================================
from __future__ import annotations

from typing import Set

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from ui_messages import info


def cross_deduplicate(scopus_df: pd.DataFrame, wos_df: pd.DataFrame, threshold: int) -> Set[str]:
    """
    Devuelve set(processed_title) duplicados detectados en WoS.
    Requiere:
      - scopus_df['processed_title'], scopus_df['DOI'], scopus_df['Year'] (si existe)
      - wos_df['processed_title'], wos_df['DOI'], wos_df['Publication Year']
    """
    duplicates: Set[str] = set()

    if scopus_df is None or wos_df is None or scopus_df.empty or wos_df.empty:
        return duplicates

    scopus_dois = set(scopus_df.get("DOI", pd.Series([], dtype=str)).values)
    scopus_titles = scopus_df["processed_title"].tolist()

    # (1) DOI match
    for _, wrow in wos_df.iterrows():
        wdoi = wrow.get("DOI", "")
        if wdoi and wdoi in scopus_dois:
            duplicates.add(wrow["processed_title"])

    # (2) Fuzzy title + año ±1
    for _, wrow in wos_df.iterrows():
        wtitle = wrow["processed_title"]
        wdoi = wrow.get("DOI", "")

        if wtitle in duplicates:
            continue

        if wdoi and wdoi in scopus_dois:
            duplicates.add(wtitle)
            continue

        best = process.extractOne(wtitle, scopus_titles, scorer=fuzz.WRatio)
        if not best:
            continue

        _, score, idx = best
        if score < threshold:
            continue

        srow = scopus_df.iloc[idx]

        wyear = pd.to_numeric(wrow.get("Publication Year", np.nan), errors="coerce")
        syear = pd.to_numeric(srow.get("Year", np.nan), errors="coerce")

        if pd.notna(wyear) and pd.notna(syear):
            if abs(int(wyear) - int(syear)) > 1:
                continue

        duplicates.add(wtitle)

    info("Deduplicación cruzada", f"Duplicados cruzados detectados (WoS en Scopus): {len(duplicates)}")
    return duplicates

