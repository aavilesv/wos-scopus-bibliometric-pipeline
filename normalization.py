# ============================================================
# normalization.py
#   - Normaliza WoS → esquema Scopus
#   - Normalización post-merge (años, ISSN, OA, citas, afiliaciones)
# ============================================================
from __future__ import annotations

import re
import pandas as pd
import numpy as np

from ui_messages import warn


def normalize_wos_to_scopus_schema(wos_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # 1) wos_non_repeated: filtra los In_Both == 0
    if "In_Both" in wos_df.columns:
        wos_non_repeated = wos_df[wos_df["In_Both"] == 0].copy()
    else:
        wos_non_repeated = wos_df.copy()

    # 2) wos_norm: renombrado + columnas necesarias
    rename_map = {
        "UT (Unique WOS ID)": "EID",
        "Author(s) ID": "Author(s) ID",
        "Document Type": "Document Type",
        "Language": "Language of Original Document",
        "Author Keywords": "Author Keywords",
        "Keywords Plus": "Index Keywords",
        "Abstract": "Abstract",
        "DOI": "DOI",
        "Author Full Names": "Author full names",
        "Authors": "Authors",
        "Cited Reference Count": "Cited by",
        "Publication Year": "Year",
        "Source Title": "Source title",
        "Article Title": "Title",
        "Addresses": "Authors with affiliations",
        "Open Access Designations": "Open Access",
        "ISSN": "ISSN",
        "Publisher": "Publisher",
        "DOI Link": "Link",
        "Source": "Source",
        "Publication Stage": "Publication Stage",
    }

    df = wos_non_repeated.rename(columns=rename_map)

    necessary_columns = [
        "Publication Stage",
        "Authors",
        "Author(s) ID",
        "Source",
        "EID",
        "Document Type",
        "Language of Original Document",
        "Author Keywords",
        "Index Keywords",
        "Abstract",
        "DOI",
        "Cited by",
        "Year",
        "Source title",
        "Title",
        "Affiliations",
        "ISSN",
        "Publisher",
        "Link",
        "Open Access",
        "Author full names",
        "Scopus_SubjectArea",
        "Authors with affiliations",
        "processed_title",
        "In_Both",
    ]

    final_cols = [c for c in necessary_columns if c in df.columns]
    wos_norm = df[final_cols].copy()

    return wos_non_repeated, wos_norm

# ----------------------------
# Post-merge normalización
# ----------------------------
def _empty(x) -> bool:
    return (pd.isna(x)) or (isinstance(x, str) and x.strip() == "")


def fill_missing_affiliations(row: pd.Series) -> pd.Series:
    affiliations = row.get("Affiliations", None)
    authors_with_aff = row.get("Authors with affiliations", None)

    if _empty(affiliations) and _empty(authors_with_aff):
        return pd.Series([affiliations, authors_with_aff], index=["Affiliations", "Authors with affiliations"])

    if _empty(affiliations):
        affiliations = authors_with_aff
    if _empty(authors_with_aff):
        authors_with_aff = affiliations

    return pd.Series([affiliations, authors_with_aff], index=["Affiliations", "Authors with affiliations"])


def normalize_country(text: str) -> str:
    if not isinstance(text, str):
        return text

    text = re.sub(r'(?i)\b(?:usa|u\.s\.a\.|united states of america|united states)\b', 'United States', text)
    text = re.sub(r'(?i)\b(?:uk|u\.k\.|united kingdom)\b', 'United Kingdom', text)
    text = re.sub(r'(?i)\b(?:united arab emirates)\b', 'United Arab Emirates', text)
    text = re.sub(r'(?i)\brepublic of korea\b', 'South Korea', text)
    text = re.sub(r'(?i)\bpeoples r china\b', 'China', text)
    text = re.sub(r'(?i)\brussian federation\b', 'Russia', text)
    text = re.sub(r'(?i)\bengland\b', 'United Kingdom', text)
    text = re.sub(r'(?i)\bScotland\b', 'United Kingdom', text)
    text = re.sub(r'(?i)\bwales\b', 'United Kingdom', text)
    text = re.sub(r'(?i)\bnorthern ireland\b', 'United Kingdom', text)

    text = re.sub(r'(?i)\bviet\s?nam\b', 'Vietnam', text)

    text = re.sub(r"(?i)\bCôte d'Ivoire\b", "Ivory Coast", text)
    text = re.sub(r"(?i)\bCote d'Ivoire\b", "Ivory Coast", text)
    text = re.sub(r"(?i)\bCote Ivoire\b", "Ivory Coast", text)

    text = re.sub(r"(?i)\bDominican Rep\b", "Dominican Republic", text)
    text = re.sub(r"(?i)\bTrinidad Tobago\b", "Trinidad and Tobago", text)
    text = re.sub(r"(?i)\bTimor Leste\b", "Timor-Leste", text)
    text = re.sub(r"(?i)\bSt Vincent\b", "Saint Vincent and the Grenadines", text)
    text = re.sub(r"(?i)\bGermany \(Democratic Republic, DDR\)\b", "Germany", text)
    text = re.sub(r"(?i)\bSao Tome & Prin\b", "Sao Tome and Principe", text)
    text = re.sub(r"(?i)\bSt Lucia\b", "Saint Lucia", text)
    text = re.sub(r"(?i)\bSt Kitts & Nevi\b", "Saint Kitts and Nevis", text)
    text = re.sub(r"(?i)\bPapua N Guinea\b", "Papua New Guinea", text)
    text = re.sub(r"(?i)\bGuinea Bissau\b", "Guinea-Bissau", text)
    text = re.sub(r"(?i)\bCent Afr Republ\b", "Central African Republic", text)
    text = re.sub(r"(?i)\bCape Verde\b", "Cabo Verde", text)
    text = re.sub(r"(?i)\bBrunei\b", "Brunei Darussalam", text)

    # Se mantiene tu lógica (aunque es riesgosa semánticamente)
    text = re.sub(r"(?i)\bNigeria\b", "Niger", text)

    text = re.sub(r"(?i)\bDEM REP CONGO\b", "Congo", text)
    text = re.sub(r"(?i)\bDemocratic Republic of the Congo\b", "Congo", text)

    text = re.sub(r"(?i)\bTurkiye\b", "Turkey", text)
    text = re.sub(r"(?i)\bSt Martin\b", "Saint Martin", text)
    text = re.sub(r"(?i)\bSaint Martin\b", "Saint Martin", text)

    return text


def process_record_affiliations(record):
    if pd.isna(record):
        return record
    record = str(record)
    record = re.sub(r"\[(.*?)\]", lambda m: m.group(0).replace(",", ""), record)
    record = normalize_country(record)
    return record


def apply_post_merge_normalization(
    combined_df: pd.DataFrame,
    scimago_map: dict,
    year_start: int,
    year_end: int,
) -> pd.DataFrame:
    """
    Aplica lo que hacías después del concat:
      - filtrar años
      - ISSN limpieza
      - canonical title (si scimago_map tiene algo) -> se hace en scimago_utils (se llama desde main)
      - fill affiliations + normalize countries
      - Open Access default
      - Cited by int
      - drop processed_title
    """
    if combined_df is None or combined_df.empty:
        return pd.DataFrame()

    df = combined_df.copy()

    # Year filter
    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df = df[df["Year"].between(year_start, year_end)]

    # ISSN
    if "ISSN" in df.columns:
        df["ISSN"] = (
            df["ISSN"]
            .replace({"": pd.NA})
            .astype(str)
            .str.replace(r"[^0-9X]", "", regex=True)
            .str.upper()
        )

    # Fill affiliations
    if "Affiliations" in df.columns and "Authors with affiliations" in df.columns:
        df[["Affiliations", "Authors with affiliations"]] = df.apply(fill_missing_affiliations, axis=1)

    # Normalize countries
    for col in ["Affiliations", "Authors with affiliations"]:
        if col in df.columns:
            df[col] = df[col].apply(process_record_affiliations)

    # Authors = Author full names
    if "Author full names" in df.columns:
        df["Authors"] = df["Author full names"]

    # Open Access default
    if "Open Access" in df.columns:
        df["Open Access"] = df["Open Access"].fillna("subscription")
        df["Open Access"] = df["Open Access"].replace(r"^\s*$", "subscription", regex=True)

    # Cited by
    if "Cited by" in df.columns:
        df["Cited by"] = pd.to_numeric(df["Cited by"], errors="coerce").fillna(0).astype(int)

    # drop processed_title
    if "processed_title" in df.columns:
        df.drop(columns=["processed_title"], inplace=True)

    return df

