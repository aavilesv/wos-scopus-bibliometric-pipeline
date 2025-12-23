# ============================================================
# loaders.py
#   - Carga y merge por fuente (ANTES del dedup cruzado)
#   - Preprocesamiento de títulos (spaCy)
#   - Limpiezas base Scopus/WoS (tu lógica)
# ============================================================
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import spacy
from spacy.lang.en.stop_words import STOP_WORDS

from ui_messages import info, warn, error


# ----------------------------
# spaCy (carga una sola vez)
# ----------------------------
def load_spacy_model(model_name: str = "en_core_web_lg"):
    try:
        return spacy.load(model_name)
    except Exception as e:
        error(
            "spaCy no disponible",
            f"No se pudo cargar el modelo '{model_name}'.\n\n"
            f"Solución:\n"
            f"1) python -m spacy download {model_name}\n"
            f"2) Reintentar.\n\n"
            f"Detalle: {e}",
        )
        raise


NLP = load_spacy_model()


# ----------------------------
# Utilidades (tu lógica)
# ----------------------------
def preprocess_title(title) -> str:
    """
    Tu preprocesamiento:
      - remove non letters
      - lower
      - remove accents
      - stopwords
      - lemmatize
    """
    if not isinstance(title, str):
        return ""

    title = re.sub(r"[^a-zA-Z\s]", "", title)
    title = title.lower()
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("utf-8", "ignore")

    words = title.split()
    filtered_words = [w for w in words if w not in STOP_WORDS]
    title = " ".join(filtered_words)

    title = re.sub(r"\s+", " ", title).strip()

    doc = NLP(title)
    lemmas = [
        token.lemma_ if token.lemma_ != "-PRON-" else token.text
        for token in doc
        if not (token.is_stop or token.is_punct or token.is_space)
    ]
    return " ".join(lemmas)


def clean_data_author_full_names(cat_str):
    """
    Tu clean_data para Scopus: remove parentesis + split ';' + trim
    """
    if pd.isna(cat_str):
        return np.nan
    cat_str = str(cat_str)
    no_par = re.sub(r"\([^)]*\)", "", cat_str)
    parts = [p.strip() for p in no_par.split(";") if p.strip()]
    return "; ".join(parts)


def normalize_document_type(doc_type):
    """
    Tu normalización de tipo documento:
      - Proceedings Paper -> Conference paper
      - si contiene conference paper -> Conference Paper
      - normaliza ';'
    """
    if pd.isna(doc_type):
        return np.nan
    doc_type = str(doc_type)

    doc_type = doc_type.replace("Proceedings Paper", "Conference paper")
    if "conference paper" in doc_type.lower():
        doc_type = "Conference Paper"

    doc_type = re.sub(r"\s*;\s*", "; ", doc_type).strip()
    return doc_type


# ----------------------------
# Loaders por fuente
# ----------------------------
def load_merge_scopus(scopus_files: List[Path]) -> Tuple[pd.DataFrame, int]:
    """
    Une múltiples CSV de Scopus en uno.
    Aplica tu limpieza clave y genera processed_title.
    Dedup interno por processed_title (y DOI si existe) para evitar ruido.
    Retorna: (df_scopus_merge, original_total_rows)
    """
    if not scopus_files:
        return pd.DataFrame(), 0

    dfs = []
    original_total = 0

    for f in scopus_files:
        df = pd.read_csv(f)
        original_total += len(df)
        if "Source" not in df.columns:
            df["Source"] = "Scopus"
        dfs.append(df)

    scopus = pd.concat(dfs, ignore_index=True)

    if "Author full names" in scopus.columns:
        scopus["Author full names"] = scopus["Author full names"].apply(clean_data_author_full_names)

    if "Title" not in scopus.columns:
        error("Scopus inválido", "No existe la columna 'Title' en el/los CSV de Scopus.")
        raise ValueError("Missing 'Title' in Scopus")

    scopus["processed_title"] = scopus["Title"].apply(preprocess_title)

    if "DOI" in scopus.columns:
        scopus["DOI"] = scopus["DOI"].fillna("").astype(str).str.lower().str.strip()
    else:
        scopus["DOI"] = ""

    # Deduplicación interna (tu intención original: evitar duplicados antes del cruce)
    before = len(scopus)
    scopus = scopus.drop_duplicates(subset=["processed_title"])
    after = len(scopus)

    info("Scopus unificado", f"Merge Scopus: {before} registros → {after} tras deduplicación interna.")
    return scopus, original_total


def load_merge_wos(wos_files: List[Path]) -> Tuple[pd.DataFrame, int]:
    """
    Une múltiples XLS/XLSX de WoS en uno.
    Aplica tu limpieza (Authors, Source Title, Document Type, etc.) y genera processed_title.
    Dedup interno por processed_title.
    Retorna: (df_wos_merge, original_total_rows)
    """
    if not wos_files:
        return pd.DataFrame(), 0

    dfs = []
    original_total = 0

    for f in wos_files:
        df = pd.read_excel(f)
        original_total += len(df)
        df["Source"] = "Web of science"
        dfs.append(df)

    wos = pd.concat(dfs, ignore_index=True)

    required = ["Article Title", "DOI", "Publication Year"]
    missing = [c for c in required if c not in wos.columns]
    if missing:
        error(
            "WoS inválido",
            f"Faltan columnas en WoS: {missing}\n\n"
            "Verifica que el export de WoS tenga las columnas estándar.",
        )
        raise ValueError(f"Missing WoS columns: {missing}")

    if "Authors" in wos.columns:
        wos["Authors"] = wos["Authors"].astype(str).str.replace(",", "", regex=False)

    # Tu lógica: Author(s) ID = Authors si no existe
    if "Author(s) ID" not in wos.columns:
        wos["Author(s) ID"] = wos["Authors"]

    wos["Publication Stage"] = wos.get("Publication Stage", "Final")

    if "Source Title" in wos.columns:
        wos["Source Title"] = wos["Source Title"].astype(str).str.replace("&", "and", regex=False)

    if "Document Type" in wos.columns:
        wos["Document Type"] = wos["Document Type"].apply(normalize_document_type)

    wos["processed_title"] = wos["Article Title"].apply(preprocess_title)
    wos["DOI"] = wos["DOI"].fillna("").astype(str).str.lower().str.strip()

    before = len(wos)
    wos = wos.drop_duplicates(subset=["processed_title"])
    after = len(wos)

    info("WoS unificado", f"Merge WoS: {before} registros → {after} tras deduplicación interna.")
    return wos, original_total

