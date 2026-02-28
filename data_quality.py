# ============================================================
# data_quality.py
#   - Módulo para validación de calidad de datos y limpieza profunda
#   - Se enfoca en DOIs, Años y Títulos.
#   - Reporta métricas de "Antes" y "Después".
# ============================================================
import re
import pandas as pd
import numpy as np
from datetime import datetime
from ui_messages import info, warn
from config import remove_blank_dois
def clean_doi(doi_value: str) -> str:
    """
    Intenta extraer un DOI válido de una cadena sucia.
    Busca patrones que empiecen por '10.' seguido de números, barra y caracteres válidos.
    Elimina prefijos como 'https://doi.org/' o 'doi:'.
    
    Args:
        doi_value: Valor del DOI (puede ser str, float/nan, etc.)
    
    Returns:
        str: DOI limpio (ej. '10.1016/j.jbusres.2023.113642') o "" si no es válido.
    """
    if doi_value is None:
        return ""
    
    # Asegurar string
    doi_str = str(doi_value).strip().lower()
    
    # Chequeo rápido de nulidad o strings vacíos/nan
    if not doi_str or doi_str == "nan" or doi_str == "none":
        return ""
    
    # Regex para capturar desde '10.' hasta el final de una cadena típica de DOI
    # Permitimos caracteres comunes en DOIs: a-z, 0-9, ., -, _, ;, (, ), /
    # Detenemos la captura si encontramos espacios o caracteres raros al final si es necesario
    doi_pattern = r'(10\.\d{4,9}/[-._;()/:a-z0-9]+)'
    match = re.search(doi_pattern, doi_str)
    
    if match:
        return match.group(1)
    
    return ""

def validate_quality(df: pd.DataFrame, source_name: str, remove_blank_dois: bool = remove_blank_dois) -> tuple[pd.DataFrame, dict]:
    """
    Aplica limpieza crítica secuencial (DOI, Title, Authors, Abstract) y reporta estadísticas para PRISMA.
    
    Args:
        df: DataFrame a validar
        source_name: Nombre de la fuente (ej. "Scopus")
        remove_blank_dois: Si True, elimina registros sin DOI válido
    
    Returns:
        tuple: (DataFrame limpio, diccionario con métricas PRISMA)
    """
    stats = {
        "initial_count": len(df),
        "removed_doi": 0,
        "removed_title": 0,
        "removed_authors": 0,
        "removed_abstract": 0,
        "final_count": len(df)
    }

    if df.empty:
        warn(f"Calidad {source_name}", "El DataFrame está vacío, saltando validación.")
        return df, stats

    info(f"Validando calidad PRISMA ({source_name})", f"Analizando {stats['initial_count']} registros...")
    
    # ----------------------------------------------------
    # 1. Limpieza y Filtrado de DOI
    # ----------------------------------------------------
    if "DOI" in df.columns:
        df["DOI"] = df["DOI"].apply(clean_doi)
        if remove_blank_dois:
            before_doi = len(df)
            df = df[df["DOI"] != ""].copy()
            stats["removed_doi"] = before_doi - len(df)
            if stats["removed_doi"] > 0:
                warn(f"Filtrado DOI ({source_name})", 
                     f"Se eliminaron {stats['removed_doi']} registros sin DOI válido.")
    
    # ----------------------------------------------------
    # 2. Filtrado de Títulos Vacíos
    # ----------------------------------------------------
    # Nota: usamos 'Title' en lugar de 'processed_title' como filtro fundamental
    if "Title" in df.columns:
        before_title = len(df)
        df["Title"] = df["Title"].fillna("").astype(str).str.strip()
        df = df[df["Title"] != ""].copy()
        stats["removed_title"] = before_title - len(df)
        if stats["removed_title"] > 0:
            warn(f"Filtrado Título ({source_name})", 
                 f"Se eliminaron {stats['removed_title']} registros sin Título.")

    # ----------------------------------------------------
    # 3. Filtrado de Autores Vacíos
    # ----------------------------------------------------
    if "Authors" in df.columns:
        before_authors = len(df)
        df["Authors"] = df["Authors"].fillna("").astype(str).str.strip()
        df = df[df["Authors"] != ""].copy()
        stats["removed_authors"] = before_authors - len(df)
        if stats["removed_authors"] > 0:
            warn(f"Filtrado Autores ({source_name})", 
                 f"Se eliminaron {stats['removed_authors']} registros sin Autores.")

    # ----------------------------------------------------
    # 4. Filtrado de Abstract Vacío
    # ----------------------------------------------------
    if "Abstract" in df.columns:
        before_abstract = len(df)
        df["Abstract"] = df["Abstract"].fillna("").astype(str).str.strip()
        df = df[df["Abstract"] != ""].copy()
        stats["removed_abstract"] = before_abstract - len(df)
        if stats["removed_abstract"] > 0:
            warn(f"Filtrado Abstract ({source_name})", 
                 f"Se eliminaron {stats['removed_abstract']} registros sin Abstract.")

    # ----------------------------------------------------
    # 5. Resumen Final PRISMA
    # ----------------------------------------------------
    stats["final_count"] = len(df)
    
    if stats["final_count"] != stats["initial_count"]:
        info(f"Resumen Calidad PRISMA ({source_name})", 
             f"Registros iniciales: {stats['initial_count']} -> Finales: {stats['final_count']}\n"
             f"Eliminados: DOI={stats['removed_doi']}, Título={stats['removed_title']}, "
             f"Autores={stats['removed_authors']}, Abstract={stats['removed_abstract']}.")
    
    return df, stats
