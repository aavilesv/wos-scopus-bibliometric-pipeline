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

def validate_quality(df: pd.DataFrame, source_name: str, remove_blank_dois: bool = False) -> pd.DataFrame:
    """
    Aplica limpieza crítica (DOI) y reporta estadísticas de calidad.
    
    Args:
        df: DataFrame a validar
        source_name: Nombre de la fuente (ej. "Scopus")
        remove_blank_dois: Si True, elimina registros sin DOI válido
    
    Returns:
        DataFrame con la columna DOI limpia (y opcionalmente filtrado)
    """
    if df.empty:
        warn(f"Calidad {source_name}", "El DataFrame está vacío, saltando validación.")
        return df

    initial_count = len(df)
    info(f"Validando calidad ({source_name})", f"Analizando {initial_count} registros...")
    
    # ----------------------------------------------------
    # 1. Limpieza de DOI
    # ----------------------------------------------------
    if "DOI" in df.columns:
        # Métricas ANTES
        original_dois = df["DOI"].fillna("").astype(str)
        # Contamos cuántos tenían "algo" (longitud > 3 para filtrar ruidos mínimos) antes
        total_phisical_dois_before = original_dois[original_dois.str.len() > 3].count()
        
        # Aplicar limpieza robusta
        df["DOI"] = df["DOI"].apply(clean_doi)
        
        # Métricas DESPUÉS
        valid_dois_after = (df["DOI"] != "").sum()
        blank_dois_count = (df["DOI"] == "").sum()
        
        # Diferencia: DOIs que creíamos tener vs los que realmente sirven
        cleaned_invalid_dois = total_phisical_dois_before - valid_dois_after
        
        # Reporte
        msg_doi = f"DOIs Válidos: {valid_dois_after} | En Blanco: {blank_dois_count} (Total registros: {initial_count})"
        
        if cleaned_invalid_dois > 0:
            warn(f"DOIs Limpiados ({source_name})", 
                 f"Se eliminaron/corrigieron {cleaned_invalid_dois} valores en columna DOI que no eran válidos.")
        
        info(f"Reporte DOI ({source_name})", msg_doi)
        
        # Opcionalmente eliminar registros sin DOI
        if remove_blank_dois:
            df_filtered = df[df["DOI"] != ""].copy()
            removed_count = len(df) - len(df_filtered)
            if removed_count > 0:
                warn(f"Filtrado DOI ({source_name})", 
                     f"Se eliminaron {removed_count} registros sin DOI válido ({removed_count/initial_count*100:.1f}%)")
            df = df_filtered
    else:
        warn(f"Falta columna DOI ({source_name})", "No se encontró la columna 'DOI' para validar.")

    # ----------------------------------------------------
    # 2. Validación de Años
    # ----------------------------------------------------
    current_year = datetime.now().year
    if "Year" in df.columns:
        years_numeric = pd.to_numeric(df["Year"], errors='coerce')
        missing_years = years_numeric.isna().sum()
        
        # Rango razonable: 1900 a Hoy+2
        valid_range_mask = (years_numeric >= 1900) & (years_numeric <= (current_year + 2))
        out_of_range = (years_numeric.notna() & ~valid_range_mask).sum()
        
        if missing_years > 0:
            warn(f"Años Faltantes ({source_name})", f"{missing_years} registros no tienen año válido ({missing_years/initial_count*100:.1f}%).")
        if out_of_range > 0:
            warn(f"Años Fuera de Rango ({source_name})", f"{out_of_range} registros tienen años sospechosos (<1900 o >{current_year+2}).")
            
    # ----------------------------------------------------
    # 3. Validación de Títulos
    # ----------------------------------------------------
    if "processed_title" in df.columns:
        empty_titles = (df["processed_title"] == "").sum()
        if empty_titles > 0:
            warn(f"Títulos Vacíos ({source_name})", f"{empty_titles} registros no tienen título válido tras preprocesamiento ({empty_titles/initial_count*100:.1f}%).")

    # ----------------------------------------------------
    # 4. Resumen Final
    # ----------------------------------------------------
    final_count = len(df)
    if final_count != initial_count:
        info(f"Filtrado Final ({source_name})", f"Registros finales: {final_count} (de {initial_count} inicial, eliminados: {initial_count - final_count})")
    
    return df
