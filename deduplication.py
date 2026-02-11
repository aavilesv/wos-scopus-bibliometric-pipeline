# ============================================================
# deduplication.py
#   - Deduplicación cruzada entre WoS (Web of Science) y Scopus
#   - Estrategia:
#       1) DOI match (exacto y rápido)
#       2) Fuzzy title + año ±1 (aproximado y lento -> PARALELIZADO)
# ============================================================
from __future__ import annotations

# Librería estándar para procesamiento paralelo (aprovechar múltiples núcleos de CPU)
import multiprocessing
# Ejecutor de procesos para lanzar tareas en paralelo
from concurrent.futures import ProcessPoolExecutor, as_completed
# Tipado estático para ayudar al IDE y desarrolladores
from typing import Set, List, Dict, Any

# Librerías científicas
import numpy as np
import pandas as pd
# Librería de comparación difusa de strings (más rápida que fuzzywuzzy)
from rapidfuzz import fuzz, process

# Importamos logger en lugar de ui_messages para registrar eventos
# aunque en este archivo mantenemos la lógica pura y usamos prints para depuración interna de workers.

# ---------------------------------------------------------
# Variables Globales para Workers (Procesos Hijos)
# ---------------------------------------------------------
# Estas variables almacenarán la copia de datos de Scopus en cada proceso.
# Se usan globales para que al crear el proceso, la memoria se comparta (copy-on-write en Linux/Mac)
# o se inicialice una sola vez en Windows, evitando pasar grandes datos en cada llamada.
_scopus_titles: List[str] = []
_scopus_years: List[float] = []

def init_worker(titles: List[str], years: List[float]):
    """
    Función de inicialización que se ejecuta UNA VEZ por cada proceso worker creado.
    Recibe la lista completa de títulos y años de Scopus.
    """
    global _scopus_titles, _scopus_years
    _scopus_titles = titles
    _scopus_years = years

def process_chunk(wos_chunk: List[Dict[str, Any]], threshold: int) -> Set[str]:
    """
    Función que ejecuta cada worker en paralelo.
    Procesa un subconjunto (chunk) de registros de WoS y busca si existen en los datos globales de Scopus.
    
    Args:
        wos_chunk: Lista de diccionarios con datos de WoS (título, año)
        threshold: Umbral de similitud (0-100) para considerar duplicado
        
    Returns:
        Set de títulos normalizados que se encontraron duplicados.
    """
    duplicates_found = set()
    
    # Accedemos a las variables globales cargadas en init_worker
    global _scopus_titles, _scopus_years

    # Iteramos sobre cada artículo de WoS en este chunk
    for row in wos_chunk:
        w_title = row.get("processed_title", "")
        w_year = row.get("year", np.nan)
        
        # Optimización: Si el título es muy corto (< 5 chars), ignorar para evitar falsos positivos ruido
        if len(w_title) < 5:
            continue

        # BUSQUEDA DIFUSA:
        # process.extractOne busca el mejor candidato en _scopus_titles para w_title
        # scorer=fuzz.WRatio usa una media ponderada de diferentes algoritmos de Levenshtein
        # Retorna una tupla: (mejor_match, puntaje, índice_en_lista)
        result = process.extractOne(w_title, _scopus_titles, scorer=fuzz.WRatio)
        
        # Si no encontró nada (lista vacía), continuar
        if not result:
            continue
            
        _, score, idx = result
        
        # Si el puntaje es menor al umbral definido (ej. 85), NO es duplicado
        if score < threshold:
            continue
            
        # VALIDACIÓN DE AÑO:
        # Si pasó el filtro de título, verificamos el año para estar seguros
        # Recuperamos el año del candidato encontrado en Scopus usando el índice
        s_year = _scopus_years[idx]
        
        # Solo comparamos si ambos registros tienen año válido (no son NaN)
        if pd.notna(w_year) and pd.notna(s_year):
            # Calculamos la diferencia absoluta entre años.
            # Permitimos un margen de error de 1 año (ej. 2020 vs 2021 es aceptable)
            # Si la diferencia es mayor a 1, asumimos que son papers distintos con título similar
            if abs(int(w_year) - int(s_year)) > 1:
                continue
                
        # Si pasa todas las pruebas, agregamos el título a la lista de duplicados encontrados
        duplicates_found.add(w_title)
        
    return duplicates_found

# ---------------------------------------------------------
# Lógica Principal de Deduplicación
# ---------------------------------------------------------
def cross_deduplicate(scopus_df: pd.DataFrame, wos_df: pd.DataFrame, threshold: int) -> Set[str]:
    """
    Función maestra para identificar registros de WoS que ya existen en Scopus.
    
    Estrategia Híbrida:
      1. DOI Match: Búsqueda exacta por identificador digital (muy rápido, O(1)).
      2. Fuzzy Match: Búsqueda aproximada por texto (lento, O(N*M), optimizado con paralelo).
      
    Returns:
        Un conjunto (Set) de títulos 'processed_title' que deben eliminarse de WoS.
    """
    duplicates: Set[str] = set()

    # Validaciones básicas: si no hay datos, no hay nada que duplicar
    if scopus_df is None or wos_df is None or scopus_df.empty or wos_df.empty:
        return duplicates

    # PREPARACIÓN DE DATOS (SCOPUS - REFERENCIA)
    # Extraemos DOIs de Scopus, normalizamos a minúsculas y limpiamos espacios
    scopus_dois = set(scopus_df.get("DOI", pd.Series([], dtype=str)).dropna().str.lower().str.strip().values)
    scopus_dois.discard("") # Asegurar que no quede string vacío
    
    # Listas para búsqueda difusa
    scopus_titles = scopus_df["processed_title"].tolist()
    # Convertimos años a numérico, forzando NaN si hay errores
    scopus_years = pd.to_numeric(scopus_df.get("Year", pd.Series([np.nan]*len(scopus_df))), errors="coerce").tolist()

    print(f"   [Deduplication] Reference Scopus: {len(scopus_titles)} rows")
    print(f"   [Deduplication] Candidates WoS: {len(wos_df)} rows")

    # -------------------------------------------------------
    # FASE 1: DOI MATCH (Exacto y Rápido)
    # -------------------------------------------------------
    # Guardamos los índices de filas de WoS que ya encontramos para no procesarlas después
    wos_duplicates_indices = set()
    
    for idx, wrow in wos_df.iterrows():
        wdoi = str(wrow.get("DOI", "")).lower().strip()
        # Si tiene DOI y ese DOI está en el set de Scopus -> DUPLICADO
        if wdoi and wdoi in scopus_dois:
            duplicates.add(wrow["processed_title"])
            wos_duplicates_indices.add(idx)

    print(f"   [Deduplication] Found by DOI: {len(duplicates)}")

    # -------------------------------------------------------
    # FASE 2: FUZZY MATCH (Paralelo)
    # -------------------------------------------------------
    # Preparamos la lista de candidatos: artículos de WoS que NO tienen DOI match
    candidates = []
    
    # Pre-calcular años de WoS vectorizadamente para rapidez
    wos_years = pd.to_numeric(wos_df.get("Publication Year", pd.Series([np.nan]*len(wos_df))), errors="coerce")
    
    for idx, wrow in wos_df.iterrows():
        # Si ya lo encontramos por DOI, saltar
        if idx in wos_duplicates_indices:
            continue
            
        wtitle = wrow.get("processed_title", "")
        # Si el título ya está en duplicados (por otro registro), saltar
        if wtitle in duplicates:
            continue
            
        # Agregamos registro liviano (solo lo necesario) para enviar al worker
        candidates.append({
            "processed_title": wtitle,
            "year": wos_years[idx]
        })

    # Si no quedan candidatos, terminamos
    if not candidates:
        return duplicates

    # CONFIGURACIÓN PARALELA
    # Usamos máximo 8 núcleos o los que tenga la CPU, lo que sea menor
    num_workers = min(multiprocessing.cpu_count(), 8)
    # Calculamos tamaño del chunk: cuántos items procesa cada worker de una vez
    chunk_size = max(1, len(candidates) // num_workers)
    
    # Dividir la lista de candidatos en sub-listas (chunks)
    chunks = [candidates[i:i + chunk_size] for i in range(0, len(candidates), chunk_size)]
    
    print(f"   [Deduplication] Starting fuzzy match on {len(candidates)} records using {num_workers} cores...")

    # INICIO DEL POOL DE PROCESOS
    # ProcessPoolExecutor gestiona el ciclo de vida de los procesos hijos
    with ProcessPoolExecutor(max_workers=num_workers, initializer=init_worker, initargs=(scopus_titles, scopus_years)) as executor:
        # Enviamos cada chunk a procesar. 'futures' son objetos que representarán el resultado futuro
        futures = [executor.submit(process_chunk, chunk, threshold) for chunk in chunks]
        
        # as_completed nos devuelve los resultados a medida que cada worker termina
        for future in as_completed(futures):
            try:
                # Obtenemos el set de duplicados encontrado por ese worker
                chunk_dupes = future.result()
                # Actualizamos el conjunto principal con los nuevos hallazgos
                duplicates.update(chunk_dupes)
            except Exception as e:
                print(f"   [Error] in worker: {e}")

    print(f"   [Deduplication] Total duplicates found: {len(duplicates)}")
    return duplicates
