import os
import json
import requests
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from config import RESULTS_DIR
from ui_messages import info, warn

# Rutas y constantes
CACHE_FILE = RESULTS_DIR / ".doi_cache.json"

# Políticas de Estado de Validación (DOI Status)
# ----------------------------------------------------
# valid       : Confirmado en Crossref, el DOI existe.
# invalid     : Crossref devolvió 404, DOI erróneo / inventado.
# unverified  : Error de red, timeout, rate limit (no se puede afirmar si existe o no).
# missing_doi : No había DOI en el registro originario.
# ----------------------------------------------------
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"  
STATUS_UNVERIFIED = "unverified"
STATUS_MISSING = "missing_doi"

def _setup_session() -> requests.Session:
    session = requests.Session()
    # Polite pool contact para no ser bloqueados rápidamente
    # Es recomendable reemplazar el email en el futuro en un sistema de producción
    session.headers.update({
        "User-Agent": "BibliometricPipeline/1.0 (mailto:pipeline_admin@example.com)"
    })
    # Estrategia de reintentos
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def check_single_doi(doi: str, session: requests.Session) -> dict:
    """
    Valida un único DOI contra la API REST de Crossref.
    Retorna un diccionario con el estado y cualquier metadata rescatada.
    """
    if not doi:
        return {"status": STATUS_MISSING, "metadata_found": False, "title": None, "year": None}

    url = f"https://api.crossref.org/works/{doi}"
    try:
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            msg = data.get("message", {})
            
            title_list = msg.get("title", [])
            title = title_list[0] if title_list else None
            
            # Intentar extraer el año oficial
            year = None
            for key in ["published-print", "published-online", "created"]:
                if key in msg and "date-parts" in msg[key]:
                    try:
                        year = msg[key]["date-parts"][0][0]
                        break
                    except (IndexError, TypeError):
                        pass

            return {
                "status": STATUS_VALID,
                "metadata_found": True,
                "title": title,
                "year": year
            }
        
        elif response.status_code == 404:
            return {"status": STATUS_INVALID, "metadata_found": False, "title": None, "year": None}
        else:
            return {"status": STATUS_UNVERIFIED, "metadata_found": False, "title": None, "year": None}
            
    except requests.exceptions.RequestException:
        # Errores de red o de timeout se consideran "no verificados", no "inválidos".
        return {"status": STATUS_UNVERIFIED, "metadata_found": False, "title": None, "year": None}


def validate_dois_online(df: pd.DataFrame, source_name: str = "") -> tuple[pd.DataFrame, dict]:
    """
    Valida en línea la columna DOI del DataFrame y anexa columnas de diagnóstico
    sin eliminar las filas.
    
    Args:
        df: DataFrame que incluye la columna 'DOI' ya limpiada sintácticamente
        source_name: String del dataset para logeo (e.g. Scopus, WoS, Combined)
        
    Returns:
        DataFrame modificado con columnas:
          - doi_verified (bool)
          - doi_validation_status (str)
          - doi_metadata_found (bool)
        Dict con los contadores de estados.
    """
    if df.empty or "DOI" not in df.columns:
        return df, {}
        
    info(f"DOI Online Validation {source_name}", f"Iniciando verificación online de Crossref para {len(df)} registros...")
    
    cache = load_cache()
    session = _setup_session()
    
    # Extraer DOIs únicos que no están en la caché
    unique_dois = set(df["DOI"].dropna().astype(str).str.strip())
    unique_dois = {d for d in unique_dois if d and d not in cache}
    
    if unique_dois:
        info(f"DOI Online Validation", f"Consultando {len(unique_dois)} DOIs a Crossref...")
        results_new = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_doi = {executor.submit(check_single_doi, d, session): d for d in unique_dois}
            
            iterable = as_completed(future_to_doi)
            if tqdm is not None:
                iterable = tqdm(iterable, total=len(unique_dois), desc=f"Validando DOIs {source_name}")
                
            for future in iterable:
                d = future_to_doi[future]
                try:
                    res = future.result()
                    results_new[d] = res
                except Exception:
                    results_new[d] = {"status": STATUS_UNVERIFIED, "metadata_found": False, "title": None, "year": None}
                    
        cache.update(results_new)
        save_cache(cache)
    else:
        info(f"DOI Online Validation", "Todos los DOIs detectados ya existían en caché.")
        
    # Asignar a las filas
    def apply_doi_status(doi_val):
        doi_str = str(doi_val).strip() if pd.notna(doi_val) else ""
        if not doi_str:
            return False, STATUS_MISSING, False
            
        res = cache.get(doi_str, {"status": STATUS_UNVERIFIED, "metadata_found": False})
        
        is_verified = (res["status"] == STATUS_VALID)
        return is_verified, res["status"], res["metadata_found"]
        
    status_tuples = df["DOI"].apply(apply_doi_status)
    
    # Creamos las columnas que pidió el usuario
    # Indicamos, en lugar de borrar, el resultado de la averiguación documental
    df["doi_verified"] = [t[0] for t in status_tuples]
    df["doi_validation_status"] = [t[1] for t in status_tuples]
    df["doi_metadata_found"] = [t[2] for t in status_tuples]
    
    # Resumen de validación
    counts = df["doi_validation_status"].value_counts().to_dict()
    info(f"Resumen DOI Online Validation ({source_name})", f"Resultados Crossref: {counts}")
    
    return df, counts
