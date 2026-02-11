from pathlib import Path

# ============================================================
# Pipeline Configuration
# Este archivo centraliza todas las constantes y rutas del proyecto.
# ============================================================

# Base Paths (Rutas Base)
# Path(__file__).resolve().parent obtiene la carpeta donde está este archivo config.py
BASE_DIR = Path(__file__).resolve().parent

# Carpetas principales de datos
FILES_DIR = BASE_DIR / "FILES"    # Donde se buscan los inputs
RESULTS_DIR = BASE_DIR / "RESULTS" # Donde se guardan los outputs

# Subdirectorios de Input específicos
SCOPUS_DIR = FILES_DIR / "SCOPUS"
WOS_DIR = FILES_DIR / "WOS"
SCIMAGO_DIR = FILES_DIR / "SCIMAGO"

# Asegurar que el directorio de resultados exista, si no, lo crea automáticamente
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Parámetros de Análisis (Analysis Parameters)
# Rango de años a filtrar (inclusive)
YEAR_START = 2015
YEAR_FINAL = 2025

# Umbral de similitud para deduplicación (85%)
# Significa que títulos con >= 85% de similitud serán considerados duplicados
FUZZY_THRESHOLD = 85

# Umbral de similitud para encontrar revistas en SCImago (90% es más estricto)
SCIMAGO_FUZZY_THRESHOLD = 90
