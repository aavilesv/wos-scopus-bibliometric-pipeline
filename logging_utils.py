import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(name: str = "utils", log_dir: Path = None) -> logging.Logger:
    """
    Configura un sistema de registro (logger) robusto.
    
    Características:
      - Salida a CONSOLA: Nivel INFO (Mensajes normales de usuario)
      - Salida a ARCHIVO: Nivel DEBUG (Traza completa para auditoría)
    
    Args:
        name: Nombre del componente que escribe el log (ej. 'main', 'deduplication')
        log_dir: Carpeta donde guardar los archivos .txt de log
    """
    # 1. Obtener o crear una instancia de logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capturar todo (Debug, Info, Warning, Error)

    # Evitar duplicar manejadores si se llama múltiples veces a esta función
    if logger.handlers:
        return logger

    # Formato del mensaje: Hora | Nivel | Mensaje
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S"
    )

    # 2. Configurar Salida a Consola (Pantalla)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO) # Solo mostrar Info o superior en pantalla, no debug
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 3. Configurar Salida a Archivo (si se provee directorio)
    if log_dir:
        try:
            # Crear directorio de logs si no existe
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Crear nombre de archivo con timestamp único (ej. pipeline_log_20240211_1530.txt)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"pipeline_log_{timestamp}.txt"
            
            # FileHandler escribe al disco
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG) # Guardar TODO en el archivo
            
            # Formato más detallado para archivo (incluye nombre del logger)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            fh.setFormatter(file_formatter)
            logger.addHandler(fh)
        except Exception as e:
            # Si falla escribir archivo (ej. permisos), avisar pero no detener programa
            print(f"Warning: Could not setup file logging: {e}")

    return logger
