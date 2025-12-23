from dataclasses import dataclass
from pathlib import Path
from typing import List

from ui_messages import info, warn, error

@dataclass(frozen=True)
class PipelinePaths:
    base_dir: Path
    files_dir: Path
    results_dir: Path
    scopus_dir: Path
    wos_dir: Path
    scimago_dir: Path
    scimago_file: Path

@dataclass(frozen=True)
class InputInventory:
    scopus_files: List[Path]
    wos_files: List[Path]
    scimago_exists: bool

def build_default_paths(base_dir: Path | None = None) -> PipelinePaths:
    base = base_dir or Path.cwd()
    files_dir = base / "FILES"
    results_dir = base / "RESULTS"
    scopus_dir = files_dir / "SCOPUS"
    wos_dir = files_dir / "WOS"
    scimago_dir = files_dir / "SCIMAGO"
    scimago_file = scimago_dir / "scimago_unificado.csv"

    results_dir.mkdir(parents=True, exist_ok=True)
    return PipelinePaths(
        base_dir=base,
        files_dir=files_dir,
        results_dir=results_dir,
        scopus_dir=scopus_dir,
        wos_dir=wos_dir,
        scimago_dir=scimago_dir,
        scimago_file=scimago_file,
    )

def scan_inputs(paths: PipelinePaths) -> InputInventory:
    scopus_files = sorted(paths.scopus_dir.glob("*.csv")) if paths.scopus_dir.exists() else []
    wos_files = []
    if paths.wos_dir.exists():
        wos_files = sorted(list(paths.wos_dir.glob("*.xls")) + list(paths.wos_dir.glob("*.xlsx")))
    scimago_exists = paths.scimago_file.exists()
    return InputInventory(scopus_files=scopus_files, wos_files=wos_files, scimago_exists=scimago_exists)

def validate_or_stop(paths: PipelinePaths, inv: InputInventory) -> bool:
    """
    Devuelve True si se puede continuar, False si se debe cortar el proceso.
    """

    # 1) Validación básica de existencia de carpetas (opcional pero útil)
    if not paths.files_dir.exists():
        error(
            "Carpeta FILES no existe",
            f"No existe la carpeta:\n{paths.files_dir}\n\n"
            "Crea la estructura:\n"
            "FILES/SCOPUS\nFILES/WOS\nFILES/SCIMAGO\n"
        )
        return False

    # 2) Mensaje informativo con inventario
    info(
        "Validación de insumos",
        "Inventario detectado:\n\n"
        f"- Scopus CSV: {len(inv.scopus_files)}\n"
        f"- WoS XLS/XLSX: {len(inv.wos_files)}\n"
        f"- SCImago: {'Sí' if inv.scimago_exists else 'No'}\n\n"
        "Rutas:\n"
        f"- SCOPUS: {paths.scopus_dir}\n"
        f"- WOS: {paths.wos_dir}\n"
        f"- SCIMAGO: {paths.scimago_file}\n"
    )

    has_scopus = len(inv.scopus_files) > 0
    has_wos = len(inv.wos_files) > 0

    # 3) Early exit: no hay nada que procesar
    if not has_scopus and not has_wos:
        error(
            "Sin archivos para procesar",
            "No se encontraron archivos válidos.\n\n"
            "Acción requerida:\n"
            "- Coloca archivos *.csv en FILES/SCOPUS\n"
            "- Coloca archivos *.xls o *.xlsx en FILES/WOS\n\n"
            "El proceso se canceló para evitar ejecución innecesaria."
        )
        return False

    # 4) Advertencias si falta una fuente
    if not has_scopus:
        warn("Scopus vacío", "No hay CSV de Scopus. El proceso continuará únicamente con WoS.")
    if not has_wos:
        warn("WoS vacío", "No hay XLS/XLSX de WoS. El proceso continuará únicamente con Scopus.")
    if not inv.scimago_exists:
        warn("SCImago no encontrado", "No hay SCImago. Se omitirá la normalización ISSN→título canónico.")

    return True
