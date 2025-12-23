# ============================================================
# ui_messages.py
# ============================================================
from __future__ import annotations

from pathlib import Path

RESULTS_DIR = Path.cwd() / "RESULTS"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def _safe_messagebox(kind: str, title: str, msg: str) -> None:
    """
    kind: 'info' | 'warning' | 'error'
    Muestra messagebox. Si no hay GUI, guarda en RESULTS/run_log.txt.
    """
    try:
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)

        if kind == "info":
            messagebox.showinfo(title, msg)
        elif kind == "warning":
            messagebox.showwarning(title, msg)
        else:
            messagebox.showerror(title, msg)

        root.destroy()
    except Exception:
        log_path = RESULTS_DIR / "run_log.txt"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{kind.upper()}] {title}\n{msg}\n\n")


def info(title: str, msg: str) -> None:
    _safe_messagebox("info", title, msg)


def warn(title: str, msg: str) -> None:
    _safe_messagebox("warning", title, msg)


def error(title: str, msg: str) -> None:
    _safe_messagebox("error", title, msg)

