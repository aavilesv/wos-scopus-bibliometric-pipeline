# ============================================================
# ui_messages.py
# Redirigido a logging para evitar bloqueos por ventanas emergentes
# ============================================================
from __future__ import annotations

import logging
from logging_utils import setup_logger

# Reutilizar el logger configurado o crear uno nuevo
logger = setup_logger("ui_messages")

def info(title: str, msg: str) -> None:
    logger.info(f"[{title}] {msg}")

def warn(title: str, msg: str) -> None:
    logger.warning(f"[{title}] {msg}")

def error(title: str, msg: str) -> None:
    logger.error(f"[{title}] {msg}")
