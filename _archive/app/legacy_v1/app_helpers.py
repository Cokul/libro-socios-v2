# app_helpers.py

import streamlit as st
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import traceback

# ---------------- UI helpers ----------------
def sticky_headers():
    st.markdown("""
    <style>
      [data-testid="stDataFrame"] .st-ag-grid .ag-header { position: sticky; top: 0; z-index: 1; }
      [data-testid="stDataFrame"] .ag-cell { line-height: 1.4rem !important; padding-top: 6px !important; padding-bottom: 6px !important; }
    </style>
    """, unsafe_allow_html=True)

def toast_ok(msg="✅ Guardado"): st.toast(msg)
def toast_info(msg="ℹ️ Hecho"): st.toast(msg)
def toast_warn(msg="⚠️ Revisa los campos"): st.toast(msg)

def filter_df_by_query(df: pd.DataFrame, query: str, cols: list[str] | None = None) -> pd.DataFrame:
    if not query:
        return df
    if cols is None:
        cols = [c for c in df.columns if df[c].dtype == "object"]
    mask = pd.Series(False, index=df.index)
    for c in cols:
        mask = mask | df[c].fillna("").str.contains(query, case=False, na=False)
    return df[mask]

# ---------------- Logging ----------------
_LOGGER = None

def setup_logging(log_dir: str = "logs", filename: str = "app.log") -> logging.Logger:
    """Configura logging con rotación; idempotente."""
    global _LOGGER
    if _LOGGER:
        return _LOGGER
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, filename)

    logger = logging.getLogger("libro_socios_app")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s (%(filename)s:%(lineno)d)")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    _LOGGER = logger
    logger.info("Logging iniciado. Fichero: %s", log_path)
    return logger

def log_exception(exc: Exception, where: str = "", extra: dict | None = None) -> None:
    """Registra traceback completo en logs/app.log."""
    logger = setup_logging()
    msg = f"Excepción en {where}" if where else "Excepción"
    if extra:
        msg += f" | extra={extra}"
    logger.error("%s\n%s", msg, "".join(traceback.format_exception(exc)))

# ---------------- Errores amigables ----------------
_ERROR_MAP = {
    "UNIQUE constraint failed": "Ya existe un registro con esos valores. Revisa duplicados.",
    "FOREIGN KEY constraint failed": "Operación no permitida por relaciones de datos. Comprueba dependencias.",
    "CHECK constraint failed": "Algún valor incumple una validación. Revisa rangos y tipos.",
}

def friendly_error(exc: Exception) -> str:
    """
    Resumen legible + recomendación de recuperación.
    Combina mapeos conocidos y mensajes por tipo de excepción SQLite.
    """
    import sqlite3
    raw = str(exc) or exc.__class__.__name__

    # mapping rápido
    for k, v in _ERROR_MAP.items():
        if k in raw:
            base = v
            break
    else:
        base = raw

    tips = "Prueba a cerrar y reabrir la aplicación; si persiste, restaura una copia de seguridad."

    if isinstance(exc, sqlite3.OperationalError):
        low = raw.lower()
        if "locked" in low:
            tips = ("La base de datos está ocupada. Cierra otras ventanas/procesos que la usen, "
                    "espera unos segundos y vuelve a intentarlo. Si persiste, reabre la app o restaura una copia.")
        elif "no such table" in low:
            tips = ("Parece faltar una tabla. Ve a Administración → Autochequeo para reparar/migrar el esquema. "
                    "Si persiste, restaura una copia.")
    elif isinstance(exc, sqlite3.IntegrityError):
        tips = ("Conflicto de datos (restricción). Revisa duplicados o referencias. "
                "Si no lo resuelves, exporta/backup y restaura una copia previa.")

    return f"{base}\n\nSugerencia: {tips}"