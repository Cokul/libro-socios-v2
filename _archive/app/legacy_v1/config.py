# config.py
import os
from pathlib import Path

try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except ImportError:
    pass  # si no está instalado, no pasa nada

# --- Directorio base del proyecto ---
BASE_DIR = Path(__file__).resolve().parent

# --- Variables principales (como Path) ---
DB_PATH     = Path(os.getenv("DB_PATH",     BASE_DIR / "data"    / "libro_socios.db")).resolve()
BACKUP_DIR  = Path(os.getenv("BACKUP_DIR",  BASE_DIR / "backups")).resolve()
EXPORT_DIR  = Path(os.getenv("EXPORT_DIR",  BASE_DIR / "exports")).resolve()
LOG_DIR     = Path(os.getenv("LOG_DIR",     BASE_DIR / "logs")).resolve()
PORT        = int(os.getenv("PORT", "8501"))

# --- Normalizar y crear carpetas ---
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)