# app/infra/db.py

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .constants import DB_PATH

INIT_SQL = Path(__file__).parent / "init_db.sql"


def _initialize_db():
    """Crea la base de datos si no existe, usando init_db.sql."""
    db_file = Path(DB_PATH)
    if not db_file.exists():
        db_file.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn, open(INIT_SQL, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        print(f"[INFO] Base de datos inicializada en {DB_PATH}")


@contextmanager
def get_connection():
    _initialize_db()  # asegura que existe y tiene esquema
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()# app/infra/db.py
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .constants import DB_PATH

INIT_SQL = Path(__file__).parent / "init_db.sql"


def _initialize_db():
    """Crea la base de datos si no existe, aplicando init_db.sql."""
    db_file = Path(DB_PATH)
    if not db_file.exists():
        db_file.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn, open(INIT_SQL, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        # Mensaje solo a consola; la app usa Streamlit (no interfiere).
        print(f"[INFO] Base de datos inicializada en {DB_PATH}")


@contextmanager
def get_connection():
    _initialize_db()  # asegura existencia y esquema
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
