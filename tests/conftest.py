# tests/conftest.py
import sys
import sqlite3
from pathlib import Path
import pytest

# --- AÑADE LA RAÍZ DEL REPO AL sys.path ---
# tests/ está en <repo>/tests → la raíz es parent
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# --- FIXTURE: conexión SQLite en memoria compartida ---
@pytest.fixture()
def inmemory_conn():
    """
    Conexión SQLite en memoria compartida para un módulo concreto.
    Devolvemos siempre la MISMA conexión (yield) para que el esquema persista
    a lo largo del test.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()