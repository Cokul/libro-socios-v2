#app/infra/db.py

import sqlite3
from contextlib import contextmanager
from .constants import DB_PATH
@contextmanager
def get_connection():
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