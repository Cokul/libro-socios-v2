# tools/migrate_merge_referencia.py
from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

with sqlite3.connect(DB_PATH) as conn:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
    if "referencia" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN referencia TEXT")
    # Si referencia está vacía, cópiala desde observaciones
    conn.execute("""
        UPDATE events
           SET referencia = COALESCE(NULLIF(referencia, ''), observaciones)
    """)
    conn.commit()
print("✅ Referencia unificada.")