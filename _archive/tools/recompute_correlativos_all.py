# tools/recompute_correlativos_all.py

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

def main():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        # Asegura columna
        cols = [r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
        if "correlativo" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN correlativo INTEGER")

        company_ids = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]
        for cid in company_ids:
            conn.execute("DROP TABLE IF EXISTS _tmp_corr")
            conn.execute(f"""
                CREATE TEMP TABLE _tmp_corr AS
                SELECT id, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY fecha, id) AS rn
                FROM events WHERE company_id={cid}
            """)
            conn.execute("""
                UPDATE events
                   SET correlativo = (SELECT rn FROM _tmp_corr WHERE _tmp_corr.id = events.id)
                 WHERE company_id=?
            """, (cid,))
            conn.execute("DROP TABLE IF EXISTS _tmp_corr")
            print(f"✓ correlativos recalculados para company_id={cid}")
        print("✅ Listo")

if __name__ == "__main__":
    main()