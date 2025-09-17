import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

def recompute():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    company_ids = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]

    for cid in company_ids:
        cur = conn.execute("""
            SELECT id, fecha
            FROM events
            WHERE company_id=?
            ORDER BY fecha, id
        """, (cid,))
        rows = cur.fetchall()
        for idx, row in enumerate(rows, start=1):
            conn.execute("UPDATE events SET correlativo=? WHERE id=?", (idx, row["id"]))
        print(f"✔ Recomputado correlativo para company_id={cid} ({len(rows)} eventos)")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    recompute()