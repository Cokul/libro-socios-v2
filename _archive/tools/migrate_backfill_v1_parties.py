# tools/migrate_backfill_v1_parties.py

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

def main():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        # Asegura columnas si no existen
        cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "socio_origen_id" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN socio_origen_id INTEGER")
        if "socio_destino_id" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN socio_destino_id INTEGER")

        # Backfill simple: copia 1:1 si están en NULL
        conn.execute("""
            UPDATE events
               SET socio_origen_id  = COALESCE(socio_origen_id,  socio_transmite),
                   socio_destino_id = COALESCE(socio_destino_id, socio_adquiere)
            WHERE socio_transmite IS NOT NULL OR socio_adquiere IS NOT NULL
        """)
        conn.commit()
    print("✅ Backfill parties completado.")

if __name__ == "__main__":
    main()