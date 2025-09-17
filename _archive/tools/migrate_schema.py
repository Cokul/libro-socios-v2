# tools/migrate_schema.py
from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

def col_exists(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def table_exists(conn, table):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def ensure_board_members(conn):
    if not table_exists(conn, "board_members"):
        conn.execute("""
        CREATE TABLE board_members (
            id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            cargo TEXT NOT NULL,
            nif TEXT,
            direccion TEXT,
            telefono TEXT,
            email TEXT,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
        )
        """)
        print("✓ Creada tabla board_members")
    else:
        # Añadir columnas nuevas si faltaran
        for col, ddl in [
            ("direccion", "ALTER TABLE board_members ADD COLUMN direccion TEXT"),
            ("telefono",  "ALTER TABLE board_members ADD COLUMN telefono TEXT"),
            ("email",     "ALTER TABLE board_members ADD COLUMN email TEXT"),
        ]:
            if not col_exists(conn, "board_members", col):
                conn.execute(ddl)
                print(f"✓ Añadida columna board_members.{col}")

def ensure_partners(conn):
    if not col_exists(conn, "partners", "fecha_nacimiento_constitucion"):
        conn.execute("ALTER TABLE partners ADD COLUMN fecha_nacimiento_constitucion TEXT")
        print("✓ Añadida columna partners.fecha_nacimiento_constitucion")

def ensure_events(conn):
    # Estas columnas las espera la v2 para mostrar/editar eventos
    for col, ddl in [
        ("socio_origen_id",  "ALTER TABLE events ADD COLUMN socio_origen_id INTEGER"),
        ("socio_destino_id", "ALTER TABLE events ADD COLUMN socio_destino_id INTEGER"),
        ("referencia",       "ALTER TABLE events ADD COLUMN referencia TEXT"),
    ]:
        if not col_exists(conn, "events", col):
            conn.execute(ddl)
            print(f"✓ Añadida columna events.{col}")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"No encuentro la base de datos en: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        try:
            ensure_board_members(conn)
            ensure_partners(conn)
            ensure_events(conn)
            conn.commit()
            print("✅ Migración completada")
        finally:
            conn.execute("PRAGMA foreign_keys=ON")

if __name__ == "__main__":
    main()