# tools/migrate_events_populate.py
from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "libro_socios.db"

EVENT_QTY_CANDIDATES = [
    "n_participaciones", "num_participaciones", "num_acciones", "cantidad", "participaciones", "n_acciones",
]

ORIGIN_ID_CANDIDATES  = ["socio_origen_id", "origen_id", "partner_from_id", "from_partner_id"]
DEST_ID_CANDIDATES    = ["socio_destino_id", "destino_id", "partner_to_id", "to_partner_id"]

ORIGIN_NIF_CANDIDATES = ["origen_nif", "from_nif", "socio_origen_nif"]
DEST_NIF_CANDIDATES   = ["destino_nif", "to_nif", "socio_destino_nif"]

ORIGIN_NAME_CANDIDATES = ["origen_nombre", "from_name", "socio_origen_nombre"]
DEST_NAME_CANDIDATES   = ["destino_nombre", "to_name", "socio_destino_nombre"]

REF_CANDIDATES = ["referencia", "escritura", "observaciones", "nota", "comentario"]

def table_cols(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def ensure_col(conn, table, col, ddl):
    cols = table_cols(conn, table)
    if col not in cols:
        conn.execute(ddl)
        print(f"✓ Añadida columna {table}.{col}")

def first_existing(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def find_partner_id_by(conn, company_id, *, nif=None, nombre=None):
    if nif:
        cur = conn.execute("SELECT id FROM partners WHERE company_id=? AND UPPER(REPLACE(nif,' ',''))=UPPER(REPLACE(?, ' ', '')) LIMIT 1", (company_id, nif))
        r = cur.fetchone()
        if r: return r[0]
    if nombre:
        cur = conn.execute("SELECT id FROM partners WHERE company_id=? AND nombre=? LIMIT 1", (company_id, nombre))
        r = cur.fetchone()
        if r: return r[0]
    return None

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"No encuentro la base de datos en {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")

        # 1) Asegurar columnas nuevas
        ensure_col(conn, "events", "n_participaciones", "ALTER TABLE events ADD COLUMN n_participaciones INTEGER")
        ensure_col(conn, "events", "socio_origen_id", "ALTER TABLE events ADD COLUMN socio_origen_id INTEGER")
        ensure_col(conn, "events", "socio_destino_id", "ALTER TABLE events ADD COLUMN socio_destino_id INTEGER")
        ensure_col(conn, "events", "referencia", "ALTER TABLE events ADD COLUMN referencia TEXT")

        ev_cols = table_cols(conn, "events")

        # Columnas fuente para cantidad/IDs
        qty_src = first_existing(ev_cols, [c for c in EVENT_QTY_CANDIDATES if c != "n_participaciones"])  # evita auto-copiarse
        orig_id_src = first_existing(ev_cols, [c for c in ORIGIN_ID_CANDIDATES if c != "socio_origen_id"])
        dest_id_src = first_existing(ev_cols, [c for c in DEST_ID_CANDIDATES if c != "socio_destino_id"])

        orig_nif_src  = first_existing(ev_cols, ORIGIN_NIF_CANDIDATES)
        dest_nif_src  = first_existing(ev_cols, DEST_NIF_CANDIDATES)
        orig_name_src = first_existing(ev_cols, ORIGIN_NAME_CANDIDATES)
        dest_name_src = first_existing(ev_cols, DEST_NAME_CANDIDATES)

        ref_sources = [c for c in REF_CANDIDATES if c in ev_cols and c != "referencia"]

        # 2) Itera eventos y rellena campos
        cur = conn.execute("SELECT * FROM events ORDER BY id")
        rows = cur.fetchall()
        updated = 0

        for r in rows:
            rid = r["id"]
            company_id = r["company_id"]

            n_part = r["n_participaciones"] if "n_participaciones" in r.keys() else None
            if n_part is None and qty_src:
                n_part = r[qty_src]

            # origen/destino id
            s_origen = r["socio_origen_id"] if "socio_origen_id" in r.keys() else None
            s_dest   = r["socio_destino_id"] if "socio_destino_id" in r.keys() else None

            if s_origen is None:
                if orig_id_src and r[orig_id_src] is not None:
                    s_origen = r[orig_id_src]
                elif orig_nif_src and r[orig_nif_src]:
                    s_origen = find_partner_id_by(conn, company_id, nif=r[orig_nif_src])
                elif orig_name_src and r[orig_name_src]:
                    s_origen = find_partner_id_by(conn, company_id, nombre=r[orig_name_src])

            if s_dest is None:
                if dest_id_src and r[dest_id_src] is not None:
                    s_dest = r[dest_id_src]
                elif dest_nif_src and r[dest_nif_src]:
                    s_dest = find_partner_id_by(conn, company_id, nif=r[dest_nif_src])
                elif dest_name_src and r[dest_name_src]:
                    s_dest = find_partner_id_by(conn, company_id, nombre=r[dest_name_src])

            # referencia
            ref_val = r["referencia"] if "referencia" in r.keys() else None
            if (not ref_val) and ref_sources:
                parts = [str(r[c]) for c in ref_sources if r[c]]
                ref_val = " | ".join(parts) if parts else None

            # aplica update si hay algo que escribir
            if (n_part is not None) or (s_origen is not None) or (s_dest is not None) or (ref_val is not None):
                conn.execute("""
                    UPDATE events SET 
                        n_participaciones = COALESCE(?, n_participaciones),
                        socio_origen_id   = COALESCE(?, socio_origen_id),
                        socio_destino_id  = COALESCE(?, socio_destino_id),
                        referencia        = COALESCE(?, referencia)
                    WHERE id=?
                """, (n_part, s_origen, s_dest, ref_val, rid))
                updated += 1

        conn.commit()
        conn.execute("PRAGMA foreign_keys=ON")
        print(f"✅ Migración completada. Filas actualizadas en events: {updated}")

if __name__ == "__main__":
    main()