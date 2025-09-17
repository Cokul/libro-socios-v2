# tests/test_normalization_denorm.py
import types
import sqlite3
from app.core.services import normalization_service as ns

def _setup_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE partners (
        id INTEGER PRIMARY KEY,
        company_id INTEGER NOT NULL,
        nombre TEXT,
        search_name TEXT,
        name_ascii TEXT
    );
    """)
    conn.commit()

def test_recompute_denormalized(monkeypatch, inmemory_conn):
    _setup_schema(inmemory_conn)
    cur = inmemory_conn.cursor()
    # Inserta nombres con tildes/ruido
    cur.executemany(
        "INSERT INTO partners (id, company_id, nombre, search_name, name_ascii) VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10, "  José   Pérez  ", None, None),
            (2, 10, "ÁLICE, S.L.",    None, None),
            (3, 11, None,             None, None),  # nombre vacío -> no cambia
        ],
    )
    inmemory_conn.commit()

    # monkeypatch del get_connection usado EN ESTE MÓDULO
    def _fake_get_connection():
        return inmemory_conn
    monkeypatch.setattr(ns, "get_connection", _fake_get_connection, raising=True)

    # Ejecuta para company_id=10 (no debe tocar la 11)
    out = ns.recompute_denormalized(company_id=10)
    part = out["partners"]
    assert part["examined"] == 2  # solo company_id=10
    assert part["updated"] >= 1
    # Muestras razonables
    assert isinstance(part["details"], list)
    assert len(part["details"]) <= 25

    # Verifica resultados
    rows = inmemory_conn.execute(
        "SELECT id, search_name, name_ascii FROM partners ORDER BY id"
    ).fetchall()
    row1 = rows[0]
    row2 = rows[1]
    # id=1
    assert row1[1] == "jose perez"
    assert row1[2] == "jose perez"
    # id=2  (ojo: name_ascii elimina signos pero respeta 's.l' tras pasar por build_name_ascii)
    assert row2[1] == "alice s.l."
    assert row2[2] == "alice sl"