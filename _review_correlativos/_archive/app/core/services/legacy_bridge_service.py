
"""
Bridge hacia la lógica v1 sin reescribirla.
Importa dinámicamente app.legacy_v1.services y expone funciones finas.
"""

import sys
from pathlib import Path
from importlib import import_module

# Asegura que el paquete app.legacy_v1 es importable si v2 se ejecuta desde raíz del proyecto
# (el archivo __init__.py ya está creado en app/legacy_v1)
def _legacy():
    return import_module("app.legacy_v1.services")

# ------------------- Lecturas básicas -------------------
def list_companies():
    svc = _legacy()
    with svc.get_connection() as conn:  # usa la conexión v1
        cur = conn.execute("SELECT id, name, cif FROM companies ORDER BY id")
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def list_partners(company_id: int):
    svc = _legacy()
    with svc.get_connection() as conn:
        cur = conn.execute("SELECT id, company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion FROM partners WHERE company_id=? ORDER BY id", (company_id,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def list_board(company_id: int):
    svc = _legacy()
    with svc.get_connection() as conn:
        cur = conn.execute("SELECT id, company_id, nombre, cargo, nif, direccion, telefono, email FROM board_members WHERE company_id=? ORDER BY id", (company_id,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def list_events(company_id: int):
    svc = _legacy()
    with svc.get_connection() as conn:
        cur = conn.execute("SELECT id, company_id, tipo, fecha, socio_origen_id, socio_destino_id, n_participaciones, referencia FROM events WHERE company_id=? ORDER BY fecha, id", (company_id,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

# ------------------- Escrituras mínimas (delegando en v1 si existen) -------------------
def upsert_partner(*, id, company_id, nombre, nif, domicilio=None, nacionalidad=None, fecha_nacimiento_constitucion=None):
    svc = _legacy()
    # Si v1 ya tiene helpers para alta/edición de socio, llama a esas funciones; si no, ejecuta SQL directo
    with svc.get_connection() as conn:
        if id:
            conn.execute(
                "UPDATE partners SET nombre=?, nif=?, domicilio=?, nacionalidad=?, fecha_nacimiento_constitucion=? WHERE id=? AND company_id=?",
                (nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion, id, company_id)
            )
            return id
        else:
            cur = conn.execute(
                "INSERT INTO partners(company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion) VALUES(?,?,?,?,?,?)",
                (company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion)
            )
            return cur.lastrowid

def upsert_board_member(*, id, company_id, nombre, cargo, nif, direccion=None, telefono=None, email=None):
    svc = _legacy()
    with svc.get_connection() as conn:
        if id:
            conn.execute(
                "UPDATE board_members SET nombre=?, cargo=?, nif=?, direccion=?, telefono=?, email=? WHERE id=? AND company_id=?",
                (nombre, cargo, nif, direccion, telefono, email, id, company_id)
            )
            return id
        else:
            cur = conn.execute(
                "INSERT INTO board_members(company_id, nombre, cargo, nif, direccion, telefono, email) VALUES(?,?,?,?,?,?,?)",
                (company_id, nombre, cargo, nif, direccion, telefono, email)
            )
            return cur.lastrowid
