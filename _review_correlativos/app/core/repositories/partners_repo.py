#app/core/repositories/partners_repo.py

from typing import Optional
from ...infra.db import get_connection
from .base import rows_to_dicts

def list_by_company(company_id: int) -> list[dict]:
    with get_connection() as conn:
        conn.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
        cur = conn.execute("""
            SELECT id, company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
            FROM partners WHERE company_id=? ORDER BY nombre
        """, (company_id,))
        return cur.fetchall()

def upsert_partner(*, id: Optional[int], company_id: int, nombre: str, nif: str,
                   domicilio: Optional[str], nacionalidad: Optional[str],
                   fecha_nacimiento_constitucion: Optional[str]) -> int:
    with get_connection() as conn:
        if id:
            conn.execute(
                """UPDATE partners
                   SET nombre=?, nif=?, domicilio=?, nacionalidad=?, fecha_nacimiento_constitucion=?
                 WHERE id=? AND company_id=?""",
                (nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion, id, company_id)
            )
            return id
        else:
            cur = conn.execute(
                """INSERT INTO partners(company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion)
                   VALUES(?,?,?,?,?,?)""",
                (company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion)
            )
            return cur.lastrowid
        
def get_partner(company_id: int, partner_id: int) -> dict | None:
    from ...infra.db import get_connection
    with get_connection() as conn:
        cur = conn.execute("""
            SELECT id, company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
            FROM partners WHERE id=? AND company_id=?
        """, (partner_id, company_id))
        row = cur.fetchone()
        return dict(row) if row else None