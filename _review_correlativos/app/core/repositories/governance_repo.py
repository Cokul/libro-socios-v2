from typing import Optional
from ...infra.db import get_connection
from .base import rows_to_dicts

def list_board(company_id: int) -> list[dict]:
    with get_connection() as conn:
        cur = conn.execute("""
            SELECT id, company_id, nombre, cargo, nif, direccion, telefono, email
            FROM board_members WHERE company_id=? ORDER BY id
        """, (company_id,))
        return rows_to_dicts(cur.fetchall())

def upsert_board_member(*, id: Optional[int], company_id: int, nombre: str, cargo: str, nif: str,
                        direccion: Optional[str], telefono: Optional[str], email: Optional[str]) -> int:
    with get_connection() as conn:
        if id:
            conn.execute(
                """UPDATE board_members
                   SET nombre=?, cargo=?, nif=?, direccion=?, telefono=?, email=?
                 WHERE id=? AND company_id=?""",
                (nombre, cargo, nif, direccion, telefono, email, id, company_id)
            )
            return id
        else:
            cur = conn.execute(
                """INSERT INTO board_members(company_id, nombre, cargo, nif, direccion, telefono, email)
                   VALUES(?,?,?,?,?,?,?)""",
                (company_id, nombre, cargo, nif, direccion, telefono, email)
            )
            return cur.lastrowid

def get_company_governance(company_id: int) -> dict | None:
    """Devuelve organo y firmantes_json desde companies."""
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT organo, firmantes_json FROM companies WHERE id=?",
            (company_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None