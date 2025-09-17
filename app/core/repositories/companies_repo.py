# app/core/repositories/companies_repo.py
from __future__ import annotations
from typing import Optional
from ...infra.db import get_connection

def _dict_row_factory(cursor, row):
    return {cursor.description[i][0]: row[i] for i in range(len(row))}

def list_companies() -> list[dict]:
    with get_connection() as conn:
        conn.row_factory = _dict_row_factory
        cur = conn.execute("""
            SELECT id, name, cif, domicilio, fecha_constitucion, valor_nominal, participaciones_totales
            FROM companies
            ORDER BY id
        """)
        return cur.fetchall()

def get_company(company_id: int) -> Optional[dict]:
    with get_connection() as conn:
        conn.row_factory = _dict_row_factory
        cur = conn.execute("""
            SELECT id, name, cif, domicilio, fecha_constitucion, valor_nominal, participaciones_totales
            FROM companies
            WHERE id = ?
        """, (company_id,))
        row = cur.fetchone()
        return row if row else None

def insert_company(
    *,
    name: str,
    cif: str,
    domicilio: Optional[str],
    fecha_constitucion: Optional[str],
    valor_nominal: float,
    participaciones_totales: int,
) -> int:
    with get_connection() as conn:
        cur = conn.execute("""
            INSERT INTO companies(name, cif, domicilio, fecha_constitucion, valor_nominal, participaciones_totales)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name.strip(), cif.strip(), domicilio, fecha_constitucion, float(valor_nominal), int(participaciones_totales)))
        return cur.lastrowid

def update_company(
    *,
    id: int,
    name: str,
    cif: str,
    domicilio: Optional[str],
    fecha_constitucion: Optional[str],
    valor_nominal: float,
    participaciones_totales: int,
) -> None:
    with get_connection() as conn:
        conn.execute("""
            UPDATE companies
               SET name = ?,
                   cif = ?,
                   domicilio = ?,
                   fecha_constitucion = ?,
                   valor_nominal = ?,
                   participaciones_totales = ?
             WHERE id = ?
        """, (name.strip(), cif.strip(), domicilio, fecha_constitucion, float(valor_nominal), int(participaciones_totales), id))

def delete_company(company_id: int) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))