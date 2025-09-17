# app/core/repositories/partners_repo.py

from __future__ import annotations
from typing import Optional, Iterable, Tuple
import sqlite3

from ...infra.db import get_connection
from .base import rows_to_dicts


# ----------------------------
# Helpers internos (no export)
# ----------------------------
def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    out = set()
    for r in rows:
        try:
            out.add(r["name"])
        except Exception:
            out.add(r[1])
    return out


def _ensure_partner_no_schema(conn: sqlite3.Connection) -> None:
    """Añade columna partner_no e índices si faltan. Idempotente."""
    have = _cols(conn, "partners")
    if "partner_no" not in have:
        conn.execute("ALTER TABLE partners ADD COLUMN partner_no INTEGER;")
    # Índices (idempotentes)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_partners_company ON partners(company_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_partners_company_partnerno ON partners(company_id, partner_no);")
    # Único opcional (comenta si prefieres evitar uniqueness estricta):
    # conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_partners_company_partnerno ON partners(company_id, partner_no);")


def _sqlite_supports_row_number(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn;")
        return True
    except Exception:
        return False


# ---------------------------------
# API pública (usada por servicios)
# ---------------------------------
def list_by_company(company_id: int) -> list[dict]:
    """
    Devuelve los socios de la compañía.
    Si existe partner_no, ordena por partner_no NULLS LAST y devuelve la columna.
    Si no existe, ordena por nombre (comportamiento anterior).
    """
    with get_connection() as conn:
        have = _cols(conn, "partners")
        conn.row_factory = sqlite3.Row

        if "partner_no" in have:
            sql = """
                SELECT id, company_id, COALESCE(partner_no, NULL) AS partner_no,
                       nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
                FROM partners
                WHERE company_id=?
                ORDER BY CASE WHEN partner_no IS NULL THEN 1 ELSE 0 END, partner_no, nombre
            """
        else:
            sql = """
                SELECT id, company_id,
                       nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
                FROM partners
                WHERE company_id=? ORDER BY nombre
            """

        cur = conn.execute(sql, (company_id,))
        return [dict(r) for r in cur.fetchall()]


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
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        have = _cols(conn, "partners")
        if "partner_no" in have:
            sql = """
                SELECT id, company_id, partner_no, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
                FROM partners WHERE id=? AND company_id=?
            """
        else:
            sql = """
                SELECT id, company_id, nombre, nif, domicilio, nacionalidad, fecha_nacimiento_constitucion
                FROM partners WHERE id=? AND company_id=?
            """
        row = conn.execute(sql, (partner_id, company_id)).fetchone()
        return dict(row) if row else None


# -------------------------------------------
# NUEVO: Recompute correlativos (partner_no)
# -------------------------------------------
def recompute_partner_no(company_id: Optional[int] = None) -> int:
    """
    Recalcula partner_no por sociedad con orden estable por id ASC.
    Si 'company_id' es None, lo hace para todas las compañías.
    Devuelve nº de filas actualizadas (estimado).
    """
    updated = 0
    with get_connection() as conn:
        _ensure_partner_no_schema(conn)
        conn.row_factory = sqlite3.Row

        if company_id is None:
            companies = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]
        else:
            companies = [company_id]

        if _sqlite_supports_row_number(conn):
            for cid in companies:
                # Ventanas (rápido y atómico por compañía)
                conn.execute("""
                    WITH ranked AS (
                        SELECT id,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY id) AS rn
                        FROM partners
                        WHERE company_id=?
                    )
                    UPDATE partners
                       SET partner_no = (SELECT rn FROM ranked WHERE ranked.id = partners.id)
                     WHERE company_id=?;
                """, (cid, cid))
                updated += conn.total_changes or 0
        else:
            # Fallback sin ROW_NUMBER()
            for cid in companies:
                ids = [r["id"] for r in conn.execute(
                    "SELECT id FROM partners WHERE company_id=? ORDER BY id", (cid,)
                ).fetchall()]
                for i, pid in enumerate(ids, start=1):
                    conn.execute("UPDATE partners SET partner_no=? WHERE id=?", (i, pid))
                    updated += 1

        conn.commit()
        return updated