# app/core/repositories/governance_repo.py
from __future__ import annotations
import sqlite3
from typing import Optional, List, Dict

from ...infra.db import get_connection


# ----------------------------
# Helpers internos
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


def _ensure_board_no_schema(conn: sqlite3.Connection) -> None:
    """Asegura columna board_no e índices en board_members. Idempotente."""
    have = _cols(conn, "board_members")
    if "board_no" not in have:
        conn.execute("ALTER TABLE board_members ADD COLUMN board_no INTEGER;")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_board_members_company ON board_members(company_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_board_members_company_no ON board_members(company_id, board_no);")
    # Unicidad opcional:
    # conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_board_members_company_no ON board_members(company_id, board_no);")


def _supports_row_number(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn;")
        return True
    except Exception:
        return False


# ----------------------------
# Lectura de metadatos compañía
# ----------------------------
def get_company_governance(company_id: int) -> Dict | None:
    """
    Devuelve {'organo': str|None, 'firmantes_json': str|None} desde companies.
    Las columnas existen tras tus migraciones V0x.
    """
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT organo, firmantes_json FROM companies WHERE id=?",
            (company_id,)
        ).fetchone()
        return dict(row) if row else None


# ----------------------------
# CRUD básico de board_members
# ----------------------------
def upsert_board_member(
    *,
    id: Optional[int],
    company_id: int,
    nombre: str,
    cargo: str,
    nif: str,
    direccion: Optional[str],
    telefono: Optional[str],
    email: Optional[str],
) -> int:
    """
    Inserta/actualiza un consejero en board_members.
    Si 'id' es None o 0, inserta; si no, actualiza esa fila (scoped por company_id).
    """
    with get_connection() as conn:
        if id:
            conn.execute(
                """UPDATE board_members
                   SET nombre=?, cargo=?, nif=?, direccion=?, telefono=?, email=?
                 WHERE id=? AND company_id=?""",
                (nombre, cargo, nif, direccion, telefono, email, id, company_id)
            )
            return int(id)
        else:
            cur = conn.execute(
                """INSERT INTO board_members
                   (company_id, nombre, cargo, nif, direccion, telefono, email)
                   VALUES(?,?,?,?,?,?,?)""",
                (company_id, nombre, cargo, nif, direccion, telefono, email)
            )
            return int(cur.lastrowid)


# ----------------------------
# Listados / lectura
# ----------------------------
def list_board(company_id: int) -> List[Dict]:
    """Devuelve miembros del consejo; si existe board_no, ordena por board_no."""
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        have = _cols(conn, "board_members")
        if "board_no" in have:
            sql = """
                SELECT id, company_id, board_no, nombre, cargo, nif, direccion, telefono, email
                FROM board_members
                WHERE company_id=?
                ORDER BY CASE WHEN board_no IS NULL THEN 1 ELSE 0 END, board_no, nombre
            """
        else:
            sql = """
                SELECT id, company_id, nombre, cargo, nif, direccion, telefono, email
                FROM board_members
                WHERE company_id=?
                ORDER BY nombre
            """
        rows = conn.execute(sql, (company_id,)).fetchall()
        return [dict(r) for r in rows]


def get_member(company_id: int, member_id: int) -> Dict | None:
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        have = _cols(conn, "board_members")
        if "board_no" in have:
            sql = """
                SELECT id, company_id, board_no, nombre, cargo, nif, direccion, telefono, email
                FROM board_members WHERE company_id=? AND id=?
            """
        else:
            sql = """
                SELECT id, company_id, nombre, cargo, nif, direccion, telefono, email
                FROM board_members WHERE company_id=? AND id=?
            """
        row = conn.execute(sql, (company_id, member_id)).fetchone()
        return dict(row) if row else None


# ----------------------------
# Recompute correlativo consejo
# ----------------------------
def recompute_board_no(company_id: Optional[int] = None) -> int:
    """
    Recalcula board_no por sociedad con orden estable por id ASC.
    Si company_id es None, lo hace para todas.
    Devuelve nº de filas actualizadas aproximado.
    """
    updated = 0
    with get_connection() as conn:
        _ensure_board_no_schema(conn)
        conn.row_factory = sqlite3.Row

        if company_id is None:
            companies = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]
        else:
            companies = [company_id]

        if _supports_row_number(conn):
            for cid in companies:
                conn.execute("""
                    WITH ranked AS (
                        SELECT id,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY id) AS rn
                        FROM board_members
                        WHERE company_id=?
                    )
                    UPDATE board_members
                       SET board_no = (SELECT rn FROM ranked WHERE ranked.id = board_members.id)
                     WHERE company_id=?;
                """, (cid, cid))
                updated += conn.total_changes or 0
        else:
            for cid in companies:
                ids = [r["id"] for r in conn.execute(
                    "SELECT id FROM board_members WHERE company_id=? ORDER BY id", (cid,)
                ).fetchall()]
                for i, mid in enumerate(ids, start=1):
                    conn.execute("UPDATE board_members SET board_no=? WHERE id=?", (i, mid))
                    updated += 1

        conn.execute("CREATE INDEX IF NOT EXISTS ix_board_members_company_no ON board_members(company_id, board_no);")
        conn.commit()
        return updated