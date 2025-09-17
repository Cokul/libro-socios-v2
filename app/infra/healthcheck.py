# app/infra/healthcheck.py
from __future__ import annotations
from typing import List, Tuple, Any
import sqlite3
from app.infra.db import get_connection

def _first_cell(row: Any) -> str:
    """
    Devuelve el primer valor de la fila (soporta tuple, dict, sqlite3.Row).
    Si falla, devuelve str(row) como último recurso.
    """
    # tuple / list
    if isinstance(row, (tuple, list)) and row:
        return row[0]
    # sqlite3.Row (soporta indexado por posición y clave)
    try:
        if isinstance(row, sqlite3.Row):
            # primero por posición
            try:
                return row[0]
            except Exception:
                # luego por nombre habitual del PRAGMA
                for k in ("integrity_check",):
                    try:
                        return row[k]
                    except Exception:
                        pass
                return str(dict(row))  # última opción legible
    except Exception:
        pass
    # dict
    if isinstance(row, dict):
        # intenta claves habituales
        for k in ("integrity_check",):
            if k in row:
                return row[k]
        # o el primer valor
        if row:
            return next(iter(row.values()))
    # fallback
    return str(row)

def integrity_check() -> List[str]:
    """
    PRAGMA integrity_check; devuelve lista de problemas (vacía si todo 'ok').
    """
    out: List[str] = []
    with get_connection() as conn:
        # forzamos Row para tener un formato estable
        conn.row_factory = sqlite3.Row
        cur = conn.execute("PRAGMA integrity_check;")
        rows = cur.fetchall()
        for r in rows:
            val = str(_first_cell(r)).strip()
            if val.lower() != "ok":
                out.append(val)
    return out

def foreign_key_check() -> List[Tuple[str, int, str, int]]:
    """
    PRAGMA foreign_key_check; devuelve (tabla, rowid, parent, fkid).
    Vacío si no hay violaciones o si FK no está habilitado.
    """
    issues: List[Tuple[str, int, str, int]] = []
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute("PRAGMA foreign_key_check;")
            for r in cur.fetchall():
                # el orden típico es: table, rowid, parent, fkid
                def _idx(i, default=None):
                    try:
                        return r[i]
                    except Exception:
                        return default
                table  = str(_idx(0, "") or "")
                rowid  = int(_idx(1, -1) or -1)
                parent = str(_idx(2, "") or "")
                fkid   = int(_idx(3, -1) or -1)
                issues.append((table, rowid, parent, fkid))
        except Exception:
            # PRAGMA no disponible o FK desactivadas
            return []
    return issues

def quick_summary() -> dict:
    integ = integrity_check()
    fks   = foreign_key_check()
    return {
        "integrity_ok": len(integ) == 0,
        "fk_ok": len(fks) == 0,
        "integrity_messages": integ,
        "fk_violations": fks,
    }