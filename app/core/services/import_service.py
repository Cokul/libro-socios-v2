# app/core/services/import_service.py
from __future__ import annotations
from typing import Any, Optional, Iterable, Dict, List, Tuple
import sqlite3
from dataclasses import dataclass

from app.infra.db import get_connection

# --- (deja aquí el resto de utilidades que ya tengas) ---

# =========================
# Helpers de esquema dinámico
# =========================
def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]

def _importable_cols(conn: sqlite3.Connection, table: str, *, exclude: Iterable[str]) -> List[str]:
    ex = set(exclude)
    return [c for c in _table_columns(conn, table) if c not in ex]

def _filter_row_to_cols(row: Dict[str, Any], cols: Iterable[str]) -> Dict[str, Any]:
    cols_set = set(cols)
    return {k: row.get(k) for k in row.keys() if k in cols_set}

# =========================
# Tipos de retorno (si ya los tienes, reutiliza los tuyos)
# =========================
@dataclass
class CommitSummary:
    inserted: int = 0
    updated: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

# =========================
# commit() – versión robusta
# =========================
def commit(kind: str, company_id: int, rows: List[Dict[str, Any]]) -> CommitSummary:
    """
    Inserta/actualiza utilizando **solo** las columnas reales de la tabla
    (intersección dinámica), ignorando silenciosamente columnas extra.
    - partners: ignora id/company_id y campos inexistentes como participaciones_totales
    - events: ignora id/company_id/correlativo (lo calcula la app)
    """
    summary = CommitSummary()

    if not rows:
        return summary

    try:
        with get_connection() as conn:
            if kind == "partners":
                excl = {"id", "company_id"}    # <- NO participaciones_totales
                cols = _importable_cols(conn, "partners", exclude=excl)

                for r in rows:
                    data = _filter_row_to_cols(r, cols)
                    # añade company_id siempre
                    data["company_id"] = company_id

                    # estrategia simple: si existe (company_id, nif) o (company_id, nombre) -> UPDATE; si no -> INSERT
                    # ajusta a tu criterio de unicidad
                    where_id = None
                    if data.get("nif"):
                        q = conn.execute(
                            "SELECT id FROM partners WHERE company_id=? AND nif=? LIMIT 1",
                            (company_id, str(data["nif"]).strip()),
                        ).fetchone()
                        if q:
                            where_id = int(q[0])
                    if where_id is None and data.get("nombre"):
                        q = conn.execute(
                            "SELECT id FROM partners WHERE company_id=? AND nombre=? LIMIT 1",
                            (company_id, str(data["nombre"]).strip()),
                        ).fetchone()
                        if q:
                            where_id = int(q[0])

                    if where_id is None:
                        # INSERT dinámico
                        cols_ins = list(data.keys())
                        placeholders = ",".join(["?"] * len(cols_ins))
                        sql = f"INSERT INTO partners({','.join(cols_ins)}) VALUES({placeholders})"
                        conn.execute(sql, tuple(data[c] for c in cols_ins))
                        summary.inserted += 1
                    else:
                        # UPDATE dinámico (no tocar company_id en SET)
                        set_cols = [c for c in data.keys() if c != "company_id"]
                        if set_cols:
                            sets = ", ".join([f"{c}=?" for c in set_cols])
                            sql = f"UPDATE partners SET {sets} WHERE id=?"
                            params = [data[c] for c in set_cols] + [where_id]
                            conn.execute(sql, params)
                        summary.updated += 1

            elif kind == "events":
                excl = {"id", "company_id", "correlativo"}
                cols = _importable_cols(conn, "events", exclude=excl)

                for r in rows:
                    data = _filter_row_to_cols(r, cols)
                    data["company_id"] = company_id

                    # INSERT puro para events (el correlativo lo recalcula la app)
                    cols_ins = list(data.keys())
                    placeholders = ",".join(["?"] * len(cols_ins))
                    sql = f"INSERT INTO events({','.join(cols_ins)}) VALUES({placeholders})"
                    conn.execute(sql, tuple(data[c] for c in cols_ins))
                    summary.inserted += 1

            else:
                summary.errors.append(f"Ámbito no soportado: {kind}")

    except Exception as e:
        summary.errors.append(str(e))

    return summary