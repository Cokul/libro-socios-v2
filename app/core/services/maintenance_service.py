# app/core/services/maintenance_service.py
from __future__ import annotations
from typing import Optional, Literal

from app.infra.db import get_connection
from app.infra.healthcheck import integrity_check, foreign_key_check, quick_summary

from app.core.repositories import events_repo, partners_repo
from app.core.services import governance_service


def run_analyze() -> None:
    with get_connection() as conn:
        conn.execute("ANALYZE;")

def run_reindex() -> None:
    with get_connection() as conn:
        conn.execute("REINDEX;")

def run_vacuum() -> None:
    with get_connection() as conn:
        conn.execute("VACUUM;")

# Re-export helpers
def db_integrity_check(): return integrity_check()
def db_fk_check(): return foreign_key_check()
def db_quick_summary(): return quick_summary()


# ----------------------------
# Correlativos existentes
# ----------------------------
def recompute_events_correlativos(company_id: Optional[int] = None) -> int:
    return events_repo.recompute_correlativo(company_id)

def recompute_partners_correlativos(company_id: Optional[int] = None) -> int:
    return partners_repo.recompute_partner_no(company_id)

# ----------------------------
# NUEVO: correlativos gobernanza
# ----------------------------
def recompute_governance_correlativos(company_id: Optional[int] = None) -> int:
    return governance_service.recompute_board_numbers(company_id)


def recompute_correlativos(
    *,
    company_id: Optional[int] = None,
    scope: Literal["events", "partners", "governance", "both", "all"] = "both"
) -> dict:
    """
    Ejecuta recompute según ámbito seleccionado.
    - "both": events + partners
    - "all": events + partners + governance
    - "events" | "partners" | "governance": solo ese
    Devuelve {'events': n, 'partners': m, 'governance': g}
    """
    out = {"events": 0, "partners": 0, "governance": 0}

    if scope in ("both", "all", "events"):
        out["events"] = recompute_events_correlativos(company_id)
    if scope in ("both", "all", "partners"):
        out["partners"] = recompute_partners_correlativos(company_id)
    if scope in ("all", "governance"):
        out["governance"] = recompute_governance_correlativos(company_id)

    return out

# === Índices mínimos recomendados ===
import sqlite3
from typing import Dict, List, Tuple
from app.infra.db import get_connection

def _have_index(conn: sqlite3.Connection, table: str, idx_name: str) -> bool:
    row = conn.execute("PRAGMA index_list(%s)" % table).fetchall()
    names = {r[1] for r in row} if row else set()
    return idx_name in names

def ensure_min_indexes() -> Dict[str, str]:
    """
    Crea (si no existen) los índices mínimos:
      - events(company_id, fecha, id)
      - partners(company_id)
    Además, si tu esquema los usa, es útil:
      - events(company_id, correlativo)
      - board_members(company_id)
    Devuelve un dict {nombre_indice: 'created'|'exists'}.
    """
    targets: List[Tuple[str, str, str]] = [
        # table, index_name, create_sql
        ("events", "idx_events_company_fecha_id",
         "CREATE INDEX IF NOT EXISTS idx_events_company_fecha_id ON events(company_id, fecha, id)"),
        ("partners", "idx_partners_company",
         "CREATE INDEX IF NOT EXISTS idx_partners_company ON partners(company_id)"),
        # Opcionales pero recomendados si se usan mucho en consultas/UI:
        ("events", "idx_events_company_correlativo",
         "CREATE INDEX IF NOT EXISTS idx_events_company_correlativo ON events(company_id, correlativo)"),
        ("board_members", "idx_board_members_company",
         "CREATE INDEX IF NOT EXISTS idx_board_members_company ON board_members(company_id)"),
    ]

    results: Dict[str, str] = {}
    with get_connection() as conn:
        for table, idx, sql in targets:
            try:
                existed = _have_index(conn, table, idx)
            except Exception:
                # Si falla PRAGMA (tabla puede no existir), intenta crear y que sea IF NOT EXISTS
                existed = False
            try:
                conn.execute(sql)
                # Si ya existía, SQLite igualmente no falla por el IF NOT EXISTS
                results[idx] = "exists" if existed else "created"
            except Exception as e:
                results[idx] = f"error: {e}"
        conn.commit()
    return results

# app/core/services/maintenance_service.py  (añade al final)
from .normalization_service import recompute_denormalized as _recompute_denorm

def recompute_denormalized(company_id: Optional[int] = None) -> dict:
    """
    Wrapper para lanzar el recompute de columnas auxiliares (si existen).
    """
    return _recompute_denorm(company_id)