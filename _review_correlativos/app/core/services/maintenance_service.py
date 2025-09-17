# app/core/services/maintenance_service.py
from __future__ import annotations
from app.infra.db import get_connection
from app.infra.healthcheck import integrity_check, foreign_key_check, quick_summary

def run_analyze() -> None:
    with get_connection() as conn:
        conn.execute("ANALYZE;")

def run_reindex() -> None:
    with get_connection() as conn:
        conn.execute("REINDEX;")

def run_vacuum() -> None:
    with get_connection() as conn:
        conn.execute("VACUUM;")

# Re-export helpers de healthcheck (para importar todo desde un sitio)
def db_integrity_check(): return integrity_check()
def db_fk_check(): return foreign_key_check()
def db_quick_summary(): return quick_summary()