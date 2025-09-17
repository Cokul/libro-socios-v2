# app/core/repositories/events_repo.py

import sqlite3
from typing import Optional
from ...infra.db import get_connection

import logging
log = logging.getLogger(__name__)

BASE_EVENT_COLS = [
    "id","company_id","correlativo","fecha","tipo",
    "socio_transmite","socio_adquiere",
    "rango_desde","rango_hasta",
    "nuevo_valor_nominal",
    "documento","observaciones",
    "hora","orden_del_dia","created_at","updated_at",
]

def _cols(conn, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    if not rows:
        return set()
    first = rows[0]
    if isinstance(first, dict):
        return {r.get("name") for r in rows if "name" in r}
    try:
        return {r[1] for r in rows}
    except Exception:
        try:
            return {r["name"] for r in rows}
        except Exception:
            return set()

def list_events_upto(company_id: int, fecha_max: Optional[str]) -> list[dict]:
    with get_connection() as conn:
        have = _cols(conn, "events")
        cols = [c for c in BASE_EVENT_COLS if c in have]
        where = "AND fecha<=?" if fecha_max else ""
        sql = f"SELECT {', '.join(cols)} FROM events WHERE company_id=? {where} ORDER BY fecha, id"
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, (company_id, fecha_max) if fecha_max else (company_id,))
        rows = [dict(r) for r in cur.fetchall()]
        # normaliza claves faltantes
        out = []
        for r in rows:
            d = {k: r.get(k) for k in cols}
            for k in BASE_EVENT_COLS:
                d.setdefault(k, None)
            out.append(d)
        return out

# Compat:
def list_events(company_id: int) -> list[dict]:
    return list_events_upto(company_id, None)

def _supports_row_number(conn: sqlite3.Connection) -> bool:
    """Devuelve True si la BD soporta ROW_NUMBER() OVER ..."""
    try:
        conn.execute("DROP TABLE IF EXISTS _tmp_probe;")
        conn.execute("""
            CREATE TEMP TABLE _tmp_probe AS
            SELECT id, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY fecha, id) AS rn
            FROM events
            LIMIT 1;
        """)
        conn.execute("DROP TABLE IF EXISTS _tmp_probe;")
        return True
    except Exception:
        return False

def recompute_correlativo(company_id: Optional[int] = None) -> int:
    """
    Recalcula y persiste el correlativo por compañía en la tabla events.
    Si 'company_id' es None, lo hace para todas.
    Devuelve el número de filas actualizadas.
    """
    updated = 0
    with get_connection() as conn:
        have = _cols(conn, "events")
        if "correlativo" not in have:
            # la columna no existe; no hacemos nada
            return 0

        conn.row_factory = sqlite3.Row

        if company_id is None:
            companies = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]
        else:
            companies = [company_id]

        if _supports_row_number(conn):
            for cid in companies:
                conn.execute("""
                    WITH ordered AS (
                        SELECT id,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY fecha, id) AS rn
                        FROM events
                        WHERE company_id=?
                    )
                    UPDATE events
                       SET correlativo = (SELECT rn FROM ordered WHERE ordered.id = events.id)
                     WHERE company_id=?;
                """, (cid, cid))
                updated += conn.total_changes
        else:
            # Fallback sin funciones ventana
            for cid in companies:
                cur = conn.execute("""
                    SELECT id
                    FROM events
                    WHERE company_id=?
                    ORDER BY fecha, id
                """, (cid,))
                rows = [r["id"] for r in cur.fetchall()]
                for i, eid in enumerate(rows, start=1):
                    conn.execute("UPDATE events SET correlativo=? WHERE id=?", (i, eid))
                    updated += 1

        conn.commit()
        
    log.info("Recompute correlativo company_id=%s updated_rows=%s",
         company_id if company_id is not None else "ALL", updated)
    
    return updated

def ensure_redenominacion_triggers() -> None:
    """
    Replica las validaciones SQL de V1 para REDENOMINACION, nominal y partes requeridas.
    Idempotente.
    """
    sql = r"""
    -- 1) Reglas de presencia mínima (similar a V1)
    CREATE TRIGGER IF NOT EXISTS trg_events_required_parties_ins
    BEFORE INSERT ON events
    BEGIN
        -- tipos que requieren adquirente/acreedor
        SELECT CASE
          WHEN NEW.tipo IN ('ALTA','AMPL_EMISION','TRANSMISION','PIGNORACION','EMBARGO','USUFRUCTO')
           AND NEW.socio_adquiere IS NULL
          THEN RAISE(ABORT, 'Falta socio adquirente/acreedor') END;

        -- tipos que requieren transmitente/titular
        SELECT CASE
          WHEN NEW.tipo IN ('TRANSMISION','BAJA','RED_AMORT','USUFRUCTO')
           AND NEW.socio_transmite IS NULL
          THEN RAISE(ABORT, 'Falta socio transmitente/titular') END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_required_parties_upd
    BEFORE UPDATE ON events
    BEGIN
        SELECT CASE
          WHEN NEW.tipo IN ('ALTA','AMPL_EMISION','TRANSMISION','PIGNORACION','EMBARGO','USUFRUCTO')
           AND NEW.socio_adquiere IS NULL
          THEN RAISE(ABORT, 'Falta socio adquirente/acreedor') END;

        SELECT CASE
          WHEN NEW.tipo IN ('TRANSMISION','BAJA','RED_AMORT','USUFRUCTO')
           AND NEW.socio_transmite IS NULL
          THEN RAISE(ABORT, 'Falta socio transmitente/titular') END;
    END;

    -- 2) Modo REDENOMINACION: o GLOBAL (sin rangos y sin socios) o POR BLOQUE (con rangos y con socio).
    CREATE TRIGGER IF NOT EXISTS trg_events_reden_mode_ins
    BEFORE INSERT ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN
            ((NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL) OR
             (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL)))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por bloque (con rangos y socio).')
        END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_reden_mode_upd
    BEFORE UPDATE ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN
            ((NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL) OR
             (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL)))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por bloque (con rangos y socio).')
        END;
    END;

    -- 3) Nominal obligatorio para AMPL_VALOR/RED_VALOR; en REDENOMINACION es opcional pero si se informa debe ser > 0
    CREATE TRIGGER IF NOT EXISTS trg_events_check_nominal_ins
    BEFORE INSERT ON events
    BEGIN
        SELECT CASE
          WHEN NEW.tipo IN ('AMPL_VALOR','RED_VALOR')
           AND (NEW.nuevo_valor_nominal IS NULL OR NEW.nuevo_valor_nominal <= 0)
          THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0') END;

        SELECT CASE
          WHEN NEW.tipo='REDENOMINACION'
           AND NEW.nuevo_valor_nominal IS NOT NULL
           AND NEW.nuevo_valor_nominal <= 0
          THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0') END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_check_nominal_upd
    BEFORE UPDATE ON events
    BEGIN
        SELECT CASE
          WHEN NEW.tipo IN ('AMPL_VALOR','RED_VALOR')
           AND (NEW.nuevo_valor_nominal IS NULL OR NEW.nuevo_valor_nominal <= 0)
          THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0') END;

        SELECT CASE
          WHEN NEW.tipo='REDENOMINACION'
           AND NEW.nuevo_valor_nominal IS NOT NULL
           AND NEW.nuevo_valor_nominal <= 0
          THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0') END;
    END;
    """
    with get_connection() as conn:
        conn.executescript(sql)