# services.py

import sqlite3
from datetime import date, datetime
import pandas as pd
import hashlib
import json
from typing import Any, Optional, Tuple
from pathlib import Path
from config import BACKUP_DIR as CFG_BACKUP_DIR

def adapt_date_iso(val: date) -> str:
    return val.isoformat()

def adapt_datetime_iso(val: datetime) -> str:
    return val.isoformat(" ")

sqlite3.register_adapter(date, adapt_date_iso)
sqlite3.register_adapter(datetime, adapt_datetime_iso)

# --- Backups y restauración segura (SQLite online backup API) ---
BACKUP_DIR = CFG_BACKUP_DIR

def ensure_backup_dir() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return BACKUP_DIR

# Compatibilidad hacia atrás (por si algo llama aún a la privada)
_ensure_backup_dir = ensure_backup_dir

def backup_database(include_hash: bool = True) -> dict:
    """
    Crea una copia consistente del DB usando la API `Connection.backup`.
    Devuelve dict con info: {ok, path, sha256?, size_bytes}.
    """
    from db import DB_PATH  # ruta actual del .db
    ensure_backup_dir()

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = BACKUP_DIR / f"libro_socios_{ts}.db"

    # Hacemos backup "online" (consistente) sin copiar archivos -wal/-shm
    with sqlite3.connect(str(DB_PATH)) as src, sqlite3.connect(str(out_path)) as dst:
        # si quieres feedback de progreso, puedes pasar un callback
        src.backup(dst)

        # Recomendable dejar mismas pragmas para el nuevo archivo
        try:
            dst.execute("PRAGMA journal_mode = WAL;")
            dst.execute("PRAGMA synchronous = NORMAL;")
            dst.execute("PRAGMA foreign_keys = ON;")
            dst.commit()
        except Exception:
            pass

    size = out_path.stat().st_size
    info = {"ok": True, "path": str(out_path), "size_bytes": size}

    if include_hash:
        try:
            info["sha256"] = _sha256_file(str(out_path))
        except Exception:
            info["sha256"] = None

    return info

def list_backups(limit: int | None = None) -> list[dict]:
    """
    Lista backups existentes en backups/*.db con tamaño y mtime.
    Ordena por fecha de modificación desc.
    """
    ensure_backup_dir()
    files = sorted(BACKUP_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    if limit is not None:
        files = files[:limit]
    out = []
    for p in files:
        out.append({
            "path": str(p),
            "name": p.name,
            "size_bytes": p.stat().st_size,
            "mtime": datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds"),
        })
    return out

def restore_database_from_path(backup_path: str, create_pre_restore_backup: bool = True) -> dict:
    """
    Restaura el contenido de `backup_path` sobre el DB actual usando `Connection.backup`.
    Por seguridad, puede hacer un backup previo automático.
    """
    ensure_backup_dir()
    from db import DB_PATH
    src_path = Path(backup_path)
    if not src_path.exists() or not src_path.is_file():
        return {"ok": False, "error": f"No existe el fichero: {backup_path}"}

    # Evita src == dst
    try:
        if DB_PATH.resolve() == src_path.resolve():
            return {"ok": False, "error": "No puedes restaurar desde el mismo fichero de base de datos activo."}
    except Exception:
        pass

    # Backup previo automático (por si hay que deshacer)
    pre = None
    if create_pre_restore_backup:
        try:
            pre = backup_database(include_hash=False)
        except Exception as e:
            # No abortamos, pero lo reportamos
            pre = {"ok": False, "error": f"No se pudo crear backup previo: {e}"}

    # Restauración con API de backup (no copiamos fichero; volcamos páginas)
    with sqlite3.connect(str(src_path)) as src, sqlite3.connect(str(DB_PATH)) as dst:
        # OJO: esto sobreescribe TODO el contenido de la base de datos destino
        src.backup(dst)

        # Reaplicar pragmas recomendados
        try:
            dst.execute("PRAGMA journal_mode = WAL;")
            dst.execute("PRAGMA synchronous = NORMAL;")
            dst.execute("PRAGMA foreign_keys = ON;")
            dst.commit()
        except Exception:
            pass

    return {"ok": True, "restored_from": str(src_path), "pre_backup": pre}

# ---------------- Autochequeo ----------------
def run_autochequeo() -> dict:
    """
    Ejecuta chequeos básicos de consistencia:
      - Versión actual de esquema (schema_meta).
      - Foreign keys huérfanas (PRAGMA foreign_key_check).
      - Conteos básicos de tablas clave.
    Devuelve un dict con resultados.
    """
    from db import get_connection, get_schema_version
    results = {}
    with get_connection() as conn:
        # Versión actual de esquema
        try:
            results["schema_version"] = get_schema_version(conn)
        except Exception as e:
            results["schema_version"] = f"Error: {e}"

        # FK check
        try:
            cur = conn.execute("PRAGMA foreign_key_check;")
            fk = cur.fetchall()
            cols = ["table", "rowid", "parent", "fkid"]
            results["fk_errors"] = [dict(zip(cols, r)) for r in fk] if fk else []
        except Exception as e:
            results["fk_errors"] = f"Error: {e}"

        # Conteos básicos
        counts = {}
        for table in ["companies", "partners", "events", "holdings"]:
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                counts[table] = n
            except Exception:
                counts[table] = None
        results["counts"] = counts

    return results

# ---------------- Helpers ----------------
def _fetchall_dict(cur) -> list:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _fetchone_dict(cur) -> Optional[dict]:
    row = cur.fetchone()
    if row is None:
        return None
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, row))

def _get_table_columns(conn, table_name:str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}

def _company_exists(conn, company_id:int) -> bool:
    row = conn.execute("SELECT 1 FROM companies WHERE id=?", (company_id,)).fetchone()
    return bool(row)

def _sha256_file(path:str) -> str:
    h = hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _df_para_excel(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Convierte ±inf->NaN y luego NaN/NaT->None para que XlsxWriter no reviente."""
    if df is None:
        return None
    import numpy as np
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    # None => celda en blanco en Excel con XlsxWriter
    return df.where(pd.notnull(df), None)

# --- Vistas, triggers e índices ---
def ensure_db_consistency_primitives(conn):
    """
    Crea vistas, triggers de validación ligera e índices para mejorar
    consistencia y rendimiento. Es idempotente.
    """
    conn.executescript("""
    -- =========================
    -- VISTAS
    -- =========================

    -- Foto actual de participaciones vigentes (plena propiedad).
    CREATE VIEW IF NOT EXISTS view_participaciones_actuales AS
    SELECT
        h.company_id,
        h.socio_id,
        p.nombre AS socio_nombre,
        SUM(h.participaciones) AS participaciones
    FROM holdings h
    JOIN partners p
      ON p.id = h.socio_id AND p.company_id = h.company_id
    WHERE h.estado = 'vigente'
      AND h.right_type = 'plena'
    GROUP BY h.company_id, h.socio_id, p.nombre;

    -- Cuotas (%) por compañía con denominador = suma vigente de 'plena'
    -- (evita depender de companies.participaciones_totales si hubiera
    -- diferencias mientras editas eventos).
    CREATE VIEW IF NOT EXISTS view_cuotas_actuales AS
    WITH totales AS (
        SELECT company_id, SUM(participaciones) AS total_part
        FROM view_participaciones_actuales
        GROUP BY company_id
    )
    SELECT
        v.company_id,
        v.socio_id,
        v.socio_nombre,
        v.participaciones,
        CASE
          WHEN t.total_part > 0 THEN (1.0 * v.participaciones / t.total_part)
          ELSE 0.0
        END AS cuota
    FROM view_participaciones_actuales v
    JOIN totales t ON t.company_id = v.company_id;

    -- =========================
    -- TRIGGERS (validación ligera en EVENTS)
    -- =========================

    -- Regla común de rangos para tipos que los requieren.
    CREATE TRIGGER IF NOT EXISTS trg_events_check_range_ins
    BEFORE INSERT ON events
    WHEN NEW.tipo IN ('ALTA','AMPL_EMISION','TRANSMISION','BAJA','RED_AMORT','PIGNORACION','EMBARGO','USUFRUCTO')
    BEGIN
        SELECT
          CASE
            WHEN NEW.rango_desde IS NULL OR NEW.rango_hasta IS NULL
                 OR NEW.rango_desde < 1 OR NEW.rango_hasta < 1
                 OR NEW.rango_desde > NEW.rango_hasta
            THEN RAISE(ABORT, 'Rango Desde/Hasta inválido (>=1 y Desde <= Hasta)')
          END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_check_range_upd
    BEFORE UPDATE OF tipo, rango_desde, rango_hasta ON events
    WHEN NEW.tipo IN ('ALTA','AMPL_EMISION','TRANSMISION','BAJA','RED_AMORT','PIGNORACION','EMBARGO','USUFRUCTO')
    BEGIN
        SELECT
          CASE
            WHEN NEW.rango_desde IS NULL OR NEW.rango_hasta IS NULL
                 OR NEW.rango_desde < 1 OR NEW.rango_hasta < 1
                 OR NEW.rango_desde > NEW.rango_hasta
            THEN RAISE(ABORT, 'Rango Desde/Hasta inválido (>=1 y Desde <= Hasta)')
          END;
    END;

    -- Nominal obligatorio y > 0 para AMPL_VALOR / RED_VALOR / REDENOMINACION (si lo informas).
    CREATE TRIGGER IF NOT EXISTS trg_events_check_nominal_ins
    BEFORE INSERT ON events
    WHEN NEW.tipo IN ('AMPL_VALOR','RED_VALOR','REDENOMINACION')
    BEGIN
        SELECT
          CASE
            WHEN NEW.nuevo_valor_nominal IS NULL OR NEW.nuevo_valor_nominal <= 0
            THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0')
          END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_check_nominal_upd
    BEFORE UPDATE OF tipo, nuevo_valor_nominal ON events
    WHEN NEW.tipo IN ('AMPL_VALOR','RED_VALOR','REDENOMINACION')
    BEGIN
        SELECT
          CASE
            WHEN NEW.nuevo_valor_nominal IS NULL OR NEW.nuevo_valor_nominal <= 0
            THEN RAISE(ABORT, 'Nuevo valor nominal debe ser > 0')
          END;
    END;

    -- Reglas de presencia de socios según tipo (mínimas; la lógica profunda ya la haces en Python).
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
    BEFORE UPDATE OF tipo, socio_transmite, socio_adquiere ON events
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

    -- REDENOMINACION: coherencia entre modo "global" y "por rangos".
    -- Regla: o bien es GLOBAL (sin rangos y sin socios), o bien es POR RANGOS (con rangos y con al menos un socio).
    CREATE TRIGGER IF NOT EXISTS trg_events_reden_mode_ins
    BEFORE INSERT ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN (NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL)
             OR (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por rangos (con rangos y socio).')
        END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_reden_mode_upd
    BEFORE UPDATE OF tipo, rango_desde, rango_hasta, socio_transmite, socio_adquiere ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN (NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL)
             OR (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por rangos (con rangos y socio).')
        END;
    END;

    -- =========================
    -- ÍNDICES
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_events_company_date ON events(company_id, fecha, id);
    CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);
    CREATE INDEX IF NOT EXISTS idx_holdings_company_flags ON holdings(company_id, right_type, estado);
    CREATE INDEX IF NOT EXISTS idx_partners_company_name ON partners(company_id, nombre);
    """)
    conn.commit()
    
def patch_triggers_redenominacion(conn):
    """
    REDENOMINACION: nominal NO obligatorio (NULL permitido).
    Si se informa, debe ser > 0.
    Y el trigger de 'modo global vs por rangos' se evalúa primero.
    """
    conn.executescript("""
    DROP TRIGGER IF EXISTS trg_events_check_nominal_ins;
    DROP TRIGGER IF EXISTS trg_events_check_nominal_upd;
    DROP TRIGGER IF EXISTS trg_events_reden_mode_ins;
    DROP TRIGGER IF EXISTS trg_events_reden_mode_upd;
    DROP TRIGGER IF EXISTS trg_events_000_reden_mode_ins;
    DROP TRIGGER IF EXISTS trg_events_000_reden_mode_upd;
    DROP TRIGGER IF EXISTS trg_events_100_check_nominal_ins;
    DROP TRIGGER IF EXISTS trg_events_100_check_nominal_upd;

    -- 1) Primero: coherencia REDENOMINACION (global vs por rangos)
    CREATE TRIGGER IF NOT EXISTS trg_events_000_reden_mode_ins
    BEFORE INSERT ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN (NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL)
            OR (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por rangos (con rangos y socio).')
        END;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_events_000_reden_mode_upd
    BEFORE UPDATE OF tipo, rango_desde, rango_hasta, socio_transmite, socio_adquiere ON events
    WHEN NEW.tipo='REDENOMINACION'
    BEGIN
        SELECT CASE
          WHEN (NEW.rango_desde IS NULL AND NEW.rango_hasta IS NULL AND NEW.socio_transmite IS NULL AND NEW.socio_adquiere IS NULL)
            OR (NEW.rango_desde IS NOT NULL AND NEW.rango_hasta IS NOT NULL AND (NEW.socio_transmite IS NOT NULL OR NEW.socio_adquiere IS NOT NULL))
          THEN NULL
          ELSE RAISE(ABORT, 'REDENOMINACION: usa modo global (sin rangos y sin socios) o modo por rangos (con rangos y socio).')
        END;
    END;

    -- 2) Después: nominal obligatorio SÓLO para AMPL_VALOR/RED_VALOR.
    --    En REDENOMINACION es opcional: si viene informado, debe ser > 0.
    CREATE TRIGGER IF NOT EXISTS trg_events_100_check_nominal_ins
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

    CREATE TRIGGER IF NOT EXISTS trg_events_100_check_nominal_upd
    BEFORE UPDATE OF tipo, nuevo_valor_nominal ON events
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
    """)
    conn.commit()


# --- Accessors explícitos ---
def get_company(conn, company_id:int) -> Optional[dict]:
    cur = conn.execute("SELECT * FROM companies WHERE id=?", (company_id,))
    return _fetchone_dict(cur)

def update_company(conn, company_id:int, **fields: Any) -> int:
    """
    UPDATE companies SET k1=?, k2=? ... WHERE id=?
    Devuelve número de filas afectadas.
    """
    if not fields:
        return 0
    keys = list(fields.keys())
    sets = ", ".join([f"{k}=?" for k in keys])
    vals = [fields[k] for k in keys] + [company_id]
    cur = conn.execute(f"UPDATE companies SET {sets} WHERE id=?", vals)
    conn.commit()
    return cur.rowcount

# --- Utilidades JSON (opcional pero recomendado) ---
def normalize_json_text(text:str) -> str:
    """
    Valida que text es JSON válido y devuelve una versión canónica (compacta, UTF-8).
    Lanza ValueError si no es JSON válido.
    """
    try:
        obj = json.loads(text)
    except Exception as e:
        raise ValueError(f"JSON inválido: {e}") from e
    # compacta y mantiene acentos
    return json.dumps(obj, ensure_ascii=False, separators=(",",":"))

# --- compactar base de datos ---
def compact_database() -> Tuple[bool, str]:
    try:
        # Import local para evitar problemas de orden/circularidad en Streamlit
        from db import compact_db  # importa el símbolo exacto cuando se necesite
        info = compact_db()
        return True, f"DB compactada en {info['elapsed_s']:.2f}s"
    except Exception as e:
        return False, f"Error compactando DB: {e}"

# ---------------- Gobernanza ----------------
def set_governance(conn, company_id:int, organo:str, firmantes:list[dict]):
    conn.execute("UPDATE companies SET organo=?, firmantes_json=? WHERE id=?",
                 (organo, json.dumps(firmantes, ensure_ascii=False), company_id))

def get_governance(conn, company_id:int):
    row = conn.execute("SELECT organo, firmantes_json FROM companies WHERE id=?",
                       (company_id,)).fetchone()
    organo = (row["organo"] if row and row["organo"] else "admin_unico")
    try:
        firmantes = json.loads(row["firmantes_json"]) if row and row["firmantes_json"] else []
    except Exception:
        firmantes = []
    # normalizamos claves
    norm = []
    for f in firmantes or []:
        norm.append({
            "nombre": f.get("nombre") or f.get("name") or "",
            "rol": (f.get("rol") or f.get("role") or "").lower()
        })
    return {"organo": organo, "firmantes": norm}

# --------- Bloques (inventario por rangos) -----------
def _split_block(block: dict, d:int, h:int) -> list:
    res = []
    a, b = block['rango_desde'], block['rango_hasta']
    if h < a or d > b:
        return [block]
    if d > a:
        res.append({**block, 'rango_desde': a, 'rango_hasta': d-1,
                    'participaciones': d-1 - a + 1})
    if h < b:
        res.append({**block, 'rango_desde': h+1, 'rango_hasta': b,
                    'participaciones': b - (h+1) + 1})
    return res

def _consolidate(blocks: list) -> list:
    clean = [b for b in blocks if b.get('rango_desde') is not None and b.get('rango_hasta') is not None]
    if not clean:
        return []
    clean = sorted(clean, key=lambda x: (x['socio_id'], x['right_type'], x['rango_desde'], x['rango_hasta']))
    merged = [clean[0].copy()]
    for b in clean[1:]:
        last = merged[-1]
        if (b['socio_id']==last['socio_id'] and
            b['right_type']==last['right_type'] and
            b['rango_desde']==last['rango_hasta']+1):
            last['rango_hasta'] = b['rango_hasta']
            last['participaciones'] = last['rango_hasta'] - last['rango_desde'] + 1
        else:
            nb = b.copy()
            nb['participaciones'] = nb['rango_hasta'] - nb['rango_desde'] + 1
            merged.append(nb)
    return merged

def replace_holdings(conn, company_id:int, new_blocks:list, fecha:str):
    conn.execute("DELETE FROM holdings WHERE company_id=?", (company_id,))
    for b in new_blocks:
        conn.execute(
            "INSERT INTO holdings (company_id, socio_id, right_type, rango_desde, rango_hasta, participaciones, estado, fecha_inicio) "
            "VALUES (?,?,?,?,?,?, 'vigente', ?)",
            (company_id, b['socio_id'], b['right_type'], b['rango_desde'], b['rango_hasta'],
             b['rango_hasta']-b['rango_desde']+1, fecha)
        )
    conn.commit()

# --------- VALIDACIONES ---------
def owner_blocks(conn, company_id:int, socio_id:int, right_type:str='plena'):
    cur = conn.execute("""
        SELECT rango_desde, rango_hasta
        FROM holdings
        WHERE company_id=? AND socio_id=? AND right_type=? AND estado='vigente'
        ORDER BY rango_desde
    """, (company_id, socio_id, right_type))
    return _fetchall_dict(cur)

def _covers(blocks, d, h) -> bool:
    need = [(d, h)]
    for b in blocks:
        a, z = b['rango_desde'], b['rango_hasta']
        new_need = []
        for (x, y) in need:
            if z < x or a > y:
                new_need.append((x, y)); continue
            if a > x: new_need.append((x, a-1))
            if z < y: new_need.append((z+1, y))
        need = [(x, y) for (x, y) in new_need if x <= y]
        if not need:
            return True
    return len(need) == 0

def validate_event(conn, ev:dict) -> list:
    import pandas as _pd
    errors = []
    def nint(x):
        try:
            if x is None or (isinstance(x, float) and _pd.isna(x)): return None
            return int(x)
        except: return None

    d = nint(ev.get("rango_desde"))
    h = nint(ev.get("rango_hasta"))
    t = ev.get("tipo")
    cid = ev.get("company_id")
    stid = ev.get("socio_transmite")
    said = ev.get("socio_adquiere")

    needs_range = t in {"ALTA","AMPL_EMISION","TRANSMISION","BAJA","RED_AMORT","PIGNORACION","EMBARGO","USUFRUCTO"}
    needs_adq   = t in {"ALTA","AMPL_EMISION","TRANSMISION","PIGNORACION","EMBARGO","USUFRUCTO"}
    needs_trans = t in {"TRANSMISION","BAJA","RED_AMORT","USUFRUCTO"}

    if needs_range and (d is None or h is None or d < 1 or h < 1 or d > h):
        errors.append("Rango 'Desde/Hasta' inválido (entero ≥1 y Desde ≤ Hasta).")
    if needs_adq and said is None:
        errors.append("Falta el socio adquirente/acreedor.")
    if needs_trans and stid is None:
        errors.append("Falta el socio transmitente/titular.")
    if t in {"AMPL_VALOR","RED_VALOR"}:
        nv = ev.get("nuevo_valor_nominal")
        if nv is None or nv <= 0:
            errors.append("Indica 'Nuevo valor nominal' > 0.")

    if errors: return errors

    if t in {"ALTA","AMPL_EMISION"}:
        cur = conn.execute("""
            SELECT 1 FROM holdings
            WHERE company_id=? AND right_type='plena' AND estado='vigente'
              AND NOT (rango_hasta < ? OR rango_desde > ?)
            LIMIT 1
        """, (cid, d, h))
        if cur.fetchone():
            errors.append("El rango indicado ya está asignado en plena propiedad a otro socio.")
    elif t == "TRANSMISION":
        blocks = owner_blocks(conn, cid, stid, 'plena')
        if not _covers(blocks, d, h):
            errors.append("El socio transmitente no posee en plena propiedad todo el rango indicado.")
    elif t in {"BAJA","RED_AMORT"}:
        blocks = owner_blocks(conn, cid, stid, 'plena')
        if not _covers(blocks, d, h):
            errors.append("No se pueden amortizar participaciones que el socio no tiene en plena propiedad.")
    elif t in {"PIGNORACION","EMBARGO"}:
        cur = conn.execute("""
            SELECT 1 FROM holdings
            WHERE company_id=? AND right_type='plena' AND estado='vigente'
              AND rango_desde <= ? AND rango_hasta >= ?
            LIMIT 1
        """, (cid, d, h))
        if not cur.fetchone():
            errors.append("No existe plena propiedad sobre todo el rango indicado; no puede gravarse.")
    elif t == "USUFRUCTO":
        blocks = owner_blocks(conn, cid, stid, 'plena')
        if not _covers(blocks, d, h):
            errors.append("Para desdoblar en nuda/uso, el transmitente debe tener plena propiedad del rango.")

    return errors

# --------- Motor genérico (aplica lista de eventos) ----------
def _apply_events(events:list, valor_nominal_inicial:float=5.0, part_tot_inicial:int=0):
    """
    Aplica los eventos agrupando por fecha y ordenando dentro del día para evitar
    que se intercalen REDENOMINACION con RED_AMORT/TRANSMISION.
    Orden por día:
      1) BAJA/RED_AMORT
      2) TRANSMISION
      3) ALTA/AMPL_EMISION
      4) USUFRUCTO/PIGNORACION/EMBARGO y AMPL_VALOR/RED_VALOR
      5) REDENOMINACION (al cierre del día)
    """
    from collections import defaultdict
    from datetime import date
    from decimal import Decimal, getcontext, ROUND_FLOOR

    # Precisión alta para cálculos de % en redenominación
    getcontext().prec = 28
    D = Decimal

    blocks = []
    valor_nominal = valor_nominal_inicial
    total_part = part_tot_inicial
    last_fecha = str(date.today())

    # --- agrupar por fecha ---
    def _to_datestr(f):
        return f if isinstance(f, str) else f.strftime("%Y-%m-%d")

    by_date = defaultdict(list)
    for ev in events:
        by_date[_to_datestr(ev['fecha'])].append(ev)

    for f in sorted(by_date.keys()):
        day = by_date[f]
        last_fecha = f

        # 1) BAJA / RED_AMORT (quitan)
        for ev in sorted([e for e in day if e['tipo'] in ('BAJA','RED_AMORT')],
                         key=lambda e: ((e.get('rango_desde') or 0), (e.get('rango_hasta') or 0))):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            new_blocks = []
            for b in blocks:
                if b['right_type']=='plena' and b['socio_id']==ev['socio_transmite']:
                    new_blocks.extend(_split_block(b, d, h))
                else:
                    new_blocks.append(b)
            blocks = _consolidate(new_blocks)

        # 2) TRANSMISION (mueven)
        for ev in sorted([e for e in day if e['tipo']=='TRANSMISION'],
                         key=lambda e: ((e.get('rango_desde') or 0), (e.get('rango_hasta') or 0))):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            # quitar al transmite
            new_blocks = []
            for b in blocks:
                if b['right_type']=='plena' and b['socio_id']==ev['socio_transmite']:
                    new_blocks.extend(_split_block(b, d, h))
                else:
                    new_blocks.append(b)
            blocks = _consolidate(new_blocks)
            # poner al adquiere
            blocks.append(dict(socio_id=ev['socio_adquiere'], right_type='plena',
                               rango_desde=d, rango_hasta=h))
            blocks = _consolidate(blocks)

        # 3) ALTAS y AMPL_EMISION (añaden)
        for ev in sorted([e for e in day if e['tipo'] in ('ALTA','AMPL_EMISION')],
                         key=lambda e: ((e.get('rango_desde') or 0), (e.get('rango_hasta') or 0))):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            blocks.append(dict(socio_id=ev['socio_adquiere'], right_type='plena',
                               rango_desde=d, rango_hasta=h))
            if h:
                total_part = max(total_part, h)
            blocks = _consolidate(blocks)

        # 4) USUFRUCTO/PIGNORACION/EMBARGO + cambio de valor nominal puntual
        for ev in [e for e in day if e['tipo'] in ('USUFRUCTO','PIGNORACION','EMBARGO')]:
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            if ev['tipo'] == 'USUFRUCTO':
                new_blocks = []
                for b in blocks:
                    if b['right_type']=='plena' and b['socio_id']==ev['socio_transmite']:
                        new_blocks.extend(_split_block(b, d, h))
                    else:
                        new_blocks.append(b)
                new_blocks.append(dict(socio_id=ev['socio_transmite'], right_type='nuda',
                                       rango_desde=d, rango_hasta=h))
                new_blocks.append(dict(socio_id=ev['socio_adquiere'], right_type='usufructo',
                                       rango_desde=d, rango_hasta=h))
                blocks = _consolidate(new_blocks)
            else:
                holder = ev['socio_adquiere'] or ev['socio_transmite']
                blocks.append(dict(socio_id=holder,
                                   right_type=('prenda' if ev['tipo']=='PIGNORACION' else 'embargo'),
                                   rango_desde=d, rango_hasta=h))
                blocks = _consolidate(blocks)

        for ev in [e for e in day if e['tipo'] in ('AMPL_VALOR','RED_VALOR')]:
            valor_nominal = ev['nuevo_valor_nominal']

        # 5) REDENOMINACION (al cierre del día) — NO altera capital ni %; compacta bloques
        if any(e['tipo'] == 'REDENOMINACION' for e in day):
            # --- Totales vigentes por socio (solo 'plena') ---
            current = {}
            for b in blocks:
                if b['right_type'] != 'plena':
                    continue
                n = b['rango_hasta'] - b['rango_desde'] + 1
                current[b['socio_id']] = current.get(b['socio_id'], 0) + n

            old_total = sum(current.values())
            old_vn = D(str(valor_nominal))
            old_capital = old_vn * D(old_total)

            # VN nuevo (opcional). Si hay varios en el día, deben ser iguales.
            vn_candidates = [
                e.get('nuevo_valor_nominal') for e in day
                if e['tipo'] == 'REDENOMINACION' and e.get('nuevo_valor_nominal') not in (None, "")
            ]
            new_vn = None
            if vn_candidates:
                vals = [float(v) for v in vn_candidates]
                if len({round(v, 6) for v in vals}) > 1:
                    raise ValueError(f"Valores nominales distintos en REDENOMINACION del día {f}: {vals}")
                new_vn = D(str(vals[-1]))
                if new_vn <= 0:
                    raise ValueError(f"Nuevo valor nominal inválido en REDENOMINACION del día {f}: {new_vn}")

            # Determinar total de participaciones tras redenominación
            if new_vn is None:
                new_total = old_total  # solo compacta/renumera
            else:
                # Capital invariable: capital debe ser múltiplo del nuevo VN
                ratio = (old_capital / new_vn)
                if ratio != ratio.to_integral_value():
                    raise ValueError(
                        f"El capital {old_capital} no es múltiplo del nuevo VN {new_vn} en REDENOMINACION del día {f}."
                    )
                new_total = int(ratio)
                valor_nominal = float(new_vn)

            # Reparto proporcional (método de restos mayores) para mantener % exactos
            if old_total == 0:
                blocks = _consolidate(blocks)
                total_part = 0
            else:
                socios = sorted(current.keys())  # orden estable por socio_id
                exact = {sid: (D(current[sid]) * D(str(new_total)) / D(old_total)) for sid in socios}
                base  = {sid: int(exact[sid].to_integral_value(rounding=ROUND_FLOOR)) for sid in socios}
                asignadas = sum(base.values())
                resto = new_total - asignadas

                if resto < 0:
                    # defensa: (no debería ocurrir), quitamos 1 a los últimos hasta cuadrar
                    for sid in reversed(socios):
                        if resto == 0: break
                        if base[sid] > 0:
                            base[sid] -= 1
                            resto += 1
                elif resto > 0:
                    # repartir restos por parte fraccionaria desc.; desempate por socio_id asc
                    fracs = sorted(
                        [(sid, exact[sid] - D(base[sid])) for sid in socios],
                        key=lambda x: (x[1], -x[0]),
                        reverse=True
                    )
                    for i in range(resto):
                        base[fracs[i][0]] += 1

                # Construir bloques compactos, un bloque por socio
                cursor = 1
                new_blocks = []
                for sid in socios:
                    n = base[sid]
                    if n <= 0:
                        continue
                    new_blocks.append(dict(
                        socio_id=sid,
                        right_type='plena',
                        rango_desde=cursor,
                        rango_hasta=cursor + n - 1
                    ))
                    cursor += n

                blocks = _consolidate(new_blocks)
                total_part = new_total

        # -- Ajuste final del día: capital en circulación por suma de bloques 'plena' --
        total_part = sum(
            b['rango_hasta'] - b['rango_desde'] + 1
            for b in blocks if b['right_type'] == 'plena'
        )

    return blocks, valor_nominal, total_part, last_fecha

# --------- Recalcular y Foto Fija ----------
def recompute_company(conn: sqlite3.Connection, company_id:int):
    """
    Recalcula las participaciones de una compañía aplicando todos los eventos
    en orden cronológico robusto:
      - Por fecha
      - Por hora (si existe)
      - Por tipo de evento (BAJA/RED_AMORT -> TRANSMISION -> ALTA/AMPL_EMISION
        -> USUFRUCTO/PIGNORACION/EMBARGO/AMPL_VALOR/RED_VALOR -> REDENOMINACION)
      - Por orden_del_dia (si existe)
      - Por id (último desempate)
    """
    cur = conn.execute("""
        SELECT *
        FROM events
        WHERE company_id=?
        ORDER BY
          fecha,
          COALESCE(hora, ''),              -- primero por hora si existe
          CASE tipo
            WHEN 'BAJA'           THEN 0
            WHEN 'RED_AMORT'      THEN 0
            WHEN 'TRANSMISION'    THEN 1
            WHEN 'ALTA'           THEN 2
            WHEN 'AMPL_EMISION'   THEN 2
            WHEN 'USUFRUCTO'      THEN 3
            WHEN 'PIGNORACION'    THEN 3
            WHEN 'EMBARGO'        THEN 3
            WHEN 'AMPL_VALOR'     THEN 4
            WHEN 'RED_VALOR'      THEN 4
            WHEN 'REDENOMINACION' THEN 5
            ELSE 6
          END,
          COALESCE(orden_del_dia, 0),
          id
    """, (company_id,))
    events = _fetchall_dict(cur)

    c = conn.execute(
        "SELECT valor_nominal, participaciones_totales FROM companies WHERE id=?",
        (company_id,)
    ).fetchone()

    blocks, vn, tot, last_fecha = _apply_events(
        events,
        c[0] if c else 5.0,
        c[1] if c else 0
    )

    # Comprobación de consistencia
    total_bloques = sum(
        b['rango_hasta'] - b['rango_desde'] + 1
        for b in blocks if b['right_type'] == 'plena'
    )
    assert total_bloques == tot, f"Descuadre: bloques={total_bloques} != total_part={tot}"

    replace_holdings(conn, company_id, blocks, fecha=last_fecha)
    conn.execute(
        "UPDATE companies SET valor_nominal=?, participaciones_totales=? WHERE id=?",
        (vn, tot, company_id)
    )
    conn.commit()
    
def snapshot_socios(conn, company_id:int):
    cur = conn.execute("""
        SELECT p.id as socio_id, p.nombre, COALESCE(SUM(h.participaciones),0) as participaciones
        FROM partners p
        LEFT JOIN holdings h 
          ON h.socio_id = p.id 
         AND h.company_id = p.company_id 
         AND h.right_type='plena' 
         AND h.estado='vigente'
        WHERE p.company_id=?
        GROUP BY p.id, p.nombre
        ORDER BY p.nombre
    """, (company_id,))
    return [dict(row) for row in cur.fetchall()]

def snapshot_socios_vigentes(conn, company_id:int):
    """
    Solo socios con participaciones vigentes (plena propiedad) > 0.
    """
    cur = conn.execute("""
        SELECT p.id AS socio_id, p.nombre, SUM(h.participaciones) AS participaciones
          FROM holdings h
          JOIN partners p
            ON p.id = h.socio_id AND p.company_id = h.company_id
         WHERE h.company_id = ?
           AND h.right_type = 'plena'
           AND h.estado = 'vigente'
         GROUP BY p.id, p.nombre
         HAVING SUM(h.participaciones) > 0
         ORDER BY p.nombre
    """, (company_id,))
    return [dict(row) for row in cur.fetchall()]

def snapshot_as_of(conn: sqlite3.Connection, company_id:int, fecha_corte:str):
    """Devuelve bloques y resumen de socios a fecha de corte (sin tocar la BD)."""
    cur = conn.execute("SELECT * FROM events WHERE company_id=? AND fecha<=? ORDER BY fecha, id", (company_id, fecha_corte))
    events = _fetchall_dict(cur)
    c = conn.execute("SELECT valor_nominal, participaciones_totales FROM companies WHERE id=?", (company_id,)).fetchone()
    blocks, vn, tot, _ = _apply_events(events, c[0] if c else 5.0, c[1] if c else 0)

    data = []
    for b in blocks:
        if b['right_type']!='plena': continue
        data.append((b['socio_id'], b['rango_hasta']-b['rango_desde']+1))
    df = pd.DataFrame(data, columns=['socio_id','n'])
    if df.empty:
        resumen = pd.DataFrame(columns=['socio_id','nombre','participaciones'])
    else:
        agg = df.groupby('socio_id')['n'].sum().reset_index()
        socios = pd.read_sql_query("SELECT id as socio_id, nombre FROM partners WHERE company_id=?", conn, params=(company_id,))
        resumen = agg.merge(socios, on='socio_id', how='left')[['socio_id','nombre','n']].rename(columns={'n':'participaciones'}).sort_values('nombre')

    if blocks:
        bl = pd.DataFrame(blocks)
        socios = pd.read_sql_query("SELECT id as socio_id, nombre FROM partners WHERE company_id=?", conn, params=(company_id,))
        bl = bl.merge(socios, on='socio_id', how='left')
        bl['participaciones'] = bl['rango_hasta'] - bl['rango_desde'] + 1
        bl = bl[['socio_id','nombre','right_type','rango_desde','rango_hasta','participaciones']].sort_values(['nombre','right_type','rango_desde'])
    else:
        bl = pd.DataFrame(columns=['socio_id','nombre','right_type','rango_desde','rango_hasta','participaciones'])

    return bl, resumen, vn, tot

# --------- Exportación a Excel (con nombres en movimientos) ----------
def export_excel(conn: sqlite3.Connection, company_id:int, fecha_corte:str, path:str):
    import xlsxwriter
    from datetime import datetime

    # --- Datos de compañía
    comp = conn.execute("SELECT * FROM companies WHERE id=?", (company_id,)).fetchone()
    if not comp:
        raise ValueError("Sociedad no encontrada.")
    nombre = comp['name']; cif = comp['cif']; dom = comp['domicilio']; fconst = comp['fecha_constitucion']

    # --- Foto fija
    bloques, resumen, valor_nominal, part_tot = snapshot_as_of(conn, company_id, fecha_corte)

    # --- Movimientos con nombres
    eventos = pd.read_sql_query("""
        SELECT e.fecha AS Fecha,
               e.tipo  AS Tipo,
               pt.nombre AS 'Socio transmite',
               pa.nombre AS 'Socio adquiere',
               e.rango_desde AS Desde,
               e.rango_hasta AS Hasta,
               e.participaciones AS Participaciones,
               e.nuevo_valor_nominal AS 'Nuevo valor nominal',
               e.documento AS Documento,
               e.observaciones AS Observaciones
        FROM events e
        LEFT JOIN partners pt ON pt.id = e.socio_transmite
        LEFT JOIN partners pa ON pa.id = e.socio_adquiere
        WHERE e.company_id=? AND e.fecha<=?
        ORDER BY e.fecha, e.id
    """, conn, params=(company_id, fecha_corte))

    # --- Cargas desde bloques
    cargas = bloques[bloques['right_type'].isin(['prenda','embargo','usufructo','nuda'])].copy() if isinstance(bloques, pd.DataFrame) and not bloques.empty else pd.DataFrame()

    # --- Tabla "Socios actuales" como en el PDF
    with conn:
        df_snap = pd.read_sql_query("""
            SELECT p.id AS socio_id, p.nombre, SUM(h.participaciones) AS participaciones
            FROM holdings h
            JOIN partners p
                ON p.id = h.socio_id AND p.company_id = h.company_id
            WHERE h.company_id = ?
              AND h.right_type = 'plena'
              AND h.estado = 'vigente'
            GROUP BY p.id, p.nombre
            HAVING SUM(h.participaciones) > 0
            ORDER BY p.nombre
        """, conn, params=(company_id,))

        df_meta = pd.read_sql_query("""
            SELECT id, nombre, nif, nacionalidad, domicilio
            FROM partners
            WHERE company_id = ?
        """, conn, params=(company_id,))

    df_snap = df_snap[["socio_id", "participaciones"]]
    socios_pdf_like = df_meta.merge(df_snap, left_on="id", right_on="socio_id", how="inner")
    socios_pdf_like["Participaciones"] = socios_pdf_like["participaciones"].fillna(0).astype(int)
    total_part = int(socios_pdf_like["Participaciones"].sum())
    socios_pdf_like["Cuota (%)"] = (socios_pdf_like["Participaciones"] / total_part * 100).round(4) if total_part > 0 else 0.0

    socios_pdf_like = socios_pdf_like.rename(columns={
        "nombre": "Nombre o razón social",
        "nif": "NIF",
        "nacionalidad": "Nacionalidad",
        "domicilio": "Domicilio"
    })[["Nombre o razón social","NIF","Nacionalidad","Domicilio","Participaciones","Cuota (%)"]]

    # Fila TOTAL
    fila_total = {
        "Nombre o razón social": "TOTAL",
        "NIF": "",
        "Nacionalidad": "",
        "Domicilio": "",
        "Participaciones": total_part,
        "Cuota (%)": 100.0 if total_part > 0 else 0.0
    }
    socios_pdf_like = pd.concat([socios_pdf_like, pd.DataFrame([fila_total])], ignore_index=True)

    # --- Saneos (evitar NaN/Inf/NaT)
    resumen = _df_para_excel(resumen) if isinstance(resumen, pd.DataFrame) else resumen
    bloques  = _df_para_excel(bloques) if isinstance(bloques, pd.DataFrame) else bloques
    cargas   = _df_para_excel(cargas)  if isinstance(cargas,  pd.DataFrame) else cargas
    eventos  = _df_para_excel(eventos) if isinstance(eventos, pd.DataFrame) else eventos
    socios_pdf_like = _df_para_excel(socios_pdf_like)

    # Fecha texto (evita NaT)
    if isinstance(eventos, pd.DataFrame) and not eventos.empty and 'Fecha' in eventos.columns:
        eventos['Fecha'] = eventos['Fecha'].astype(str)

    # --- Mapa de tipos cortos (como PDF) y leyenda
    TIPO_MAP = {
        "ALTA": "ALTA",
        "AMPL_EMISION": "AMP-EMIS",
        "AMPL_VALOR": "AMP-VAL",
        "TRANSMISION": "TRANS",
        "BAJA": "BAJA",
        "RED_AMORT": "RED-AM",
        "RED_VALOR": "RED-VAL",
        "PIGNORACION": "PIGN",
        "EMBARGO": "EMB",
        "USUFRUCTO": "USUF",
        "REDENOMINACION": "REDEN",
    }
    legend_text = ("ALTA: Alta; AMP-EMIS: Ampliación por emisión de participaciones; AMP-VAL: Ampliación por valor nominal; "
                   "TRANS: Transmisión; BAJA: Baja; RED-AM: Reducción por amortización; RED-VAL: Reducción por valor nominal; "
                   "PIGN: Pignoración; EMB: Embargo; USUF: Usufructo; REDEN: Redenominación")

    if isinstance(eventos, pd.DataFrame) and not eventos.empty:
        eventos.insert(1, "Tipo (corto)", eventos["Tipo"].map(lambda x: TIPO_MAP.get(str(x), str(x))))

    # --- Workbook (protección ante NaN/Inf)
    wb = xlsxwriter.Workbook(path, {'nan_inf_to_errors': True})

    h1 = wb.add_format({'bold': True, 'font_size': 14})
    h2 = wb.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1})
    cell = wb.add_format({'border': 1})
    ital = wb.add_format({'italic': True})
    num0 = wb.add_format({'border': 1, 'num_format': '#,##0'})
    pct4 = wb.add_format({'border': 1, 'num_format': '0.0000%'})

    # === Portada (como PDF) ===
    ws = wb.add_worksheet("Portada")
    ws.write("A1", "Libro Registro de Socios (SL)", h1)
    ws.write("A3", "Sociedad:", h2); ws.write("B3", nombre)
    ws.write("A4", "CIF:", h2); ws.write("B4", cif)
    ws.write("A5", "Domicilio:", h2); ws.write("B5", dom or "")
    ws.write("A6", "Fecha constitución:", h2); ws.write("B6", str(fconst or ""))
    ws.write("A7", "Valor nominal (€):", h2); ws.write("B7", valor_nominal)
    ws.write("A8", "Participaciones totales:", h2); ws.write("B8", part_tot)
    ws.write("A9", "Fecha de corte:", h2); ws.write("B9", fecha_corte)
    ws.write("A10","Generado el:", h2); ws.write("B10", datetime.now().strftime("%Y-%m-%d %H:%M"))
    ws.set_column("A:A", 28); ws.set_column("B:B", 60)
    ws.write("A12", "Incluye: Socios actuales, inventario, cargas, movimientos, firmas y leyenda.", ital)

    # === Socios actuales (idéntico al PDF) ===
    ws = wb.add_worksheet("Socios actuales")
    headers = ["Nombre o razón social","NIF","Nacionalidad","Domicilio","Participaciones","Cuota (%)"]
    ws.write_row(0, 0, headers, h2)
    if isinstance(socios_pdf_like, pd.DataFrame) and not socios_pdf_like.empty:
        for i, row in socios_pdf_like[headers].reset_index(drop=True).iterrows():
            # Porcentaje en Excel como número (0..1) => para mostrar 4 decimales de % aplicamos formato pct4
            cuota = row["Cuota (%)"]
            cuota_num = (float(cuota)/100.0) if isinstance(cuota, (int,float)) else None
            ws.write(i+1, 0, row["Nombre o razón social"], cell)
            ws.write(i+1, 1, row["NIF"], cell)
            ws.write(i+1, 2, row["Nacionalidad"], cell)
            ws.write(i+1, 3, row["Domicilio"], cell)
            ws.write_number(i+1, 4, int(row["Participaciones"]) if row["Participaciones"] not in (None,"") else 0, num0)
            if cuota_num is None:
                ws.write(i+1, 5, None, cell)
            else:
                ws.write_number(i+1, 5, cuota_num, pct4)
    ws.set_column(0, 0, 36); ws.set_column(1, 1, 18); ws.set_column(2, 2, 14); ws.set_column(3, 3, 40)
    ws.set_column(4, 4, 16); ws.set_column(5, 5, 12)

    # === Inventario actual ===
    ws = wb.add_worksheet("Inventario actual")
    ws.write_row(0, 0, ["ID socio","Nombre","Derecho","Desde","Hasta","Participaciones"], h2)
    if isinstance(bloques, pd.DataFrame) and not bloques.empty:
        for i, row in bloques.reset_index(drop=True).iterrows():
            ws.write_row(i+1, 0, [row['socio_id'], row['nombre'], row['right_type'],
                                  row['rango_desde'], row['rango_hasta'], row['participaciones']], cell)
    ws.set_column("A:A", 10); ws.set_column("B:B", 40); ws.set_column("C:C", 14)
    ws.set_column("D:E", 10); ws.set_column("F:F", 16)

    # === Cargas ===
    ws = wb.add_worksheet("Cargas")
    ws.write_row(0, 0, ["ID socio/titular","Nombre","Derecho","Desde","Hasta","Participaciones"], h2)
    if isinstance(cargas, pd.DataFrame) and not cargas.empty:
        for i, row in cargas.reset_index(drop=True).iterrows():
            ws.write_row(i+1, 0, [row['socio_id'], row['nombre'], row['right_type'],
                                  row['rango_desde'], row['rango_hasta'], row['participaciones']], cell)
    else:
        ws.write_row(1, 0, ["–","–","–","–","–","–"], cell)
    ws.set_column("A:A", 16); ws.set_column("B:B", 40); ws.set_column("C:C", 14)
    ws.set_column("D:E", 10); ws.set_column("F:F", 16)

    # === Movimientos (con Tipo corto como en PDF) ===
    ws = wb.add_worksheet("Movimientos")
    if isinstance(eventos, pd.DataFrame) and not eventos.empty:
        headers = ["Fecha","Tipo (corto)","Tipo","Socio transmite","Socio adquiere","Desde","Hasta",
                   "Participaciones","Nuevo valor nominal","Documento","Observaciones"]
        ws.write_row(0, 0, headers, h2)
        for i, row in eventos[headers].reset_index(drop=True).iterrows():
            ws.write_row(i+1, 0, [
                row["Fecha"], row["Tipo (corto)"], row["Tipo"], row["Socio transmite"], row["Socio adquiere"],
                row["Desde"], row["Hasta"], row["Participiciones"] if "Participiciones" in row else row["Participaciones"],
                row["Nuevo valor nominal"], row["Documento"], row["Observaciones"]
            ], cell)
        ws.set_column("A:A", 12); ws.set_column("B:B", 12); ws.set_column("C:C", 16); ws.set_column("D:E", 22)
        ws.set_column("F:H", 12); ws.set_column("I:I", 20); ws.set_column("J:K", 40)
    else:
        ws.write("A1", "No hay movimientos hasta la fecha de corte.", ital)

    # === Firmas (como PDF) ===
    ws = wb.add_worksheet("Firmas")
    gov = get_governance(conn, company_id)
    organo = gov["organo"]
    firmantes = gov["firmantes"]  # [{"nombre","rol"}, ...]

    def pick_signers(organo:str, firmantes:list[dict]):
        by = {}
        for f in firmantes:
            by.setdefault(f["rol"], []).append(f["nombre"])
        lines = []
        if organo == "admin_unico":
            nombre = (by.get("administrador_unico") or by.get("administrador único") or [""])[0]
            lines.append(("El Administrador Único", nombre))
        elif organo == "admins_solidarios":
            admins = by.get("administrador_solidario") or by.get("administradores_solidarios") or [f["nombre"] for f in firmantes]
            for n in admins:
                lines.append(("Administrador Solidario", n))
        else:
            pres = (by.get("presidente") or [""])[0]
            sec  = (by.get("secretario") or [])
            cd   = (by.get("consejero_delegado") or [])
            cons = (by.get("consejero") or [])
            vp   = (by.get("vicepresidente") or [])
            if pres: lines.append(("Presidente del Consejo", pres))
            if vp:   lines.append(("Vicepresidente del Consejo", vp[0]))
            if sec:  lines.append(("Secretario del Consejo", sec[0]))
            elif cd: lines.append(("Consejero Delegado", cd[0]))
            elif cons: lines.append(("Consejero", cons[0]))
            if len(lines) == 1 and len(cons) > 1:
                lines.append(("Consejero", cons[1]))
            if not lines:
                lines = [("Presidente del Consejo",""), ("Secretario del Consejo","")]
        return lines

    ws.write_row(0, 0, ["Cargo","Nombre","Firma"], h2)
    rowi = 1
    for cargo, nombre_sig in pick_signers(organo, firmantes):
        ws.write_row(rowi, 0, [cargo, nombre_sig, "_______________________________"], cell)
        rowi += 1
    ws.set_column("A:A", 28); ws.set_column("B:B", 42); ws.set_column("C:C", 30)
    ws.write(rowi+1, 0, "Las firmas anteriores se adecúan al órgano de administración configurado para la sociedad.", ital)

    # === Leyenda de tipos (texto) ===
    ws = wb.add_worksheet("Leyenda")
    ws.write("A1", "Leyenda de códigos de 'Tipo' (Movimientos)", h1)
    ws.write("A3", legend_text)
    ws.set_column("A:A", 120)

    wb.close()

# --------- Socios: alta, modificación y consultas ---------

def create_partner(conn, company_id:int, nombre:str, nif:str, domicilio:str, nacionalidad:str="Española"):
    """
    Da de alta un socio.
    """
    conn.execute("""
        INSERT INTO partners (company_id, nombre, nif, domicilio, nacionalidad)
        VALUES (?, ?, ?, ?, ?)
    """, (company_id, nombre, nif, domicilio, nacionalidad))


def update_partner(conn, socio_id:int, nombre:str, nif:str, domicilio:str, nacionalidad:str):
    """
    Actualiza los datos de un socio existente.
    """
    conn.execute("""
        UPDATE partners
           SET nombre = ?, nif = ?, domicilio = ?, nacionalidad = ?
         WHERE id = ?
    """, (nombre, nif, domicilio, nacionalidad, socio_id))


def list_partners(conn, company_id:int):
    """
    Devuelve todos los socios de una compañía con sus datos básicos.
    """
    cur = conn.execute("""
        SELECT id, nombre, nif, domicilio, nacionalidad
          FROM partners
         WHERE company_id = ?
         ORDER BY nombre
    """, (company_id,))
    return [dict(row) for row in cur.fetchall()]


def last_annotations(conn, company_id:int):
    """
    Devuelve dict {socio_id: 'YYYY-MM-DD'} con la última fecha en la que el socio aparece
    como transmitente o adquirente en events para esa compañía.
    """
    cur = conn.execute("""
        SELECT socio_id, MAX(fecha) AS ultima
        FROM (
            SELECT socio_transmite AS socio_id, fecha
              FROM events
             WHERE company_id = ? AND socio_transmite IS NOT NULL
            UNION ALL
            SELECT socio_adquiere AS socio_id, fecha
              FROM events
             WHERE company_id = ? AND socio_adquiere IS NOT NULL
        )
        GROUP BY socio_id
    """, (company_id, company_id))
    return {row["socio_id"]: row["ultima"] for row in cur.fetchall()}

# ---------------------------------------------
# Exportación a PDF (apaisado) con TIPOS cortos
# ---------------------------------------------
def export_pdf(conn: sqlite3.Connection, company_id:int, fecha_corte:str, path:str):
    """
    Genera el PDF legalizable (A4 apaisado) con:
    - Portada info + Socios + Inventario + Cargas + Movimientos + Firmas
    - 'Movimientos':
        * 'Tipo' con códigos cortos
        * Leyenda de códigos SOLO en páginas de Movimientos (pie de página, cursiva, 6 pt, 1 línea)
        * Cabeceras y celdas largas con Paragraph(wordWrap='CJK')
        * colWidths reescalados al ancho útil, números a la derecha, repeatRows=1, splitByRow=1
    - Portada adicional con hash SHA-256
    """
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import (
        BaseDocTemplate, PageTemplate, Frame, NextPageTemplate,
        Paragraph, Spacer, Table, TableStyle, PageBreak
    )
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.pdfgen import canvas as _canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
    

    # --- Mapeo a códigos cortos para 'Tipo' ---
    TIPO_MAP = {
        "ALTA": "ALTA",
        "AMPL_EMISION": "AMP-EMIS",
        "AMPL_VALOR": "AMP-VAL",
        "TRANSMISION": "TRANS",
        "BAJA": "BAJA",
        "RED_AMORT": "RED-AM",
        "RED_VALOR": "RED-VAL",
        "PIGNORACION": "PIGN",
        "EMBARGO": "EMB",
        "USUFRUCTO": "USUF",
        "REDENOMINACION": "REDEN",
    }

    # --- Leyenda en UNA LÍNEA para el pie de página (solo Movimientos) ---
    legend_text = (
        "ALTA: Alta; AMP-EMIS: Ampliación por emisión de participaciones; "
        "AMP-VAL: Ampliación por valor nominal; TRANS: Transmisión; BAJA: Baja; "
        "RED-AM: Reducción por amortización; RED-VAL: Reducción por valor nominal; "
        "PIGN: Pignoración; EMB: Embargo; USUF: Usufructo; REDEN: Redenominación"
    )

    # --- Datos sociedad ---
    comp = conn.execute("SELECT * FROM companies WHERE id=?", (company_id,)).fetchone()
    if not comp:
        raise ValueError("Sociedad no encontrada.")
    nombre = comp['name']; cif = comp['cif']; dom = comp['domicilio']; fconst = comp['fecha_constitucion']

    # --- Foto fija e inputs ---
    bloques, resumen, valor_nominal, part_tot = snapshot_as_of(conn, company_id, fecha_corte)

    # --- Eventos con nombres (hasta fecha corte) ---
    eventos = pd.read_sql_query("""
        SELECT DATE(e.fecha)       AS Fecha,
               e.tipo              AS Tipo,
               pt.nombre           AS "Socio transmite",
               pa.nombre           AS "Socio adquiere",
               e.rango_desde       AS Desde,
               e.rango_hasta       AS Hasta,
               e.participaciones   AS Participaciones,
               e.nuevo_valor_nominal AS "Nuevo valor nominal",
               e.documento         AS Documento,
               e.observaciones     AS Observaciones
        FROM events e
        LEFT JOIN partners pt ON pt.id = e.socio_transmite
        LEFT JOIN partners pa ON pa.id = e.socio_adquiere
        WHERE e.company_id=? AND e.fecha<=?
        ORDER BY e.fecha, e.id
    """, conn, params=(company_id, fecha_corte))

    has_moves = isinstance(eventos, pd.DataFrame) and not eventos.empty

    # --- Estilos ---
    styles = getSampleStyleSheet()
    h1 = styles['Title']; h2 = styles['Heading2']
    style_header = ParagraphStyle('header', parent=styles['Normal'], fontName="DejaVuSans", fontSize=8, leading=9, alignment=1, wordWrap='CJK')
    style_cell   = ParagraphStyle('cell', parent=styles['Normal'], fontName="DejaVuSans", fontSize=8, leading=9, wordWrap='CJK')
    style_small  = ParagraphStyle('small', parent=styles['Normal'], fontName="DejaVuSans", fontSize=9, leading=11)

    def P(x): return Paragraph("" if x is None else str(x), style_cell)

    # --- Documento con plantillas (para pie solo en Movimientos) ---
    doc = BaseDocTemplate(
        path, pagesize=landscape(A4),
        leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='F')
    def footer_mov(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica-Oblique", 8)  # itálica 8 pt
        canvas.setFillGray(0.3)
        # Coloca en el margen inferior respetando bottomMargin
        x = doc_.leftMargin
        y = doc_.bottomMargin - 10  # 10 puntos por debajo del área útil
        canvas.drawString(x, max(8, y), legend_text)  # evita irse por debajo del borde
        canvas.restoreState()
    tpl_default = PageTemplate(id='default', frames=[frame])                     # sin pie
    tpl_moves   = PageTemplate(id='moves',   frames=[frame], onPage=footer_mov)  # con pie
    doc.addPageTemplates([tpl_default, tpl_moves])

    story = []
    usable_w = doc.width

    # ========= Portada info =========
    story.append(Paragraph("<b>Libro Registro de Socios (Sociedad Limitada)</b>", h1))
    story.append(Spacer(1, 8))
    portada = [
        ["Sociedad", nombre],
        ["CIF", cif],
        ["Domicilio", dom or ""],
        ["Fecha constitución", str(fconst or "")],
        ["Valor nominal (€)", f"{valor_nominal}"],
        ["Participaciones totales", f"{part_tot}"],
        ["Fecha de corte", str(fecha_corte)],
        ["Generado el", datetime.now().strftime("%Y-%m-%d %H:%M")],
    ]
    t = Table(portada, colWidths=[0.20*usable_w, 0.80*usable_w])
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(0,-1),colors.whitesmoke),
        ('BOX',(0,0),(-1,-1),0.5,colors.black),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.grey),
        ('FONTSIZE',(0,0),(-1,-1),9),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
    ]))
    story.append(t)
    story.append(PageBreak())

    # ========= Socios actuales (vigentes) =========
    # Traemos socios vigentes (plena, estado vigente) y calculamos % con 4 decimales
    with conn:  # usa la misma conn ya abierta arriba
        df_snap = pd.read_sql_query("""
            SELECT p.id AS socio_id, p.nombre, SUM(h.participaciones) AS participaciones
            FROM holdings h
            JOIN partners p
                ON p.id = h.socio_id AND p.company_id = h.company_id
            WHERE h.company_id = ?
            AND h.right_type = 'plena'
            AND h.estado = 'vigente'
            GROUP BY p.id, p.nombre
            HAVING SUM(h.participaciones) > 0
            ORDER BY p.nombre
        """, conn, params=(company_id,))

        df_meta = pd.read_sql_query("""
            SELECT id, nombre, nif, nacionalidad, domicilio
            FROM partners
            WHERE company_id = ?
        """, conn, params=(company_id,))

    df_snap = df_snap[["socio_id", "participaciones"]]

    df = df_meta.merge(df_snap, left_on="id", right_on="socio_id", how="inner")
    df["Participaciones"] = df["participaciones"].fillna(0).astype(int)
    total_part = int(df["Participaciones"].sum())
    df["Cuota (%)"] = (df["Participaciones"] / total_part * 100).round(4) if total_part > 0 else 0.0

    # Orden/renombre columnas
    df = df[["nombre","nif","nacionalidad","domicilio","Participaciones","Cuota (%)"]].rename(columns={
        "nombre":"Nombre o razón social",
        "nif":"NIF",
        "nacionalidad":"Nacionalidad",
        "domicilio":"Domicilio"
    })

    # Fila TOTAL
    fila_total = {
        "Nombre o razón social":"TOTAL",
        "NIF":"",
        "Nacionalidad":"",
        "Domicilio":"",
        "Participaciones": total_part,
        "Cuota (%)": 100.0 if total_part>0 else 0.0
    }
    df = pd.concat([df, pd.DataFrame([fila_total])], ignore_index=True)

    # Formato visual
    def miles_es(x):
        try: return f"{int(x):,}".replace(",", ".")
        except: return x
    df["Participaciones"] = df["Participaciones"].map(miles_es)
    df["Cuota (%)"] = df["Cuota (%)"].map(lambda x: f"{x:.4f}%" if isinstance(x,(int,float)) else x)

    # Construcción tabla PDF
    story.append(Paragraph("<b>Socios actuales (plena propiedad)</b>", h2))
    headers = ["Nombre o razón social","NIF","Nacionalidad","Domicilio","Participaciones","Cuota (%)"]
    socios_data = [headers] + df[headers].values.tolist()

    soc_tbl = Table(socios_data, colWidths=[
        0.28*usable_w, 0.12*usable_w, 0.10*usable_w, 0.25*usable_w, 0.12*usable_w, 0.13*usable_w
    ], repeatRows=1)
    soc_tbl.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
        ('BOX',(0,0),(-1,-1),0.5,colors.black),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.grey),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('ALIGN',(-2,1),(-1,-1),'RIGHT'),
        ('FONTSIZE',(0,0),(-1,-1),9),
    ]))
    story.append(soc_tbl)
    story.append(PageBreak())

    # ========= Inventario =========
    story.append(Paragraph("<b>Inventario actual por rangos</b>", h2))
    inv_data = [["ID socio","Nombre","Derecho","Desde","Hasta","Participaciones"]]
    if isinstance(bloques, pd.DataFrame) and not bloques.empty:
        for _, r in bloques.reset_index(drop=True).iterrows():
            inv_data.append([
                str(r['socio_id']), r['nombre'] or "", r['right_type'],
                str(int(r['rango_desde'])), str(int(r['rango_hasta'])), str(int(r['participaciones']))
            ])
    inv_tbl = Table(inv_data,
        colWidths=[0.08*usable_w, 0.42*usable_w, 0.15*usable_w, 0.10*usable_w, 0.10*usable_w, 0.15*usable_w],
        repeatRows=1)
    inv_tbl.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
        ('BOX',(0,0),(-1,-1),0.5,colors.black),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.grey),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('ALIGN',(3,1),(5,-1),'RIGHT'),
        ('FONTSIZE',(0,0),(-1,-1),9),
    ]))
    story.append(inv_tbl)

    # ========= Movimientos =========
    # Cambiamos la plantilla de las SIGUIENTES páginas a 'moves' (con pie)
    if has_moves:
        story.append(NextPageTemplate('moves'))
        story.append(PageBreak())
    
    story.append(Paragraph("<b>Movimientos (hasta la fecha de corte)</b>", h2))

    # Cabecera con índice
    headers = [
        Paragraph("Nº", style_header),
        Paragraph("Fecha", style_header),
        Paragraph("Tipo", style_header),
        Paragraph("Socio transmite", style_header),
        Paragraph("Socio adquiere", style_header),
        Paragraph("Desde", style_header),
        Paragraph("Hasta", style_header),
        Paragraph("Participaciones", style_header),
        Paragraph("Nuevo valor nominal", style_header),
        Paragraph("Documento", style_header),
        Paragraph("Observaciones", style_header),
    ]
    mov_data = [headers]

    if isinstance(eventos, pd.DataFrame) and not eventos.empty:
        for i, (_, r) in enumerate(eventos.iterrows(), start=1):
            mov_data.append([
                str(i),
                "" if pd.isna(r["Fecha"]) else str(r["Fecha"]),
                Paragraph(TIPO_MAP.get(str(r["Tipo"]), str(r["Tipo"])), style_cell),
                P(r["Socio transmite"]),
                P(r["Socio adquiere"]),
                "" if pd.isna(r["Desde"]) else str(int(r["Desde"])),
                "" if pd.isna(r["Hasta"]) else str(int(r["Hasta"])),
                "" if pd.isna(r["Participaciones"]) else str(int(r["Participaciones"])),
                "" if pd.isna(r["Nuevo valor nominal"]) else str(r["Nuevo valor nominal"]),
                P(r["Documento"]),
                P(r["Observaciones"]),
            ])
    else:
        mov_data.append(["", "No hay movimientos hasta la fecha de corte","","","","","","","","",""])

    # Ajusta anchos (añadimos una col. pequeña para Nº)
    # Ajusta anchos (prioriza Documento/Observaciones, compacta Tipo)
    # Orden de columnas: Nº, Fecha, Tipo, Transmite, Adquiere, Desde, Hasta, Particip., Nuevo VN, Documento, Observaciones
    base_cm = [
        1.0,  # Nº
        2.0,  # Fecha
        1.4,  # Tipo (abreviado => más estrecho)
        4.2,  # Socio transmite
        4.2,  # Socio adquiere
        2.0,  # Desde
        2.0,  # Hasta
        2.6,  # Participaciones
        2.4,  # Nuevo valor nominal
        5.0,  # Documento (más ancho)
        6.0,  # Observaciones (más ancho)
    ]
    colWidths = [w*cm for w in base_cm]
    total = sum(colWidths)
    if total > 0:
        scale = usable_w / total
        colWidths = [w*scale for w in colWidths]

    mov_tbl = Table(mov_data, colWidths=colWidths, repeatRows=1, splitByRow=1, hAlign='LEFT')
    mov_tbl.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
        ('BOX',(0,0),(-1,-1),0.5,colors.black),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.grey),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('ALIGN',(5,1),(8,-1),'RIGHT'),   # numéricos a la derecha
        ('FONTNAME', (0,0), (-1,-1), 'DejaVuSans'),
        ('FONTSIZE',(0,0),(-1,-1),8),
        ('LEFTPADDING',(0,0),(-1,-1),3),
        ('RIGHTPADDING',(0,0),(-1,-1),3),
        ('TOPPADDING',(0,0),(-1,-1),2),
        ('BOTTOMPADDING',(0,0),(-1,-1),2),
    ]))
    story.append(mov_tbl)
    story.append(Spacer(1, 10))

    # Volvemos a plantilla sin pie para lo que venga después
    if has_moves:
        story.append(NextPageTemplate('default'))

    # ========= Firmas según órgano de gobierno =========
    gov = get_governance(conn, company_id)
    organo = gov["organo"]
    firmantes = gov["firmantes"]  # lista de dicts: {"nombre","rol"}

    def pick_signers(organo:str, firmantes:list[dict]):
        # Utilidades por rol
        by = {}
        for f in firmantes:
            by.setdefault(f["rol"], []).append(f["nombre"])

        lines = []  # cada línea: (cargo, nombre, nota_opcional)
        if organo == "admin_unico":
            nombre = (by.get("administrador_unico") or by.get("administrador único") or [""])[0]
            lines.append(("El Administrador Único", nombre, ""))

        elif organo == "admins_solidarios":
            admins = by.get("administrador_solidario") or by.get("administradores_solidarios") or []
            if not admins:  # fallback: por si no han marcado el rol exacto
                admins = [f["nombre"] for f in firmantes]
            # una línea por cada solidario, con nota
            for n in admins:
                lines.append(("Administrador Solidario", n, "(firma de uno de ellos)"))

        else:  # consejo
            pres = (by.get("presidente") or [""])[0]
            sec  = (by.get("secretario") or [])
            cd   = (by.get("consejero_delegado") or [])
            cons = (by.get("consejero") or [])
            vp   = (by.get("vicepresidente") or [])

            # Preferimos Presidente + Secretario
            if pres:
                lines.append(("Presidente del Consejo", pres, ""))
            if vp:
                lines.append(("Vicepresidente del Consejo", vp[0], ""))
            if sec:
                lines.append(("Secretario del Consejo", sec[0], ""))
            elif cd:
                lines.append(("Consejero Delegado", cd[0], ""))
            else:
                # si no hay secretario ni CD, usamos cualquier consejero disponible
                if cons:
                    lines.append(("Consejero", cons[0], ""))

            # Aseguramos al menos dos líneas en consejo
            if len(lines) == 1 and cons[1:]:
                lines.append(("Consejero", cons[1], ""))

            if not lines:  # fallback total
                lines = [("Presidente del Consejo", "", ""), ("Secretario del Consejo", "", "")]

        return lines

    sig_lines = pick_signers(organo, firmantes)

    story.append(Paragraph("<b>Firmas</b>", h2))
    sig_data = [["Cargo", "Nombre", ""]]
    for cargo, nombre, nota in sig_lines:
        sig_data.append([cargo, nombre, "_______________________________"])

    sig_tbl = Table(sig_data, colWidths=[0.28*usable_w, 0.42*usable_w, 0.30*usable_w])
    sig_tbl.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.whitesmoke),
        ('BOX',(0,0),(-1,-1),0.5,colors.black),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.grey),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('FONTSIZE',(0,0),(-1,-1),9),
    ]))
    story.append(sig_tbl)

    # Nota legal breve
    nota = "Las firmas anteriores se adecúan al órgano de administración configurado para la sociedad."
    story.append(Spacer(1, 6))
    story.append(Paragraph(nota, style_small))

    # Construcción principal
    doc.build(story)

    # Portada con hash (primera página)
    try:
        try:
            from pypdf import PdfReader, PdfWriter
        except Exception:
            from PyPDF2 import PdfReader, PdfWriter

        h = _sha256_file(path)
        tmp = str(Path(path).with_suffix(".portada.pdf"))
        c = _canvas.Canvas(tmp, pagesize=landscape(A4))
        c.setFont("Helvetica-Bold", 16); c.drawString(24, 560, "Libro Registro de Socios – Portada técnica")
        c.setFont("Helvetica", 11)
        y = 532
        for label, val in [
            ("Sociedad", nombre),
            ("CIF", cif),
            ("Fecha de corte", str(fecha_corte)),
            ("Generado el", datetime.now().strftime("%Y-%m-%d %H:%M")),
            ("Hash SHA-256", h),
        ]:
            c.drawString(24, y, f"{label}: {val}"); y -= 18
        c.showPage(); c.save()

        rd_doc  = PdfReader(path); rd_port = PdfReader(tmp)
        wr = PdfWriter()
        for p in rd_port.pages: wr.add_page(p)
        for p in rd_doc.pages:  wr.add_page(p)
        with open(path, "wb") as out: wr.write(out)
        try: Path(tmp).unlink(missing_ok=True)
        except Exception: pass
    except Exception:
        pass