# db.py
import sqlite3
from datetime import datetime
from config import DB_PATH


# ----------------------------- Helpers / Migrations -----------------------------

def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """
    Devuelve True si la columna existe en la tabla.
    """
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def migrate_add_nacionalidad(conn: sqlite3.Connection) -> None:
    """
    Asegura la columna partners.nacionalidad y rellena valor por defecto 'Española'
    cuando esté NULL o cadena vacía.
    """
    if not column_exists(conn, "partners", "nacionalidad"):
        conn.execute("ALTER TABLE partners ADD COLUMN nacionalidad TEXT")

    # backfill seguro (sirve tanto si acabamos de crear la columna como si ya existía)
    conn.execute("""
        UPDATE partners
           SET nacionalidad = 'Española'
         WHERE nacionalidad IS NULL OR TRIM(nacionalidad) = ''
    """)


# --------------------------------- Conexión ------------------------------------

def get_connection() -> sqlite3.Connection:
    """
    Devuelve una conexión lista para usar y que:
    - Permite hilos (Streamlit re-ejecuta el script).
    - Espera si la BD está ocupada (timeout).
    - Activa foreign_keys.
    - Usa WAL para reducir bloqueos.
    """
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")  # 5s extra para evitar "database is locked"
    except Exception:
        # En entornos donde PRAGMA pudiera fallar no interrumpimos la app
        pass
    return conn

# --- Esquema / Versionado ---
def ensure_schema_meta(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_meta(
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL DEFAULT 0,
            applied_at TEXT NOT NULL
        )
    """)
    row = conn.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_meta(id, version, applied_at) VALUES (1, 0, datetime('now'))")
    conn.commit()

def get_schema_version(conn: sqlite3.Connection) -> int:
    ensure_schema_meta(conn)
    v = conn.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    return int(v[0]) if v else 0

def set_schema_version(conn: sqlite3.Connection, new_version: int) -> None:
    conn.execute("UPDATE schema_meta SET version=?, applied_at=datetime('now') WHERE id=1", (new_version,))
    conn.commit()

# ------------------------------ Inicialización ---------------------------------

def init_db() -> None:
    """
    Inicializa la base de datos y aplica migraciones versionadas.
    Puede invocarse en cada arranque (idempotente).
    """
    with get_connection() as conn:
        cur = conn.cursor()
        # Esquema base
        with open("models.sql", "r", encoding="utf-8") as f:
            cur.executescript(f.read())
        conn.commit()

        # Asegura tabla de versionado y aplica migraciones
        ensure_schema_meta(conn)
        final_ver = apply_migrations(conn)

        # Índices (idempotentes; por si migraciones añadieron tablas/cols)
        ensure_indexes(conn)

        conn.commit()
        # Opcional: log simple en consola
        try:
            print(f"[init_db] schema version = {final_ver}")
        except Exception:
            pass


# --------------------------------- Índices -------------------------------------

def ensure_indexes(conn: sqlite3.Connection) -> None:
    """
    Crea índices necesarios para acelerar consultas habituales (solo si existen las tablas).
    """
    def table_exists(name: str) -> bool:
        r = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (name,)
        ).fetchone()
        return r is not None

    # events
    if table_exists("events"):
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_company_fecha_id
              ON events(company_id, fecha, id);
        """)

    # partners
    if table_exists("partners"):
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_partners_company_nombre
              ON partners(company_id, nombre);
        """)

    # holdings (muy útil para joins y recálculos)
    if table_exists("holdings"):
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_holdings_company_socio
              ON holdings(company_id, socio_id);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_holdings_company_estado
              ON holdings(company_id, estado);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_holdings_company_right_rangos
              ON holdings(company_id, right_type, rango_desde, rango_hasta);
        """)

    conn.commit()


# --------------------------- Columnas / Migraciones ----------------------------

def ensure_extra_columns(conn: sqlite3.Connection) -> None:
    """
    Añade columnas opcionales y realiza migraciones ligeras:
      - companies.organo + companies.firmantes_json 
      - events.hora (TEXT)
      - events.orden_del_dia (INTEGER)
      - partners.nacionalidad (TEXT) + backfill 'Española'
    """
    
    # ---- companies.organo + companies.firmantes_json ----
    cols_comp = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
    if "organo" not in cols_comp:
        conn.execute("ALTER TABLE companies ADD COLUMN organo TEXT DEFAULT 'admin_unico'")
    if "firmantes_json" not in cols_comp:
        conn.execute("ALTER TABLE companies ADD COLUMN firmantes_json TEXT")
    
    # ---- events.hora / events.orden_del_dia ----
    cols_events = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
    if "hora" not in cols_events:
        conn.execute("ALTER TABLE events ADD COLUMN hora TEXT;")
    if "orden_del_dia" not in cols_events:
        conn.execute("ALTER TABLE events ADD COLUMN orden_del_dia INTEGER;")

    # ---- partners.nacionalidad ----
    migrate_add_nacionalidad(conn)

    conn.commit()


# --------------------------- Utilidades de mantenimiento -----------------------

def backfill_orden_del_dia(conn: sqlite3.Connection, company_id: int) -> None:
    """
    Rellena events.orden_del_dia de forma incremental por fecha (si estaba a NULL).
    Útil para ordenar eventos en la misma fecha.
    """
    cur = conn.execute("""
        SELECT id, fecha
          FROM events
         WHERE company_id = ?
         ORDER BY fecha, id
    """, (company_id,))
    rows = cur.fetchall()

    from collections import defaultdict
    counters = defaultdict(int)

    for row in rows:
        ev_id = row["id"]
        fecha = row["fecha"]
        counters[fecha] += 1
        conn.execute(
            "UPDATE events SET orden_del_dia = ? WHERE id = ? AND orden_del_dia IS NULL",
            (counters[fecha], ev_id)
        )

    conn.commit()
    
def compact_db() -> dict:
    """
    Ejecuta wal_checkpoint(TRUNCATE) + VACUUM + ANALYZE para:
      - Consolidar el WAL y reducir tamaño.
      - Reescribir el fichero .db y desfragmentar.
      - Actualizar estadísticas de índices.
    Devuelve un dict con info básica para mostrar en UI/log.
    """
    start = datetime.now()
    # Usamos una conexión corta y aislada
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
        except Exception:
            pass

        cur = conn.cursor()
        # Recomendable en modo WAL antes de VACUUM
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        # VACUUM debe ir fuera de transacciones activas
        cur.execute("VACUUM;")
        cur.execute("ANALYZE;")
        cur.close()

    elapsed = (datetime.now() - start).total_seconds()
    return {"ok": True, "elapsed_s": elapsed, "db_path": DB_PATH}

# -------- Migraciones numeradas (idempotentes) --------
def mig_1_add_schema_basics(conn: sqlite3.Connection) -> None:
    """
    v1:
    - Añade indices básicos si faltan (backup de ensure_indexes por si el usuario llega aquí sin ellos).
    """
    ensure_indexes(conn)  # idempotente

def mig_2_add_events_hora_orden(conn: sqlite3.Connection) -> None:
    """
    v2:
    - Asegura events.hora (TEXT) y events.orden_del_dia (INTEGER)
    """
    cols_events = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
    if "hora" not in cols_events:
        conn.execute("ALTER TABLE events ADD COLUMN hora TEXT")
    if "orden_del_dia" not in cols_events:
        conn.execute("ALTER TABLE events ADD COLUMN orden_del_dia INTEGER")

def mig_3_companies_governance(conn: sqlite3.Connection) -> None:
    """
    v3:
    - Asegura companies.organo y companies.firmantes_json
    """
    cols_comp = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
    if "organo" not in cols_comp:
        conn.execute("ALTER TABLE companies ADD COLUMN organo TEXT DEFAULT 'admin_unico'")
    if "firmantes_json" not in cols_comp:
        conn.execute("ALTER TABLE companies ADD COLUMN firmantes_json TEXT")

def mig_4_partners_nacionalidad(conn: sqlite3.Connection) -> None:
    """
    v4:
    - Asegura partners.nacionalidad + backfill 'Española' si está NULL o vacío
    """
    migrate_add_nacionalidad(conn)

def mig_5_audit_columns(conn: sqlite3.Connection) -> None:
    """
    v5:
    - Auditoría ligera: created_at, updated_at en tablas clave (si no existen)
    - Trigger UPDATE para updated_at
    """
    def ensure_audit_for(table: str, pk: str = "id"):
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if "created_at" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN created_at TEXT")
            conn.execute(f"UPDATE {table} SET created_at = COALESCE(created_at, datetime('now'))")
        if "updated_at" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN updated_at TEXT")
            conn.execute(f"UPDATE {table} SET updated_at = COALESCE(updated_at, datetime('now'))")
        # Trigger update
        conn.execute(f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_updated_at
            AFTER UPDATE ON {table}
            FOR EACH ROW
            BEGIN
                UPDATE {table} SET updated_at = datetime('now') WHERE {pk} = NEW.{pk};
            END;
        """)

    # Ajusta pks si fuera distinto
    ensure_audit_for("companies", "id")
    ensure_audit_for("partners", "id")
    ensure_audit_for("events", "id")
    ensure_audit_for("holdings", "id")

# --- v6: Auditoría ligera (created_at / updated_at + triggers) ---

def _ensure_audit_cols(conn: sqlite3.Connection, table: str) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if "created_at" not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN created_at TEXT")
    if "updated_at" not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN updated_at TEXT")

def _backfill_audit(conn: sqlite3.Connection, table: str) -> None:
    # Rellena nulos con timestamps actuales
    conn.execute(
        f"""UPDATE {table}
               SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
                   updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        """
    )

def _create_updated_at_trigger(conn: sqlite3.Connection, table: str, pk: str = "id") -> None:
    # Evita recursión: la segunda UPDATE sólo cambia updated_at -> WHEN ya no coincide
    conn.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at;")
    conn.executescript(f"""
    CREATE TRIGGER IF NOT EXISTS trg_{table}_updated_at
    AFTER UPDATE ON {table}
    FOR EACH ROW
    WHEN NEW.updated_at = OLD.updated_at
    BEGIN
        UPDATE {table}
           SET updated_at = CURRENT_TIMESTAMP
         WHERE {pk} = NEW.{pk};
    END;
    """)

def migrate_v6_auditoria(conn: sqlite3.Connection) -> None:
    tables = ["companies", "partners", "events", "holdings"]
    for t in tables:
        _ensure_audit_cols(conn, t)
        _backfill_audit(conn, t)
        _create_updated_at_trigger(conn, t, pk="id")
    conn.commit()

# Lista ordenada de migraciones: (versión_objetivo, función)
MIGRATIONS: list[tuple[int, callable]] = [
    (1, mig_1_add_schema_basics),
    (2, mig_2_add_events_hora_orden),
    (3, mig_3_companies_governance),
    (4, mig_4_partners_nacionalidad),
    (5, mig_5_audit_columns),
    (6, migrate_v6_auditoria),
]

def apply_migrations(conn: sqlite3.Connection) -> int:
    """
    Aplica migraciones pendientes, dentro de transacciones por versión.
    Devuelve la versión final.
    """
    ensure_schema_meta(conn)
    current = get_schema_version(conn)
    for ver, fn in MIGRATIONS:
        if ver > current:
            # Cada migración en su propia transacción: si falla, no se avanza de versión
            try:
                conn.execute("BEGIN")
                fn(conn)
                set_schema_version(conn, ver)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise RuntimeError(f"Fallo en migración v{ver}: {e}") from e
    return get_schema_version(conn)