from pathlib import Path
import sqlite3
import shutil
import time

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "libro_socios.db"
BACKUP_PATH = ROOT / "data" / f"libro_socios_pre_events_cleanup_{int(time.time())}.db"

def backup_db():
    if DB_PATH.exists():
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"Backup creado: {BACKUP_PATH}")

def main():
    backup_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        # No usamos BEGIN/COMMIT manuales; dejamos que Python gestione la transacción.
        # Si algo falla, hacemos rollback más abajo.

        # 1) Crear tabla nueva "limpia"
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events_new (
          id INTEGER PRIMARY KEY,
          company_id INTEGER NOT NULL,
          correlativo INTEGER,
          fecha TEXT NOT NULL,
          tipo TEXT NOT NULL,
          socio_transmite INTEGER,
          socio_adquiere INTEGER,
          rango_desde INTEGER,
          rango_hasta INTEGER,
          n_participaciones INTEGER,
          nuevo_valor_nominal REAL,
          documento TEXT,
          observaciones TEXT,
          FOREIGN KEY(company_id) REFERENCES companies(id)
        );
        """)

        # 2) Copiar datos desde la tabla antigua mapeando columnas
        conn.executescript("""
        INSERT INTO events_new (
          id, company_id, correlativo, fecha, tipo,
          socio_transmite, socio_adquiere,
          rango_desde, rango_hasta, n_participaciones,
          nuevo_valor_nominal, documento, observaciones
        )
        SELECT
          e.id,
          e.company_id,
          e.correlativo,
          e.fecha,
          UPPER(e.tipo) as tipo,
          COALESCE(e.socio_transmite, e.socio_origen_id) as socio_transmite,
          COALESCE(e.socio_adquiere, e.socio_destino_id) as socio_adquiere,
          e.rango_desde,
          e.rango_hasta,
          e.n_participaciones,
          e.nuevo_valor_nominal,
          e.documento,
          COALESCE(NULLIF(e.referencia, ''), e.observaciones) as observaciones
        FROM events e;
        """)

        # 3) Renombrar tablas
        conn.execute("ALTER TABLE events RENAME TO events_backup_old;")
        conn.execute("ALTER TABLE events_new RENAME TO events;")

        # 4) Índices recomendados
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_company_date ON events(company_id, fecha, id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);")

        # Confirmar todo lo anterior
        conn.commit()

        # 5) Recalcular correlativo por compañía (en una transacción independiente)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        company_ids = [r["id"] for r in conn.execute("SELECT id FROM companies").fetchall()]

        for cid in company_ids:
            conn.execute("DROP TABLE IF EXISTS _tmp_corr;")
            conn.execute(f"""
                CREATE TEMP TABLE _tmp_corr AS
                SELECT id, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY fecha, id) AS rn
                FROM events
                WHERE company_id={cid}
            """)
            conn.execute("""
                UPDATE events
                   SET correlativo = (SELECT rn FROM _tmp_corr WHERE _tmp_corr.id = events.id)
                 WHERE company_id=?
            """, (cid,))
            conn.execute("DROP TABLE IF EXISTS _tmp_corr;")
        conn.commit()

        print("✅ Migración de 'events' completada. Tabla limpia y correlativos recalculados.")
        print("ℹ️ La tabla antigua se guardó como 'events_backup_old' por si necesitas comparar.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        print("Se ha hecho rollback. Tienes el backup para restaurar si lo necesitas.")
        raise
    finally:
        try:
            conn.execute("PRAGMA foreign_keys=ON")
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()