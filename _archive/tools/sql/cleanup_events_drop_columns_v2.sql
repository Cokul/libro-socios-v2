PRAGMA foreign_keys = OFF;

-- Limpia restos de ejecuciones previas
DROP TABLE IF EXISTS events_new;
DROP TABLE IF EXISTS events_backup_old;

BEGIN;

-- Esquema final SIN: socio_origen_id, socio_destino_id, participaciones, referencia
CREATE TABLE events_new (
  id                  INTEGER PRIMARY KEY,
  company_id          INTEGER NOT NULL,
  fecha               TEXT    NOT NULL,
  tipo                TEXT    NOT NULL,
  socio_transmite     INTEGER,
  socio_adquiere      INTEGER,
  rango_desde         INTEGER,
  rango_hasta         INTEGER,
  nuevo_valor_nominal REAL,
  documento           TEXT,
  observaciones       TEXT,
  hora                TEXT,
  orden_del_dia       INTEGER,
  created_at          TEXT,
  updated_at          TEXT,
  FOREIGN KEY(company_id) REFERENCES companies(id)
);

-- OJO: si tu tabla 'events' NO tiene 'referencia', ver variante más abajo
INSERT INTO events_new (
  id, company_id, fecha, tipo,
  socio_transmite, socio_adquiere,
  rango_desde, rango_hasta,
  nuevo_valor_nominal, documento, observaciones,
  hora, orden_del_dia, created_at, updated_at
)
SELECT
  id, company_id, fecha, UPPER(tipo),
  socio_transmite, socio_adquiere,
  rango_desde, rango_hasta,
  nuevo_valor_nominal, documento,
  COALESCE(NULLIF(referencia, ''), observaciones) AS observaciones,
  hora, orden_del_dia, created_at, updated_at
FROM events;

-- Renombrar tablas
ALTER TABLE events RENAME TO events_backup_old;
ALTER TABLE events_new RENAME TO events;

-- Índices recomendados
CREATE INDEX IF NOT EXISTS idx_events_company_date ON events(company_id, fecha, id);
CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);

COMMIT;

PRAGMA foreign_keys = ON;