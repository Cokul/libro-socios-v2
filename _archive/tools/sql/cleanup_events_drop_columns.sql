-- Limpieza tabla events: elimina columnas redundantes
-- Columnas a eliminar detectadas: socio_origen_id, socio_destino_id, participaciones, referencia
-- Se mantienen: id, company_id, fecha, tipo, socio_transmite, socio_adquiere,
--               rango_desde, rango_hasta, nuevo_valor_nominal,
--               documento, observaciones, hora, orden_del_dia, created_at, updated_at

PRAGMA foreign_keys = OFF;

BEGIN;

CREATE TABLE events_new (
  id                INTEGER PRIMARY KEY,
  company_id        INTEGER NOT NULL,
  fecha             DATE    NOT NULL,
  tipo              TEXT    NOT NULL,
  socio_transmite   INTEGER,
  socio_adquiere    INTEGER,
  rango_desde       INTEGER,
  rango_hasta       INTEGER,
  nuevo_valor_nominal REAL,
  documento         TEXT,
  observaciones     TEXT,
  hora              TEXT,
  orden_del_dia     INTEGER,
  created_at        TEXT,
  updated_at        TEXT,
  FOREIGN KEY(company_id) REFERENCES companies(id)
);

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

ALTER TABLE events RENAME TO events_backup_old;
ALTER TABLE events_new RENAME TO events;

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_events_company_date ON events(company_id, fecha, id);
CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);

COMMIT;

PRAGMA foreign_keys = ON;