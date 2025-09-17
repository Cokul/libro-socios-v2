PRAGMA foreign_keys = ON;

-- === Tables ===

-- board_members
CREATE TABLE IF NOT EXISTS board_members (
            id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            cargo TEXT NOT NULL,
            nif TEXT,
            direccion TEXT,
            telefono TEXT,
            email TEXT, board_no INTEGER,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

-- companies
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cif TEXT UNIQUE NOT NULL,
    domicilio TEXT,
    fecha_constitucion DATE,
    valor_nominal REAL NOT NULL,
    participaciones_totales INTEGER NOT NULL
, organo TEXT DEFAULT 'admin_unico', firmantes_json TEXT, created_at TEXT, updated_at TEXT);

-- events
CREATE TABLE IF NOT EXISTS "events" (
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
  updated_at          TEXT, correlativo INTEGER,
  FOREIGN KEY(company_id) REFERENCES companies(id)
);

-- holdings
CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    socio_id INTEGER NOT NULL,
    right_type TEXT NOT NULL,
    rango_desde INTEGER NOT NULL,
    rango_hasta INTEGER NOT NULL,
    participaciones INTEGER NOT NULL,
    estado TEXT DEFAULT 'vigente',
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE, created_at TEXT, updated_at TEXT,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(socio_id) REFERENCES partners(id)
);

-- partners
CREATE TABLE IF NOT EXISTS partners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    nombre TEXT NOT NULL,
    nif TEXT,
    domicilio TEXT,
    tipo TEXT DEFAULT 'socio', nacionalidad TEXT, created_at TEXT, updated_at TEXT, fecha_nacimiento_constitucion TEXT, partner_no INTEGER,
    FOREIGN KEY(company_id) REFERENCES companies(id)
);

-- schema_meta
CREATE TABLE IF NOT EXISTS schema_meta(
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL DEFAULT 0,
            applied_at TEXT NOT NULL
        );

-- === Indexes ===

-- idx_board_members_company
CREATE INDEX IF NOT EXISTS idx_board_members_company ON board_members(company_id);

-- idx_events_company_correlativo
CREATE INDEX IF NOT EXISTS idx_events_company_correlativo ON events(company_id, correlativo);

-- idx_events_company_date
CREATE INDEX IF NOT EXISTS idx_events_company_date ON events(company_id, fecha, id);

-- idx_events_company_fecha_id
CREATE INDEX IF NOT EXISTS idx_events_company_fecha_id ON events(company_id, fecha, id);

-- idx_events_tipo
CREATE INDEX IF NOT EXISTS idx_events_tipo ON events(tipo);

-- idx_holdings_company_estado
CREATE INDEX IF NOT EXISTS idx_holdings_company_estado
              ON holdings(company_id, estado);

-- idx_holdings_company_flags
CREATE INDEX IF NOT EXISTS idx_holdings_company_flags ON holdings(company_id, right_type, estado);

-- idx_holdings_company_right_rangos
CREATE INDEX IF NOT EXISTS idx_holdings_company_right_rangos
              ON holdings(company_id, right_type, rango_desde, rango_hasta);

-- idx_holdings_company_socio
CREATE INDEX IF NOT EXISTS idx_holdings_company_socio
              ON holdings(company_id, socio_id);

-- idx_partners_company
CREATE INDEX IF NOT EXISTS idx_partners_company ON partners(company_id);

-- idx_partners_company_name
CREATE INDEX IF NOT EXISTS idx_partners_company_name ON partners(company_id, nombre);

-- idx_partners_company_nombre
CREATE INDEX IF NOT EXISTS idx_partners_company_nombre
          ON partners(company_id, nombre);

-- ix_board_members_company
CREATE INDEX IF NOT EXISTS ix_board_members_company ON board_members(company_id);

-- ix_board_members_company_no
CREATE INDEX IF NOT EXISTS ix_board_members_company_no ON board_members(company_id, board_no);

-- ix_partners_company
CREATE INDEX IF NOT EXISTS ix_partners_company ON partners(company_id);

-- ix_partners_company_partnerno
CREATE INDEX IF NOT EXISTS ix_partners_company_partnerno ON partners(company_id, partner_no);

-- === Views ===

-- view_cuotas_actuales
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

-- view_participaciones_actuales
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

-- === Triggers ===

-- trg_companies_updated_at
CREATE TRIGGER trg_companies_updated_at
    AFTER UPDATE ON companies
    FOR EACH ROW
    WHEN NEW.updated_at = OLD.updated_at
    BEGIN
        UPDATE companies
           SET updated_at = CURRENT_TIMESTAMP
         WHERE id = NEW.id;
    END;

-- trg_events_check_nominal_ins
CREATE TRIGGER trg_events_check_nominal_ins
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

-- trg_events_check_nominal_upd
CREATE TRIGGER trg_events_check_nominal_upd
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

-- trg_events_reden_mode_ins
CREATE TRIGGER trg_events_reden_mode_ins
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

-- trg_events_reden_mode_upd
CREATE TRIGGER trg_events_reden_mode_upd
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

-- trg_events_required_parties_ins
CREATE TRIGGER trg_events_required_parties_ins
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

-- trg_events_required_parties_upd
CREATE TRIGGER trg_events_required_parties_upd
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

-- trg_holdings_updated_at
CREATE TRIGGER trg_holdings_updated_at
    AFTER UPDATE ON holdings
    FOR EACH ROW
    WHEN NEW.updated_at = OLD.updated_at
    BEGIN
        UPDATE holdings
           SET updated_at = CURRENT_TIMESTAMP
         WHERE id = NEW.id;
    END;

-- trg_partners_updated_at
CREATE TRIGGER trg_partners_updated_at
    AFTER UPDATE ON partners
    FOR EACH ROW
    WHEN NEW.updated_at = OLD.updated_at
    BEGIN
        UPDATE partners
           SET updated_at = CURRENT_TIMESTAMP
         WHERE id = NEW.id;
    END;

-- === Seed schema version ===
INSERT INTO schema_meta(version, applied_at) VALUES (1, DATE('now'));

-- Reset autoincrement sequences
DELETE FROM sqlite_sequence;