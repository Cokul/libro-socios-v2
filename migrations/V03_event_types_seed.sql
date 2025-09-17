-- V03_event_types_seed.sql
-- Si tienes tabla de catálogo de tipos de evento, inserta los nuevos.
-- Si no la tienes, ignora este script (o crea la tabla si te interesa).
-- Ajusta nombres de columnas/tabla si son distintos.

-- CREATE TABLE IF NOT EXISTS event_types(id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
INSERT OR IGNORE INTO event_types(code, name) VALUES ('SUCESION', 'Sucesión (herencia)');
INSERT OR IGNORE INTO event_types(code, name) VALUES ('RED_AMORT', 'Reducción por amortización');