-- V02_add_partners_board_columns.sql
-- Añade columnas nuevas (ajusta nombres si difieren de tu esquema).
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

-- partners: fecha de nacimiento / constitución
ALTER TABLE partners ADD COLUMN fecha_nacimiento_constitucion TEXT;

-- board_members: datos de contacto
ALTER TABLE board_members ADD COLUMN direccion TEXT;
ALTER TABLE board_members ADD COLUMN telefono TEXT;
ALTER TABLE board_members ADD COLUMN email TEXT;

COMMIT;
PRAGMA foreign_keys=ON;