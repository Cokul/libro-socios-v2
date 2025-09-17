-- V04_add_companies_extras.sql
-- Añade columnas opcionales a companies (ISO yyyy-mm-dd para fecha_constitucion)
ALTER TABLE companies ADD COLUMN domicilio TEXT;
ALTER TABLE companies ADD COLUMN fecha_constitucion TEXT;