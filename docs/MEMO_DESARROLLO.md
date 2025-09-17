# 🛠️ Libro de Socios – Memo de Desarrollo (V2)

Este documento describe la arquitectura técnica de la aplicación, sus módulos principales y las reglas de negocio que implementa.  
Está pensado para un desarrollador que quiera mantener, extender o portar la aplicación a otra infraestructura.

---

## 1. Arquitectura general

La aplicación sigue una estructura en capas:
(UI (Streamlit)  →  Servicios (core/services)  →  Repositorios (core/repositories)  →  Infraestructura (SQLite, PDF/Excel))
- **UI (Streamlit)**  
  - Punto de entrada: `app/streamlit_app.py`.  
  - Layout general en `app/ui/layout.py`.  
  - Páginas en `app/ui/pages/*` (sociedades, socios, eventos, reports, utilities…).  
  - El usuario interactúa siempre desde Streamlit, que invoca a los servicios.

- **Servicios (`app/core/services`)**  
  - Contienen la **lógica de negocio**.  
  - Orquestan validaciones y llamadas a los repositorios.  
  - Ejemplos: `events_service.py`, `partners_service.py`, `companies_service.py`, `reporting_service.py`, `export_service.py`.

- **Repositorios (`app/core/repositories`)**  
  - Encapsulan acceso a datos (SQLite).  
  - Definen operaciones CRUD sobre `companies`, `partners`, `events`, `governance`.  
  - Se apoyan en `infra/db.py` para obtener conexiones.

- **Infraestructura (`app/infra`)**  
  - `db.py`: conexión SQLite y utilidades.  
  - `logging.py`: logging con `RotatingFileHandler` a `logs/app.log`.  
  - `constants.py`: rutas de datos y backups (`data/libro_socios.db`, `data/backups`).  
  - `pdf_fonts.py`: registro de fuentes DejaVuSans para ReportLab.

---

## 2. Modelo de datos

La base de datos es **SQLite**, ubicada por defecto en `data/libro_socios.db`.

Tablas principales:
- **companies** – sociedades (nombre, CIF, datos básicos).  
- **partners** – socios/titulares (nombre, NIF, domicilio, nacionalidad, fecha).  
- **events** – actos societarios (tipo, fecha, socios, rangos, valor nominal, documento, observaciones).  
- **governance** – información de órganos de administración.  

Existen **migrations SQL** en `/migrations` para bootstrap y ampliaciones de esquema.

Triggers importantes:
- Validan coherencia en **redenominaciones** (global vs por bloque).  
- Obligan a VN > 0 en ampliaciones/reducciones de valor.  
- Bloquean rangos incompletos.

---

## 3. Servicios clave

- **events_service.py**  
  - Valida rangos, valor nominal y modalidades de redenominación.  
  - Expone CRUD y listados filtrados para UI.  
  - Se asegura de que los triggers de BD se carguen.

- **partners_service.py**  
  - Alta/edición de socios.  
  - Control de NIF/NIE normalizados.  

- **companies_service.py**  
  - Alta/edición de sociedades.  
  - Control de CIF normalizado.  

- **compute_service.py** y **reporting_service.py**  
  - Calculan el “estado” (cap table, timeline, históricos).  
  - Normalizan tipos de evento con `normalize_event_type`.  

- **export_service.py**  
  - Exportación a **PDF** (con ReportLab) y **Excel** (con XlsxWriter).  
  - Incluye Libro Registro legalizable, certificados y reportes económicos.  

- **backup_service.py**  
  - Copias de seguridad de la base de datos (`data/backups`).  

- **maintenance_service.py**  
  - Operaciones de mantenimiento: `ANALYZE`, `REINDEX`, `VACUUM`.  
  - Comprobaciones de salud: `PRAGMA integrity_check`, `PRAGMA foreign_key_check`.

---

## 4. Reglas de negocio esenciales

- **Eventos**  
  - Cada evento refleja un acto jurídico único.  
  - Validaciones de campos en UI + triggers en BD.  
  - **Redenominación**:  
    - Global – constancia: VN opcional (>0).  
    - Global – recálculo: VN obligatorio, capital múltiplo exacto.  
    - Por bloque: requiere socio + rango, VN opcional.  

- **Consistencia del capital**  
  - Siempre se cumple:  
    ```
    Capital = nº de participaciones × valor nominal
    ```
  - Nunca se permite VN = 0 ni capital fraccionado.  

- **Socios y sociedades**  
  - CIF/NIF se normalizan (mayúsculas, sin guiones, sin espacios).  
  - Los nombres se usan tal cual para etiquetas en UI.

---

## 5. Exportación

- **PDF**: ReportLab con fuentes DejaVuSans (`app/assets/fonts/`).  
- **Excel**: XlsxWriter, hojas separadas para movimientos y cap table.  
- **Reports disponibles**:  
  - Libro Registro de Socios (legalizable).  
  - Certificados individuales.  
  - Resumen de movimientos.

---

## 6. Logging y utilidades

- Log de aplicación: `logs/app.log`, rotativo.  
- Desde “Utilidades” el usuario puede:  
  - Hacer backups de BD.  
  - Ejecutar mantenimiento (ANALYZE/REINDEX/VACUUM).  
  - Consultar logs.  
  - Importar CSVs normalizados.  

---

## 7. Extensibilidad y portabilidad

- **Añadir un nuevo tipo de evento**:  
  - Definirlo en `core/enums.py`.  
  - Añadir validaciones en `events_service.py` y triggers si procede.  
  - Ajustar UI en `ui/pages/events.py`.

- **Cambiar de base de datos**:  
  - Reescribir `infra/db.py` para otra tecnología (p. ej. Postgres).  
  - Mantener interfaces en `repositories/*`.  
  - La lógica de negocio en `services/*` y la UI seguirán funcionando igual.

- **Ampliar exportaciones**:  
  - Implementar en `export_service.py` y conectar en la UI de *Reports*.

---

## 8. Requisitos técnicos

- **Python ≥ 3.10** (uso de `str | None` en type hints).  
- Dependencias externas:  
  - `streamlit` – interfaz.  
  - `pandas` – dataframes.  
  - `reportlab` – PDFs.  
  - `XlsxWriter` – Excel.  

---

## 9. Tests

En `/tests` se incluyen pruebas básicas:  
- `test_enums.py` – validación de enums.  
- `test_validators.py` – validaciones de campos.  
- `test_normalization.py` – normalización de socios/eventos.  

Se recomienda extender con pruebas de servicios.

---

## 10. Mantenimiento

- Usar `requirements.txt` para instalación básica.  
- `requirements-dev.txt` para entorno de desarrollo (pytest, mypy, linters).  
- Revisar y aplicar `migrations/*.sql` si cambian los esquemas.  
- Mantener actualizado el log de cambios en commits y tags (`v2.0.0`, etc.).