# 📘 Libro Registro de Socios – v2

[![CI](https://github.com/Cokul/libro-socios-v2/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Cokul/libro-socios-v2/actions/workflows/ci.yml)

Aplicación en **Streamlit + SQLite** para la gestión del Libro Registro de Socios conforme a la **Ley de Sociedades de Capital**.  

Incluye funcionalidades de:

- Gestión de sociedades y socios
- Registro de eventos societarios (ampliaciones, transmisiones, redenominaciones, etc.)
- Exportación en **PDF** y **Excel**
- Copias de seguridad automáticas
- Normalización de datos (nombres, NIF/CIF)
- Tests automáticos y tipado gradual
- Mantenimiento y chequeos de integridad de la base de datos

---

## 🚀 Instalación

Requisitos previos:
- **Python ≥ 3.10**
- Git

Clonar el repo y crear un entorno virtual:

```bash
git clone git@github.com:Cokul/libro-socios-v2.git
cd libro-socios-v2
python3 -m venv .venv
source .venv/bin/activate   # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## ▶️ Ejecución

```bash
streamlit run app/streamlit_app.py
```

La aplicación usará la base de datos en `data/libro_socios.db`.  
👉 Nota: este repositorio no incluye bases de datos reales. Para pruebas, inicializa la base con las migraciones de `/migrations`.

---

## 🧪 Tests

Ejecutar la batería de tests con `pytest`:

```bash
pytest --cov=app --cov-report=term-missing
```

---

## 🛠️ Tipado estático

El proyecto utiliza **mypy** para tipado gradual:

```bash
mypy app
```

---

## 📂 Estructura del proyecto

```
app/
 ├── core/          # Servicios, validadores, lógica de negocio
 ├── infra/         # Conexión DB, logging, fuentes PDF
 ├── ui/            # Páginas Streamlit
 ├── streamlit_app.py
migrations/         # Scripts SQL de bootstrap/esquema
tests/              # Tests con pytest
docs/               # Manuales y memo de desarrollo
.github/workflows/  # CI en GitHub Actions
```

---

## 📖 Documentación adicional

- docs/MANUAL_USO.md
- docs/MEMO_DESARROLLO.md
- docs/INSTALL_macOS_win.md

---

## 📜 Licencia

Este proyecto se distribuye bajo la licencia [MIT](LICENSE).
