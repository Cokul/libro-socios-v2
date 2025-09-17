# 📘 Libro de Socios – v2  

**Versión 2.0.0 – Publicación inicial completa**

---

## ✨ Novedades principales
- Aplicación en **Streamlit + SQLite** para la gestión del **Libro Registro de Socios** conforme a la Ley de Sociedades de Capital.  
- Auto-inicialización de la base de datos (`app/infra/init_db.sql`), sin necesidad de migrations.  
- Manuales incluidos en `docs/`:  
  - `MANUAL_USO.md`: guía funcional para usuarios.  
  - `MEMO_DESARROLLO.md`: arquitectura técnica para desarrolladores.  
  - `INSTALL_macOS_win.md`: instrucciones de instalación en macOS y Windows.  
- Limpieza de archivos internos y migraciones antiguas.  
- CI simplificado con smoke test (importación de la app y creación de la base de datos).  

---

## 🛠️ Instalación rápida
1. Descarga este repositorio (`Code → Download ZIP`) o clona con Git:  
   ```bash
   git clone https://github.com/Cokul/libro-socios-v2.git
   cd libro-socios-v2
   ```
2. Sigue la guía detallada en [`docs/INSTALL_macOS_win.md`](docs/INSTALL_macOS_win.md).  

---

## ▶️ Ejecución
Con el entorno configurado:  
```bash
streamlit run app/streamlit_app.py
```
La aplicación se abrirá en tu navegador en [http://localhost:8501](http://localhost:8501).

---

## 📂 Archivos incluidos en esta release
- Código fuente de la aplicación (`app/`).  
- Manuales de uso e instalación (`docs/`).  
- Tests básicos (`tests/`).  
- Archivos auxiliares: `.gitignore`, `requirements.txt`, `.github/workflows/ci.yml`.  

---
