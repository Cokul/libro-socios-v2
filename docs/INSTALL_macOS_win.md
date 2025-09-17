# 📖 Guía de Instalación – Libro de Socios (macOS y Windows)

Este documento explica, paso a paso y sin necesidad de conocimientos técnicos, cómo instalar y poner en marcha la aplicación **Libro de Socios – v2** en un ordenador Mac o Windows.

---

## 1. Antes de empezar

### ¿Qué vas a instalar?
- **Python**: el lenguaje en el que está hecha la aplicación (necesario para ejecutarla).  
- **Git**: herramienta para descargar la aplicación desde GitHub.  
- **Libro de Socios – v2**: la propia aplicación.  

### ¿Qué necesitas?
- Un ordenador con **macOS o Windows**.  
- Conexión a Internet.  

---

## 2. Instalar Python y Git

### En macOS
1. Abre Safari o Chrome y descarga **Python 3.10 o superior** desde la página oficial:  
   👉 [https://www.python.org/downloads/](https://www.python.org/downloads/)  
   Haz clic en **Download Python 3.x.x** (elige la última versión 3.x).  
   Una vez descargado, abre el archivo `.pkg` y sigue los pasos de instalación.

2. Para instalar **Git**, la forma más sencilla es usar Homebrew (si lo tienes):  
   - Abre la app **Terminal**.  
   - Escribe:  
     ```bash
     brew install git
     ```  
   Si no tienes Homebrew, descárgalo desde: 👉 [https://git-scm.com/download/mac](https://git-scm.com/download/mac)

### En Windows
1. Descarga **Python 3.10 o superior** desde:  
   👉 [https://www.python.org/downloads/windows/](https://www.python.org/downloads/windows/)  
   Elige **Windows installer (64-bit)**. Durante la instalación, marca la casilla **"Add Python to PATH"** antes de continuar.

2. Descarga **Git para Windows** desde:  
   👉 [https://git-scm.com/download/win](https://git-scm.com/download/win)  
   Abre el instalador `.exe` y sigue los pasos por defecto.

---

## 3. Descargar la aplicación

1. Abre una **ventana de terminal** (en macOS) o **PowerShell** (en Windows).  
2. Escribe este comando para descargar la aplicación desde GitHub:

```bash
git clone https://github.com/Cokul/libro-socios-v2.git
```

Esto creará una carpeta llamada `libro-socios-v2` en tu ordenador.  
3. Entra en la carpeta descargada:

```bash
cd libro-socios-v2
```

---

## 4. Crear un entorno aislado

Este paso prepara un “espacio separado” para que la aplicación use sus propios programas sin interferir con el resto del ordenador.

- En **macOS**:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```

- En **Windows**:
  ```powershell
  python -m venv .venv
  .venv\Scripts\activate
  ```

Al activarse, verás que en la línea de comandos aparece `(.venv)` al principio.

---

## 5. Instalar dependencias

Con el entorno activado, instala lo necesario para que funcione la aplicación:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Esto descargará e instalará los programas adicionales (Streamlit, ReportLab, etc.).

---

## 6. Preparar carpetas de datos

Crea las carpetas necesarias para datos y copias de seguridad:

- En **macOS**:
  ```bash
  mkdir -p data/backups logs
  ```

- En **Windows**:
  ```powershell
  mkdir data\backups
  mkdir logs
  ```

👉 **Nota importante**:   
La aplicación creará automáticamente la base de datos `data/libro_socios.db` con toda la estructura necesaria en el primer arranque.

---

## 7. Abrir la aplicación

Con todo instalado, ejecuta:

```bash
streamlit run app/streamlit_app.py
```

- Se abrirá automáticamente una pestaña en tu navegador (normalmente en [http://localhost:8501](http://localhost:8501)).  
- Ahí verás la aplicación funcionando.  

---

## 8. Notas útiles

- La base de datos se guarda en `data/libro_socios.db` (se crea sola al iniciar la app si no existe).  
- Los **logs** de errores o mensajes están en `logs/app.log`.  
- Las copias de seguridad se guardan en `data/backups/`.  
- Si tienes algún error, revisa primero que Python y Git estén bien instalados.  

---

## 9. Desinstalación

- Para eliminar la aplicación, basta con borrar la carpeta `libro-socios-v2`.  
- Si quieres liberar espacio, también puedes borrar la carpeta `.venv`, `data/` y `logs/`.  
