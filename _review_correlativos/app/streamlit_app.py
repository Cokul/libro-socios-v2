#app/streamlit_app.py

# --- bootstrap de rutas: asegura que la raíz del proyecto está en sys.path ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]  # .../libro_socios_v2
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# -----------------------------------------------------------------------------

import streamlit as st
from app.infra.logging import setup_logging
from app.ui.layout import sidebar_selector, sidebar_menu
from app.ui.routing import render_page

# Inicializar logging una sola vez
if "logging_setup" not in st.session_state:
    logger = setup_logging()
    st.session_state["logging_setup"] = True
else:
    import logging
    logger = logging.getLogger()

st.set_page_config(page_title="📘 Libro Registro de Socios – v2", layout="wide")
with st.sidebar:
    sidebar_selector()
    section = sidebar_menu()
render_page(section, st.session_state.get("company_id"))