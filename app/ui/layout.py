#app/ui/layout.py

import streamlit as st
from app.core.services.companies_service import list_companies
def sidebar_selector():
    companies = list_companies()
    options = ["(elige)"] + [f"{c['id']} – {c['name']} – {c['cif']}" for c in companies]
    sel = st.selectbox("Sociedad", options, key="company_selector")
    st.session_state.company_id = int(sel.split(" – ")[0]) if sel != "(elige)" else None
def sidebar_menu():
    return st.sidebar.radio(
        "Secciones",
        ["Overview","Sociedades","Gobernanza","Socios","Eventos","Reports","Utilidades"],
        key="section_selector"
    )