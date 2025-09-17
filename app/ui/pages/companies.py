# app/ui/pages/companies.py
from __future__ import annotations
import streamlit as st
import pandas as pd
from datetime import date, datetime
import logging

from app.core.services.companies_service import (
    list_companies, get_company, save_company, delete_company
)

log = logging.getLogger(__name__)

MIN_CO_DATE = date(1900, 1, 1)   # soporta sociedades antiguas
MAX_CO_DATE = date.today()

def _to_date_or_none(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _clear_form_state():
    st.session_state.setdefault("co_id", 0)
    st.session_state["co_name"] = ""
    st.session_state["co_cif"] = ""
    st.session_state["co_dom"] = ""
    st.session_state["co_fec"] = None

def render(company_id: int | None):
    st.subheader("Sociedades")
    
    # --- resets/prefills antes de instanciar widgets ---
    if st.session_state.get("co_form_reset", False):
        for k, v in (("co_id", 0), ("co_name", ""), ("co_cif", ""), ("co_dom", ""), ("co_fec", None)):
            st.session_state[k] = v
        st.session_state["co_form_reset"] = False

    # Listado
    rows = list_companies()
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id","name","cif","domicilio","fecha_constitucion"])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("---")
    with st.expander("➕ Alta / edición de sociedad", expanded=True):
        # Estado inicial del formulario
        if "co_name" not in st.session_state:
            _clear_form_state()

        # Fila superior: ID + Cargar datos
        col_id, col_btn = st.columns([1,1])
        with col_id:
            cid_input = st.number_input(
                "ID (0 para alta)", min_value=0, step=1, key="co_id"
            )
        with col_btn:
            if st.button("🔎 Cargar datos"):
                if cid_input and cid_input > 0:
                    row = get_company(int(cid_input))
                    if row:
                        st.session_state["co_name"] = row.get("name") or ""
                        st.session_state["co_cif"]  = row.get("cif") or ""
                        st.session_state["co_dom"]  = row.get("domicilio") or ""
                        st.session_state["co_fec"]  = _to_date_or_none(row.get("fecha_constitucion"))
                        st.success(f"Cargada sociedad ID {cid_input}.")
                    else:
                        st.warning(f"No se encontró la sociedad ID {cid_input}.")
                else:
                    st.info("Introduce un ID > 0 para cargar.")
                st.rerun()

        # Campos principales (pre-rellenados desde session_state)
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Nombre", value=st.session_state.get("co_name",""), key="co_name")
            domicilio = st.text_input("Domicilio", value=st.session_state.get("co_dom",""), key="co_dom")
        with col2:
            cif = st.text_input("CIF/NIF", value=st.session_state.get("co_cif",""), key="co_cif")
            fec = st.date_input(
                "Fecha constitución",
                min_value=MIN_CO_DATE, max_value=MAX_CO_DATE,
                format="YYYY-MM-DD", key="co_fec"
            )

        # Botonera
        cA, cB, cC = st.columns([1,1,1])
        with cA:
            if st.button("💾 Guardar"):
                fec_txt = st.session_state["co_fec"].isoformat() if st.session_state.get("co_fec") else None
                new_id = save_company(
                    id=(int(st.session_state.get("co_id") or 0) or None),
                    name=st.session_state.get("co_name","").strip(),
                    cif=st.session_state.get("co_cif","").strip(),
                    domicilio=(st.session_state.get("co_dom","").strip() or None),
                    fecha_constitucion=fec_txt
                )
                log.info("UI save company id=%s", new_id)
                st.success(f"Sociedad guardada (ID {new_id}).")
                _clear_form_state()
                st.rerun()
        with cB:
            if st.button("🗑️ Eliminar", disabled=(int(st.session_state.get("co_id",0)) == 0)):
                delete_company(int(st.session_state["co_id"]))
                log.warning("UI delete company id=%s", int(st.session_state["co_id"]))
                st.success(f"Sociedad {int(st.session_state['co_id'])} eliminada.")
                _clear_form_state()
                st.rerun()
        with cC:
            if st.button("🧹 Limpiar formulario"):
                st.session_state["co_form_reset"] = True
                st.rerun()

    st.caption("Esta pantalla usa la capa de servicios/repos para leer y persistir 'companies'.")