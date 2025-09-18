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
DEFAULT_VALOR_NOMINAL = 1.00
DEFAULT_PART_TOTALES = 1

def _to_date_or_none(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _prime_defaults():
    """Solo establece valores por defecto si no existen aún (no pisa valores)."""
    st.session_state.setdefault("co_id", 0)
    st.session_state.setdefault("co_name", "")
    st.session_state.setdefault("co_cif", "")
    st.session_state.setdefault("co_dom", "")
    st.session_state.setdefault("co_fec", None)
    st.session_state.setdefault("co_vnom", DEFAULT_VALOR_NOMINAL)
    st.session_state.setdefault("co_ptot", DEFAULT_PART_TOTALES)

def _schedule_form_reset():
    """Marca reset y fuerza rerender. El borrado real ocurre al inicio del render."""
    st.session_state["co_form_reset"] = True
    st.rerun()

def _apply_reset_if_needed():
    """
    Si hay bandera de reset, elimina las claves ANTES de instanciar widgets.
    Eliminar (pop) es seguro; reasignar después de instanciar no lo es.
    """
    if st.session_state.get("co_form_reset"):
        for k in ("co_id", "co_name", "co_cif", "co_dom", "co_fec", "co_vnom", "co_ptot"):
            st.session_state.pop(k, None)
        st.session_state["co_form_reset"] = False

def render(company_id: int | None):
    st.subheader("Sociedades")

    # --- aplicar reset si está programado (antes de cualquier widget) ---
    _apply_reset_if_needed()
    # --- inyectar defaults si faltan (no pisa existentes) ---
    _prime_defaults()

    # Listado
    rows = list_companies()
    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["id","name","cif","domicilio","fecha_constitucion","valor_nominal","participaciones_totales"]
    )
    st.dataframe(df, width='stretch', hide_index=True)

    st.markdown("---")
    with st.expander("➕ Alta / edición de sociedad", expanded=True):

        # Fila superior: ID + Cargar datos
        col_id, col_btn = st.columns([1,1])
        with col_id:
            st.number_input("ID (0 para alta)", min_value=0, step=1, key="co_id")
        with col_btn:
            if st.button("🔎 Cargar datos"):
                cid_input = int(st.session_state.get("co_id") or 0)
                if cid_input > 0:
                    row = get_company(cid_input)
                    if row:
                        # Cargamos en session_state ANTES de widgets (estamos aún en el mismo render;
                        # pero estos keys todavía no han instanciado widget en este bloque, es seguro)
                        st.session_state["co_name"] = row.get("name") or ""
                        st.session_state["co_cif"]  = row.get("cif") or ""
                        st.session_state["co_dom"]  = row.get("domicilio") or ""
                        st.session_state["co_fec"]  = _to_date_or_none(row.get("fecha_constitucion"))
                        try:
                            st.session_state["co_vnom"] = float(row.get("valor_nominal")) if row.get("valor_nominal") is not None else DEFAULT_VALOR_NOMINAL
                        except Exception:
                            st.session_state["co_vnom"] = DEFAULT_VALOR_NOMINAL
                        try:
                            st.session_state["co_ptot"] = int(row.get("participaciones_totales")) if row.get("participaciones_totales") is not None else DEFAULT_PART_TOTALES
                        except Exception:
                            st.session_state["co_ptot"] = DEFAULT_PART_TOTALES
                        st.success(f"Cargada sociedad ID {cid_input}.")
                    else:
                        st.warning(f"No se encontró la sociedad ID {cid_input}.")
                else:
                    st.info("Introduce un ID > 0 para cargar.")
                st.rerun()

        # Campos principales (solo key; sin value para no colisionar con session_state)
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Nombre", key="co_name", placeholder="Nombre de la sociedad")
            st.text_input("Domicilio", key="co_dom", placeholder="Calle, número, ciudad…")
            st.number_input(
                "Valor nominal (€/participación)",
                min_value=0.0001, step=0.01, format="%.2f",
                key="co_vnom"
            )
        with col2:
            st.text_input("CIF/NIF", key="co_cif", placeholder="A12345678 / 12345678Z")
            st.date_input(
                "Fecha constitución",
                min_value=MIN_CO_DATE, max_value=MAX_CO_DATE,
                format="YYYY-MM-DD", key="co_fec"
            )
            st.number_input(
                "Participaciones totales",
                min_value=1, step=1,
                key="co_ptot"
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
                    fecha_constitucion=fec_txt,
                    valor_nominal=float(st.session_state.get("co_vnom", DEFAULT_VALOR_NOMINAL)),
                    participaciones_totales=int(st.session_state.get("co_ptot", DEFAULT_PART_TOTALES)),
                )
                log.info("UI save company id=%s", new_id)
                st.success(f"Sociedad guardada (ID {new_id}).")
                _schedule_form_reset()  # <-- marcar reset + rerun (no tocar session_state ahora)

        with cB:
            disabled_del = (int(st.session_state.get("co_id",0)) == 0)
            if st.button("🗑️ Eliminar", disabled=disabled_del):
                delete_company(int(st.session_state["co_id"]))
                log.warning("UI delete company id=%s", int(st.session_state["co_id"]))
                st.success(f"Sociedad {int(st.session_state['co_id'])} eliminada.")
                _schedule_form_reset()  # <-- marcar reset + rerun

        with cC:
            if st.button("🧹 Limpiar formulario"):
                _schedule_form_reset()  # <-- marcar reset + rerun

    st.caption("Esta pantalla usa la capa de servicios/repos para leer y persistir 'companies'.")