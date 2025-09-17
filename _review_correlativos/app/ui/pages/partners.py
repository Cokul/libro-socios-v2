# app/ui/partners.py
from __future__ import annotations
import streamlit as st
import pandas as pd
import logging
from datetime import date, datetime

from app.core.services.partners_service import list_partners, save_partner
from app.core.repositories.partners_repo import get_partner  # para cargar por ID
from app.core.services.reporting_service import active_encumbrances_affecting_partner as enc_aff
from app.infra.db import get_connection                     # delete simple

log = logging.getLogger(__name__)

MIN_PERSON_DATE = date(1900, 1, 1)
MAX_PERSON_DATE = date.today()

# ---------------- helpers ----------------
def _to_date_or_none(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _delete_partner(company_id: int, partner_id: int) -> None:
    """Elimina un socio por id/compañía. (Podemos moverlo a partners_service más adelante.)"""
    with get_connection() as conn:
        conn.execute("DELETE FROM partners WHERE id=? AND company_id=?", (partner_id, company_id))

def _reset_form_state():
    # Valores por defecto del formulario
    st.session_state["pa_id"] = 0
    st.session_state["pa_nombre"] = ""
    st.session_state["pa_nif"] = ""
    st.session_state["pa_dom"] = ""
    st.session_state["pa_nac"] = ""
    st.session_state["pa_fecha"] = None

# ---------------- UI ---------------------
def render(company_id: int):
    st.subheader("Socios")

    # ---- estado inicial y “pendientes” (ANTES de dibujar widgets) ----
    if "pa_id" not in st.session_state:
        _reset_form_state()
    if "pa_id_pending" not in st.session_state:
        st.session_state["pa_id_pending"] = None
    if "pa_form_reset" not in st.session_state:
        st.session_state["pa_form_reset"] = False

    # aplica pa_id_pending en el arranque del render y límpialo
    if st.session_state["pa_id_pending"] is not None:
        st.session_state["pa_id"] = int(st.session_state["pa_id_pending"])
        st.session_state["pa_id_pending"] = None

    # reset solicitado en el ciclo anterior
    if st.session_state.get("pa_form_reset", False):
        _reset_form_state()
        st.session_state["pa_form_reset"] = False

    # Listado
    data = list_partners(company_id)
    df = pd.DataFrame(data) if data else pd.DataFrame(columns=[
        "id","company_id","nombre","nif","domicilio","nacionalidad","fecha_nacimiento_constitucion"
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("---")
    with st.expander("➕ Alta / edición de socio", expanded=True):
        # Fila ID + cargar
        col_id, col_btn = st.columns([1,1])
        with col_id:
            st.number_input("ID (0 para alta)", min_value=0, step=1, key="pa_id")
        with col_btn:
            if st.button("🔎 Cargar datos"):
                pid = int(st.session_state.get("pa_id") or 0)
                if pid > 0:
                    row = get_partner(company_id, pid)
                    if row:
                        st.session_state["pa_nombre"] = row.get("nombre") or ""
                        st.session_state["pa_nif"]    = row.get("nif") or ""
                        st.session_state["pa_dom"]    = row.get("domicilio") or ""
                        st.session_state["pa_nac"]    = row.get("nacionalidad") or ""
                        st.session_state["pa_fecha"]  = _to_date_or_none(row.get("fecha_nacimiento_constitucion"))
                        st.success(f"Cargado socio ID {pid}.")
                    else:
                        st.warning(f"No se encontró el socio ID {pid}.")
                else:
                    st.info("Introduce un ID > 0 para cargar.")
                st.rerun()

        # Campos (usan session_state como única fuente de verdad)
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Nombre", key="pa_nombre")
            st.text_input("NIF/NIE/CIF", key="pa_nif")
            st.text_input("Domicilio", key="pa_dom")
        with col2:
            st.text_input("Nacionalidad", key="pa_nac")
            st.date_input(
                "Fecha nacimiento / constitución",
                min_value=MIN_PERSON_DATE, max_value=MAX_PERSON_DATE,
                format="YYYY-MM-DD", key="pa_fecha"
            )

        # Botonera
        bA, bB, bC = st.columns([1,1,1])
        with bA:
            if st.button("💾 Guardar socio"):
                fid = int(st.session_state.get("pa_id") or 0) or None
                fecha_iso = st.session_state["pa_fecha"].isoformat() if st.session_state.get("pa_fecha") else None
                new_id = save_partner(
                    id=fid,
                    company_id=company_id,
                    nombre=st.session_state.get("pa_nombre","").strip(),
                    nif=st.session_state.get("pa_nif","").strip(),
                    domicilio=(st.session_state.get("pa_dom","").strip() or None),
                    nacionalidad=(st.session_state.get("pa_nac","").strip() or None),
                    fecha_nacimiento_constitucion=fecha_iso
                )
                log.info("Partner saved id=%s company_id=%s", new_id, company_id)
                st.success(f"Guardado socio ID {new_id}")
                # ⚠️ no tocar pa_id directamente; usa pending + rerun
                st.session_state["pa_id_pending"] = int(new_id)
                st.rerun()

        with bB:
            if st.button("🗑️ Eliminar socio", disabled=(int(st.session_state.get("pa_id") or 0) == 0)):
                pid = int(st.session_state["pa_id"])
                try:
                    _delete_partner(company_id, pid)
                    log.warning("Partner deleted id=%s company_id=%s", pid, company_id)
                    st.success(f"Socio {pid} eliminado.")
                    st.session_state["pa_form_reset"] = True
                except Exception as e:
                    st.error(f"No se pudo eliminar: {e}")
                st.rerun()

        with bC:
            if st.button("🧹 Limpiar formulario"):
                st.session_state["pa_form_reset"] = True
                st.rerun()
