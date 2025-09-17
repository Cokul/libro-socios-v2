# app/ui/pages/governance.py

from __future__ import annotations

import streamlit as st
import pandas as pd

from app.core.services.governance_service import (
    get_governance,
    save_board_member,
    migrate_firmantes_to_board,
    recompute_board_numbers,
)
from app.core.enums import GOVERNANCE_ROLES


def _reset_form_state():
    st.session_state["gov_member_id"] = 0
    st.session_state["gov_nombre"] = ""
    st.session_state["gov_cargo_sel"] = GOVERNANCE_ROLES[0] if GOVERNANCE_ROLES else "Administrador Único"
    st.session_state["gov_cargo_custom"] = ""
    st.session_state["gov_nif"] = ""
    st.session_state["gov_direccion"] = ""
    st.session_state["gov_telefono"] = ""
    st.session_state["gov_email"] = ""


def render(company_id: int):
    st.subheader("Gobernanza (Consejo)")

    # Acción rápida de recompute para esta sociedad
    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("↻ Recomputar correlativo (consejo)"):
            n = recompute_board_numbers(company_id)
            st.success(f"Recomputado board_no para {n} filas.")
            st.rerun()

    data = get_governance(company_id)
    organo = data.get("organo")
    board = data.get("board", [])
    source = data.get("source")

    if organo:
        st.markdown(f"**Órgano de administración:** `{organo}`")

    st.caption(f"Fuente de datos: `{source}`")

    # DataFrame del consejo (con board_no si existe)
    df = pd.DataFrame(board)
    if "board_no" in df.columns:
        df = df.rename(columns={"board_no": "Nº consejero"})
        # Orden visual por correlativo si existe
        df = df.sort_values(by=["Nº consejero", "nombre"], na_position="last")
        cols = ["Nº consejero", "id", "company_id", "nombre", "cargo", "nif", "direccion", "telefono", "email"]
        df = df[[c for c in cols if c in df.columns]]
    st.dataframe(df, width="stretch", hide_index=True)

    if source == "firmantes_json" and len(board) > 0:
        if st.button("Migrar firmantes a tabla 'board_members'"):
            inserted = migrate_firmantes_to_board(company_id)
            st.success(f"Migrados {inserted} registros a board_members.")
            st.rerun()

    st.markdown("---")

    # ---- Formulario de alta/edición ----
    with st.expander("➕ Alta / edición de consejero", expanded=True):
        if "gov_member_id" not in st.session_state:
            _reset_form_state()

        col1, col2 = st.columns(2)
        with col1:
            member_id = st.number_input("ID (0 para alta)", min_value=0, step=1, value=int(st.session_state.get("gov_member_id", 0)), key="gov_member_id")
            nombre = st.text_input("Nombre", value=st.session_state.get("gov_nombre", ""), key="gov_nombre")

            opciones = GOVERNANCE_ROLES + ["Otro…"]
            cargo_sel = st.selectbox(
                "Cargo / Rol",
                opciones,
                index=opciones.index(st.session_state.get("gov_cargo_sel", opciones[0])) if st.session_state.get("gov_cargo_sel") in opciones else 0,
                key="gov_cargo_sel"
            )
            cargo_custom = ""
            if cargo_sel == "Otro…":
                cargo_custom = st.text_input("Especifica el rol", value=st.session_state.get("gov_cargo_custom", ""), placeholder="p. ej. Vocal", key="gov_cargo_custom")

            nif = st.text_input("NIF/NIE", value=st.session_state.get("gov_nif", ""), key="gov_nif")

        with col2:
            direccion = st.text_input("Dirección", value=st.session_state.get("gov_direccion", ""), key="gov_direccion")
            telefono = st.text_input("Teléfono", value=st.session_state.get("gov_telefono", ""), key="gov_telefono")
            email = st.text_input("Email", value=st.session_state.get("gov_email", ""), key="gov_email")

        cargo_final = (cargo_custom.strip() if cargo_sel == "Otro…" else cargo_sel).strip()

        b1, b2 = st.columns(2)
        with b1:
            if st.button("💾 Guardar consejero", use_container_width=True):
                try:
                    fid = int(member_id) if member_id else None
                    new_id = save_board_member(
                        id=fid,
                        company_id=company_id,
                        nombre=nombre.strip(),
                        cargo=cargo_final,
                        nif=nif.strip(),
                        direccion=(direccion or None),
                        telefono=(telefono or None),
                        email=(email or None),
                    )
                    st.success(f"Guardado consejero ID {new_id}")
                    _reset_form_state()
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        with b2:
            if st.button("🧹 Limpiar formulario", use_container_width=True):
                _reset_form_state()
                st.rerun()