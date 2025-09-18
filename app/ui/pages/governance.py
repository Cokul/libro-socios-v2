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


def _prime_defaults():
    """Inicializa claves si no existen (no pisa valores ya establecidos)."""
    st.session_state.setdefault("gov_member_id", 0)
    st.session_state.setdefault("gov_nombre", "")
    st.session_state.setdefault(
        "gov_cargo_sel",
        GOVERNANCE_ROLES[0] if GOVERNANCE_ROLES else "Administrador Único",
    )
    st.session_state.setdefault("gov_cargo_custom", "")
    st.session_state.setdefault("gov_nif", "")
    st.session_state.setdefault("gov_direccion", "")
    st.session_state.setdefault("gov_telefono", "")
    st.session_state.setdefault("gov_email", "")


def _schedule_form_reset():
    """Marca para resetear y fuerza rerun. El borrado real ocurre al inicio del render."""
    st.session_state["gov_form_reset"] = True
    st.rerun()


def _apply_reset_if_needed():
    """
    Si hay bandera de reset, elimina las claves ANTES de instanciar widgets.
    Eliminar (pop) es seguro; reasignar tras instanciar no lo es.
    """
    if st.session_state.get("gov_form_reset"):
        for k in (
            "gov_member_id",
            "gov_nombre",
            "gov_cargo_sel",
            "gov_cargo_custom",
            "gov_nif",
            "gov_direccion",
            "gov_telefono",
            "gov_email",
        ):
            st.session_state.pop(k, None)
        st.session_state["gov_form_reset"] = False


def render(company_id: int):
    st.subheader("Gobernanza (Consejo)")

    # === Reset seguro ANTES de cualquier widget ===
    _apply_reset_if_needed()
    _prime_defaults()

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
        cols = [
            "Nº consejero",
            "id",
            "company_id",
            "nombre",
            "cargo",
            "nif",
            "direccion",
            "telefono",
            "email",
        ]
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
        col1, col2 = st.columns(2)
        with col1:
            # Widgets SIN 'value='; usan Session State vía 'key'
            st.number_input("ID (0 para alta)", min_value=0, step=1, key="gov_member_id")
            st.text_input("Nombre", key="gov_nombre")

            opciones = GOVERNANCE_ROLES + ["Otro…"]
            st.selectbox("Cargo / Rol", opciones, key="gov_cargo_sel")
            if st.session_state.get("gov_cargo_sel") == "Otro…":
                st.text_input("Especifica el rol", placeholder="p. ej. Vocal", key="gov_cargo_custom")

            st.text_input("NIF/NIE", key="gov_nif")

        with col2:
            st.text_input("Dirección", key="gov_direccion")
            st.text_input("Teléfono", key="gov_telefono")
            st.text_input("Email", key="gov_email")

        cargo_sel = st.session_state.get("gov_cargo_sel")
        cargo_custom = st.session_state.get("gov_cargo_custom", "")
        cargo_final = (cargo_custom.strip() if cargo_sel == "Otro…" else str(cargo_sel)).strip()

        b1, b2 = st.columns(2)
        with b1:
            if st.button("💾 Guardar consejero", width='stretch'):
                try:
                    fid = int(st.session_state.get("gov_member_id") or 0) or None
                    new_id = save_board_member(
                        id=fid,
                        company_id=company_id,
                        nombre=st.session_state.get("gov_nombre", "").strip(),
                        cargo=cargo_final,
                        nif=st.session_state.get("gov_nif", "").strip(),
                        direccion=(st.session_state.get("gov_direccion") or None),
                        telefono=(st.session_state.get("gov_telefono") or None),
                        email=(st.session_state.get("gov_email") or None),
                    )
                    st.success(f"Guardado consejero ID {new_id}")
                    _schedule_form_reset()  # marcar reset + rerun (no tocar session_state ahora)
                except Exception as e:
                    st.error(str(e))

        with b2:
            if st.button("🧹 Limpiar formulario", width='stretch'):
                _schedule_form_reset()  # marcar reset + rerun