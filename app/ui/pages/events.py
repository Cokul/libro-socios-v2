# app/ui/pages/events.py
from __future__ import annotations
import streamlit as st
import pandas as pd
from datetime import date, datetime
import logging

from app.core.enums import EVENT_TYPES
from app.core.services.events_service import (
    list_events_for_ui,
    create_event_generic,
    get_event,
    update_event,
    delete_event,
    create_redenominacion,
)
from app.core.services.partners_service import list_partners

log = logging.getLogger(__name__)

MIN_EVENT_DATE = date(1900, 1, 1)
MAX_EVENT_DATE = date.today()

START_TYPES = tuple(t for t in EVENT_TYPES if t in ("PIGNORACION", "EMBARGO"))
CANCEL_TYPES = tuple(
    t for t in EVENT_TYPES
    if t in ("CANCELA_PIGNORACION", "CANCELA_EMBARGO", "LEV_GRAVAMEN", "ALZAMIENTO")
)

def _partners_maps(company_id: int):
    partners = list_partners(company_id)
    choices = [p["id"] for p in partners]
    labels = {p["id"]: f'{p["id"]} – {p["nombre"]} ({p.get("nif") or "-"})' for p in partners}
    names  = {p["id"]: p["nombre"] for p in partners}
    return choices, labels, names

def _to_date_or_none(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _reset_edit_state():
    st.session_state["ev_id"]   = 0
    st.session_state["ev_tipo"] = (EVENT_TYPES or ["OTRO"])[0]
    st.session_state["ev_fecha"] = None
    st.session_state["ev_st"] = None
    st.session_state["ev_sa"] = None
    st.session_state["ev_rd"] = 0
    st.session_state["ev_rh"] = 0
    st.session_state["ev_np"] = 0
    st.session_state["ev_nvn"] = 0.0
    st.session_state["ev_doc"] = ""
    st.session_state["ev_obs"] = ""

def render(company_id: int):
    st.subheader("Eventos")

    if st.session_state.get("ev_form_reset", False):
        _reset_edit_state()
        st.session_state["ev_form_reset"] = False
    if "ev_id" not in st.session_state:
        _reset_edit_state()

    with st.expander("🔎 Filtros"):
        colf1, colf2 = st.columns(2)
        with colf1:
            f_desde = st.date_input("Desde", value=None, format="YYYY-MM-DD",
                                    min_value=MIN_EVENT_DATE, max_value=MAX_EVENT_DATE, key="ev_filter_from")
        with colf2:
            f_hasta = st.date_input("Hasta", value=None, format="YYYY-MM-DD",
                                    min_value=MIN_EVENT_DATE, max_value=MAX_EVENT_DATE, key="ev_filter_to")

    data_ui = list_events_for_ui(company_id)
    if f_desde:
        data_ui = [e for e in data_ui if e["fecha"] and e["fecha"] >= f_desde.isoformat()]
    if f_hasta:
        data_ui = [e for e in data_ui if e["fecha"] and e["fecha"] <= f_hasta.isoformat()]

    cols_view = [
        "id","correlativo","fecha","tipo",
        "socio_transmite","socio_adquiere",
        "rango_desde","rango_hasta",
        "n_participaciones","nuevo_valor_nominal",
        "documento","observaciones",
    ]
    df_view = pd.DataFrame(data_ui)
    for c in cols_view:
        if c not in df_view.columns:
            df_view[c] = None
    st.dataframe(df_view[cols_view], width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader("➕ Alta de evento")

    tipo_opts = list(dict.fromkeys((EVENT_TYPES or []) + ["OTRO"]))
    tipo = st.selectbox("Tipo de evento", tipo_opts, index=0, key="ev_new_tipo")

    fecha = st.date_input(
        "Fecha del evento",
        value=date.today(), format="YYYY-MM-DD",
        min_value=MIN_EVENT_DATE, max_value=MAX_EVENT_DATE,
        key="ev_new_fecha"
    ).isoformat()
    observaciones = st.text_input("Observaciones (opcional)", value="", key="ev_new_obs")
    choices, labels, _names = _partners_maps(company_id)

    def _soc_select(label: str, default=None, key:str=""):
        opts = [None] + choices
        idx = 0
        if default in choices:
            idx = opts.index(default)
        return st.selectbox(
            label, opts, index=idx,
            format_func=lambda v: "—" if v is None else labels.get(v, str(v)),
            key=key or f"ev_new_sel_{label.replace(' ', '_')}"
        )

    socio_transmite = None
    socio_adquiere = None
    rango_desde = None
    rango_hasta = None
    n_participaciones = None
    nuevo_valor_nominal = None
    documento = st.text_input("Documento (opcional)", value="", key="ev_new_doc")

    # -------- Tipos habituales --------
    if tipo in ("TRANSMISION", "SUCESION"):
        st.info("Mueve un rango de participaciones de un socio a otro.")
        socio_transmite = _soc_select("Socio transmite (origen)", key="ev_new_tran_st")
        socio_adquiere  = _soc_select("Socio adquiere (destino)", key="ev_new_tran_sa")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="ev_new_tran_rd")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="ev_new_tran_rh")
        if st.button("Guardar evento", key="ev_new_tran_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrada (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo in ("ALTA", "AMPL_EMISION"):
        st.info("Añade participaciones nuevas a un socio (por rangos).")
        socio_adquiere = _soc_select("Socio adquiere", key="ev_new_alta_sa")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="alta_desde")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="alta_hasta")
        if st.button("Guardar alta/ampliación", key="ev_new_alta_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=None,
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrada (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo in ("BAJA", "RED_AMORT"):
        st.info("Reduce participaciones de un socio (por rangos).")
        socio_transmite = _soc_select("Socio transmite (titular)", key="ev_new_baja_st")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="baja_desde")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="baja_hasta")
        if st.button("Guardar baja/reducción", key="ev_new_baja_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,
                    socio_adquiere=None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrada (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo in ("USUFRUCTO",):
        st.info("Desdobla plena propiedad en nuda propiedad (transmite) y usufructo (adquiere) para un rango.")
        socio_transmite = _soc_select("Socio titular (nuda)", key="ev_new_usuf_st")
        socio_adquiere  = _soc_select("Socio usufructuario", key="ev_new_usuf_sa")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="usuf_desde")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="usuf_hasta")
        if st.button("Guardar usufructo", key="ev_new_usuf_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrado (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    # -------- Gravámenes --------
    elif tipo in START_TYPES:
        st.info("Grava un rango a favor de un acreedor. El **titular** va en 'Socio transmite' y el **acreedor** en 'Socio adquiere'.")
        socio_transmite = _soc_select("Socio transmite (titular afectado)", key="ev_new_grav_st")
        socio_adquiere  = _soc_select("Acreedor / beneficiario", key="ev_new_grav_sa")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="grav_desde")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="grav_hasta")

        if st.button("Guardar gravamen", key="ev_new_grav_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrado (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo in CANCEL_TYPES:
        st.info("Cancela total o parcialmente un gravamen previo. Indica el **mismo acreedor** y el **rango** a levantar.")
        socio_transmite = _soc_select("Socio transmite (titular afectado)", key="ev_new_lev_st")
        socio_adquiere  = _soc_select("Acreedor / beneficiario", key="ev_new_lev_sa")
        c1, c2 = st.columns(2)
        with c1:
            rango_desde = st.number_input("Rango desde", min_value=1, step=1, value=1, key="lev_desde")
        with c2:
            rango_hasta = st.number_input("Rango hasta", min_value=int(rango_desde or 1), step=1, value=int(rango_desde or 1), key="lev_hasta")

        if st.button("Guardar cancelación", key="ev_new_lev_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,  # titular
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,    # acreedor
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=None, nuevo_valor_nominal=None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrado (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo in ("AMPL_VALOR", "RED_VALOR"):
        st.info("Ajusta el valor nominal (obligatorio en estos tipos).")
        nuevo_valor_nominal = st.number_input(
            "Nuevo valor nominal (€)",
            min_value=0.0, step=0.01, value=0.0, format="%.2f",
            key="ev_new_nvn_change"
        )
        if st.button("Guardar cambio de valor nominal", key="ev_new_nvn_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo=tipo, fecha=fecha,
                    socio_transmite=None, socio_adquiere=None,
                    rango_desde=None, rango_hasta=None,
                    n_participaciones=None,
                    nuevo_valor_nominal=float(nuevo_valor_nominal) if nuevo_valor_nominal else None,
                    documento=documento or None, observaciones=observaciones or None,
                )
                st.success(f"{tipo} registrado (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif tipo == "REDENOMINACION":
        st.info(
            "La REDENOMINACIÓN se aplica al cierre del día y no altera el % por socio. "
            "Puedes: (a) Global (constancia), (b) Global con recálculo del nº (VN>0 y capital múltiplo), "
            "(c) Por bloque (RD–RH) para compactar/renumerar un bloque)."
        )
        por_bloque = st.toggle("Por bloque (usar RD–RH y socio titular)", value=False, key="ev_new_reden_block")
        recalcular_num = st.toggle("Recalcular nº de participaciones (modo global)", value=False, disabled=por_bloque, key="ev_new_reden_recalc_toggle")

        choices, labels, _ = _partners_maps(company_id)
        socio_bloque = None
        rd = rh = None
        if por_bloque:
            socio_bloque = st.selectbox(
                "Socio (titular del bloque)",
                [None] + choices, index=0,
                format_func=lambda v: "—" if v is None else labels.get(v, str(v)),
                key="ev_new_reden_soc"
            )
            c1, c2 = st.columns(2)
            with c1:
                rd = st.number_input("Rango desde (RD)", min_value=1, step=1, value=1, key="reden_rd")
            with c2:
                rh = st.number_input("Rango hasta (RH)", min_value=int(rd or 1), step=1, value=int(rd or 1), key="reden_rh")

        if st.button("Guardar redenominación", key="ev_new_reden_save"):
            try:
                new_id = create_redenominacion(
                    company_id=company_id,
                    fecha=fecha,
                    por_bloque=bool(por_bloque),
                    socio_id=int(socio_bloque) if (por_bloque and socio_bloque) else None,
                    rango_desde=int(rd) if (por_bloque and rd) else None,
                    rango_hasta=int(rh) if (por_bloque and rh) else None,
                    recalcular_numero=bool(recalcular_num),
                    nuevo_valor_nominal=None,
                    documento=documento or None,
                    observaciones=observaciones or None,
                )
                st.success(f"REDENOMINACION registrada (evento ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    else:  # OTRO
        st.info("Tipo libre: puedes informar cualquiera de los campos opcionalmente.")
        c1, c2 = st.columns(2)
        with c1:
            socio_transmite = _soc_select("Socio transmite (opcional)", key="ev_new_otro_st")
            rango_desde = st.number_input("Rango desde (opcional)", min_value=0, step=1, value=0, key="otro_desde")
        with c2:
            socio_adquiere = _soc_select("Socio adquiere (opcional)", key="ev_new_otro_sa")
            rango_hasta = st.number_input("Rango hasta (opcional)", min_value=0, step=1, value=0, key="otro_hasta")
        n_participaciones = st.number_input("Nº de participaciones (opcional)", min_value=0, step=1, value=0, key="ev_new_otro_np")
        if st.button("Guardar evento genérico", key="ev_new_otro_save"):
            try:
                new_id = create_event_generic(
                    company_id=company_id, tipo="OTRO", fecha=fecha,
                    socio_transmite=int(socio_transmite) if socio_transmite else None,
                    socio_adquiere=int(socio_adquiere) if socio_adquiere else None,
                    rango_desde=int(rango_desde) if rango_desde else None,
                    rango_hasta=int(rango_hasta) if rango_hasta else None,
                    n_participaciones=int(n_participaciones) if n_participaciones else None,
                    nuevo_valor_nominal=None,
                    documento=documento or None,
                    observaciones=observaciones or None,
                )
                st.success(f"Evento registrado (ID {new_id}).")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    # -------- Edición / borrado --------
    st.markdown("---")
    with st.expander("✏️ Editar / Eliminar evento", expanded=False):
        choices_full, labels_map, _ = _partners_maps(company_id)

        def safe_index(options, value, default=0):
            try:
                return options.index(value)
            except Exception:
                return default

        col_id, col_btn = st.columns([1, 1])
        with col_id:
            st.number_input("ID evento", min_value=0, step=1, key="ev_id")
        with col_btn:
            if st.button("🔎 Cargar evento"):
                eid = int(st.session_state.get("ev_id") or 0)
                if eid > 0:
                    ev = get_event(company_id, eid)
                    if ev:
                        st.session_state["ev_tipo"]  = ev.get("tipo") or (EVENT_TYPES[0] if EVENT_TYPES else "OTRO")
                        st.session_state["ev_fecha"] = _to_date_or_none(ev.get("fecha"))
                        st.session_state["ev_st"]    = int(ev["socio_transmite"]) if ev.get("socio_transmite") is not None else None
                        st.session_state["ev_sa"]    = int(ev["socio_adquiere"]) if ev.get("socio_adquiere") is not None else None
                        st.session_state["ev_rd"]    = int(ev["rango_desde"] or 0)
                        st.session_state["ev_rh"]    = int(ev["rango_hasta"] or 0)
                        st.session_state["ev_np"]    = int(ev.get("n_participaciones") or 0)
                        st.session_state["ev_nvn"]   = float(ev.get("nuevo_valor_nominal") or 0.0)
                        st.session_state["ev_doc"]   = ev.get("documento") or ""
                        st.session_state["ev_obs"]   = ev.get("observaciones") or ""
                        st.success(f"Cargado evento ID {eid}.")
                    else:
                        st.warning(f"No se encontró el evento ID {eid}.")
                else:
                    st.info("Introduce un ID > 0 para cargar.")
                st.rerun()

        tipo_opts_full = list(dict.fromkeys((EVENT_TYPES or []) + ["OTRO"]))
        col1, col2 = st.columns(2)
        with col1:
            st.selectbox(
                "Tipo",
                tipo_opts_full,
                index=safe_index(
                    tipo_opts_full,
                    st.session_state.get("ev_tipo", "OTRO"),
                    default=safe_index(tipo_opts_full, "OTRO", 0)
                ),
                key="ev_tipo"
            )
            st.date_input(
                "Fecha",
                value=st.session_state.get("ev_fecha", None),
                min_value=MIN_EVENT_DATE, max_value=MAX_EVENT_DATE,
                format="YYYY-MM-DD",
                key="ev_fecha"
            )
            st.number_input("Rango desde", min_value=0, step=1,
                            value=int(st.session_state.get("ev_rd") or 0), key="ev_rd")
            st.number_input("Rango hasta", min_value=0, step=1,
                            value=int(st.session_state.get("ev_rh") or 0), key="ev_rh")
            st.number_input("Nº de participaciones (si aplica)", min_value=0, step=1,
                            value=int(st.session_state.get("ev_np") or 0), key="ev_np")

        with col2:
            opts_soc = [None] + choices_full
            st.selectbox(
                "Socio transmite",
                opts_soc,
                index=safe_index(opts_soc, st.session_state.get("ev_st")),
                format_func=lambda v: "—" if v is None else labels_map.get(v, str(v)),
                key="ev_st"
            )
            st.selectbox(
                "Socio adquiere",
                opts_soc,
                index=safe_index(opts_soc, st.session_state.get("ev_sa")),
                format_func=lambda v: "—" if v is None else labels_map.get(v, str(v)),
                key="ev_sa"
            )
            st.number_input(
                "Nuevo valor nominal (€)",
                min_value=0.0, step=0.01, format="%.2f",
                value=float(st.session_state.get("ev_nvn") or 0.0),
                key="ev_nvn"
            )
            st.text_input("Documento", value=st.session_state.get("ev_doc",""), key="ev_doc")
            st.text_input("Observaciones", value=st.session_state.get("ev_obs",""), key="ev_obs")

        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("💾 Guardar cambios"):
                eid = int(st.session_state.get("ev_id") or 0)
                if eid <= 0:
                    st.error("Indica un ID de evento válido.")
                else:
                    try:
                        fecha_edit = (
                            st.session_state["ev_fecha"].isoformat()
                            if st.session_state.get("ev_fecha") else date.today().isoformat()
                        )
                        update_event(
                            event_id=eid,
                            company_id=company_id,
                            tipo=st.session_state.get("ev_tipo"),
                            fecha=fecha_edit,
                            socio_transmite=st.session_state.get("ev_st"),
                            socio_adquiere=st.session_state.get("ev_sa"),
                            rango_desde=(int(st.session_state.get("ev_rd") or 0) or None),
                            rango_hasta=(int(st.session_state.get("ev_rh") or 0) or None),
                            n_participaciones=(int(st.session_state.get("ev_np") or 0) or None),
                            nuevo_valor_nominal=(float(st.session_state.get("ev_nvn") or 0.0) or None),
                            documento=(st.session_state.get("ev_doc") or None),
                            observaciones=(st.session_state.get("ev_obs") or None),
                        )
                        log.info("Event updated id=%s company_id=%s", eid, company_id)
                        st.success("Evento actualizado.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
        with b2:
            if st.button("🗑️ Eliminar evento", disabled=(int(st.session_state.get("ev_id") or 0) == 0)):
                try:
                    eid = int(st.session_state["ev_id"])
                    delete_event(event_id=eid, company_id=company_id)
                    log.warning("Event deleted id=%s company_id=%s", eid, company_id)
                    st.success("Evento eliminado.")
                    st.session_state["ev_form_reset"] = True
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with b3:
            if st.button("🧹 Limpiar formulario"):
                st.session_state["ev_form_reset"] = True
                st.rerun()