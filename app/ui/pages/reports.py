# app/ui/pages/reports.py
from __future__ import annotations
import streamlit as st
import pandas as pd
import logging
import sqlite3
import datetime as dt
from pathlib import Path

log = logging.getLogger(__name__)

from app.infra.db import get_connection
from app.core.services.reporting_service import (
    cap_table, kpis, movements, event_timeline, partner_position,
    partner_holdings_ranges, active_encumbrances_affecting_partner as active_encumbrances_aff,
    capital_timeline,
)
from app.core.services.export_service import (
    export_cap_table_excel, export_movements_excel, export_partner_certificate_pdf,
    export_ledger_pdf_legalizable, export_ledger_excel_legalizable,
)

MIN_REPORT_DATE = dt.date(1900, 1, 1)
MAX_REPORT_DATE = dt.date.today()

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _event_types() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT tipo FROM events ORDER BY tipo").fetchall()
    return [r[0] for r in rows if r and r[0]]

def _as_of_state_key() -> str:
    return "rep_global_as_of"

def _get_global_as_of_iso() -> str:
    """Obtiene 'A fecha' global desde session_state en ISO (YYYY-MM-DD)."""
    if _as_of_state_key() not in st.session_state:
        st.session_state[_as_of_state_key()] = dt.date.today()
    return st.session_state[_as_of_state_key()].isoformat()

# ------------------------------------------------------------
# Página
# ------------------------------------------------------------
def render(company_id: int | None = None):
    st.subheader("📊 Reporting")

    if not company_id:
        st.write("Elige una sociedad en la barra lateral para comenzar.")
        return

    # ---------- Filtro GLOBAL "A fecha" ----------
    st.markdown("#### Filtros generales")
    # Asegura un valor dentro de los límites
    _asof_val = st.session_state.get(_as_of_state_key(), dt.date.today())
    if _asof_val < MIN_REPORT_DATE:
        _asof_val = MIN_REPORT_DATE
    elif _asof_val > MAX_REPORT_DATE:
        _asof_val = MAX_REPORT_DATE

    st.session_state[_as_of_state_key()] = st.date_input(
        "A fecha (global)",
        value=_asof_val,
        min_value=MIN_REPORT_DATE,
        max_value=MAX_REPORT_DATE,
        format="YYYY-MM-DD",
        key="rep_global_as_of_input",
    )
    as_of_global = _get_global_as_of_iso()
    st.caption("Este valor se utiliza en Cap table/KPIs, Detalle socio, Certificaciones y Gráficas, "
               "así como en las exportaciones asociadas.")
    st.divider()

    tabs = st.tabs([
        "Cap table & KPIs",
        "Detalle socio",
        "Movimientos",
        "Certificaciones",
        "Gráficas",
    ])

    # --------------------------------------------------------
    # 1) Cap table & KPIs
    # --------------------------------------------------------
    with tabs[0]:
        _k = kpis(company_id, as_of_global)
        colk1, colk2, colk3, colk4, colk5 = st.columns(5)
        colk1.metric("Socios con saldo", f"{_k.num_partners}")
        colk2.metric("Participaciones totales", f"{_k.total_shares:,}".replace(",", "."))
        colk3.metric("Nominal", f"{_k.share_nominal:.2f}" if _k.share_nominal is not None else "—")
        colk4.metric("Capital", f"{_k.share_capital:,.2f}".replace(",", ".") if _k.share_capital is not None else "—")
        colk5.metric("Último apunte", _k.last_event_date or "—")
        st.caption(f"Datos calculados a {as_of_global}.")

        st.markdown("#### Tabla de capitalización")
        df = cap_table(company_id, as_of_global).copy()

        # Si el servicio trae partner_no, anteponerlo y ocultar partner_id visualmente
        if "partner_no" in df.columns:
            df.rename(columns={
                "partner_no": "Nº socio",
                "partner_name": "Socio",
                "nif": "NIF",
                "classes": "Clase",
                "shares": "Participaciones",
                "capital_socio": "Capital socio (€)",
                "pct": "%"
            }, inplace=True)
            cols = [c for c in ["Nº socio", "Socio", "NIF", "Clase", "Participaciones", "Capital socio (€)", "%"] if c in df.columns]
            st.dataframe(df[cols], hide_index=True, width="stretch")
        else:
            st.dataframe(
                df.rename(columns={
                    "partner_id": "partner_id",
                    "partner_name": "Socio",
                    "nif": "NIF",
                    "classes": "Clase",
                    "shares": "Participaciones",
                    "capital_socio": "Capital socio (€)",
                    "pct": "%"
                }),
                hide_index=True,
                width="stretch"
            )

        # --- Filtros para el LIBRO ---
        st.markdown("##### Libro registro – filtros")
        colf1, colf2, colf3 = st.columns([1, 1, 2])
        with colf1:
            dfrom_cap = st.date_input("Desde (libro)", value=None, format="YYYY-MM-DD", key="rep_cap_lib_from")
        with colf2:
            dto_cap = st.date_input("Hasta (libro)", value=None, format="YYYY-MM-DD", key="rep_cap_lib_to")
        with colf3:
            ev_opts_cap = _event_types()
            selected_types_cap = st.multiselect("Tipos de evento (libro)", ev_opts_cap, default=[], key="rep_cap_lib_types")

        date_from = dfrom_cap.isoformat() if dfrom_cap else None
        date_to   = dto_cap.isoformat() if dto_cap else None

        # --- Exportaciones ---
        st.markdown("##### Exportaciones")
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("⤵️ Exportar Cap Table (Excel)", key="rep_cap_export_xlsx"):
                xls = export_cap_table_excel(company_id, as_of=as_of_global)
                st.download_button(
                    label="Descargar CapTable.xlsx",
                    data=xls.getvalue(),
                    file_name=f"cap_table_{company_id}_{as_of_global}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width="stretch"
                )
        with c2:
            if st.button("⤵️ Libro (PDF legalizable)", key="rep_cap_export_pdf_ledger"):
                pdf = export_ledger_pdf_legalizable(
                    company_id,
                    date_from,
                    date_to,
                    selected_types_cap or None,
                    as_of=as_of_global,                 # 👈 SIEMPRE explícito
                    diligencia_apertura=None,
                    diligencia_cierre=None,
                )
                st.download_button(
                    "Descargar LibroRegistro.pdf",
                    data=pdf.getvalue(),
                    file_name=f"libro_registro_{company_id}.pdf",
                    mime="application/pdf",
                    width="stretch"
                )
        with c3:
            if st.button("⤵️ Libro (Excel legalizable)", key="rep_cap_export_xlsx_ledger"):
                xls_leg = export_ledger_excel_legalizable(
                    company_id,
                    date_from,
                    date_to,
                    selected_types_cap or None,
                    diligencia_apertura=None,
                    diligencia_cierre=None,
                    as_of=as_of_global,
                )
                st.download_button(
                    "Descargar LibroRegistro.xlsx",
                    data=xls_leg.getvalue(),
                    file_name=f"libro_registro_{company_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width="stretch"
                )

    # --------------------------------------------------------
    # 2) Detalle socio
    # --------------------------------------------------------
    with tabs[1]:
        st.markdown(f"#### Detalle de un socio a fecha {as_of_global}")

        # Traer partner_no si existe (NULLS LAST)
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            have = {r["name"] for r in conn.execute("PRAGMA table_info(partners)")}
            if "partner_no" in have:
                dfp = pd.read_sql_query(
                    """SELECT id, nombre, nif, partner_no
                       FROM partners
                       WHERE company_id = ?
                       ORDER BY CASE WHEN partner_no IS NULL THEN 1 ELSE 0 END, partner_no, nombre""",
                    conn, params=(company_id,)
                )
                labels = [
                    f"{int(r['id'])} – Nº {int(r['partner_no']) if pd.notna(r['partner_no']) else '—'} – {r['nombre']} ({r['nif'] or ''})"
                    for _, r in dfp.iterrows()
                ]
            else:
                dfp = pd.read_sql_query(
                    "SELECT id, nombre, nif FROM partners WHERE company_id = ? ORDER BY nombre",
                    conn, params=(company_id,)
                )
                labels = [f"{int(r['id'])} – {r['nombre']} ({r['nif'] or ''})" for _, r in dfp.iterrows()]

        if dfp.empty:
            st.info("No hay socios en esta sociedad.")
        else:
            pick = st.selectbox("Socio", labels, index=0, key="rep_det_partner_pick")
            partner_id = int(pick.split("–", 1)[0].strip())

            # Resumen del socio
            pos = partner_position(company_id, partner_id, as_of_global)
            st.subheader("Resumen")
            cA, cB, cC = st.columns(3)
            cA.metric("Participaciones", f"{pos.get('shares',0):,}".replace(",", "."))
            cB.metric("%", f"{pos.get('pct',0.0):.4f} %")
            cC.metric("NIF", pos.get("nif") or "—")

            # Bloques de participaciones
            st.markdown("##### Bloques de participaciones")
            rangos = partner_holdings_ranges(company_id, partner_id, as_of_global)
            if rangos.empty:
                st.info("Sin bloques vigentes.")
            else:
                st.dataframe(
                    rangos.rename(columns={
                        "rango_desde": "Desde",
                        "rango_hasta": "Hasta",
                        "participaciones": "Participaciones"
                    }),
                    hide_index=True,
                    width="stretch"
                )
                total_bloques = int(rangos["participaciones"].sum())
                st.caption(f"Suma de bloques: {total_bloques} • Total socio: {int(pos.get('shares',0))}")

            # Gravámenes
            st.markdown("### Gravámenes a la fecha (pignoraciones/embargos)")
            enc = active_encumbrances_aff(company_id, partner_id, as_of_global)

            if enc is None or enc.empty:
                enc_view = pd.DataFrame(columns=["Fecha","Tipo","A favor de","Desde","Hasta"])
            else:
                enc = enc.copy()
                enc["acreedor_nombre"] = enc.get("acreedor_nombre").fillna("").astype(str).str.strip()
                enc["acreedor_nif"]    = enc.get("acreedor_nif").fillna("").astype(str).str.strip()

                def _compose_name_nif(row):
                    nom = row["acreedor_nombre"]
                    nif = row["acreedor_nif"]
                    return f"{nom} ({nif})" if nif else nom

                enc["A favor de"] = enc.apply(_compose_name_nif, axis=1)

                enc_view = enc.rename(columns={
                    "fecha": "Fecha",
                    "tipo": "Tipo",
                    "rango_desde": "Desde",
                    "rango_hasta": "Hasta",
                })[["Fecha", "Tipo", "A favor de", "Desde", "Hasta"]]

                enc_view["Desde"] = pd.to_numeric(enc_view["Desde"], errors="coerce")
                enc_view["Hasta"] = pd.to_numeric(enc_view["Hasta"], errors="coerce")
                enc_view = enc_view.sort_values(by=["Fecha", "Desde", "Hasta"], na_position="last").reset_index(drop=True)

            st.dataframe(enc_view, width="stretch", hide_index=True)

        # --------------------------------------------------------
        # 3) Movimientos
        # --------------------------------------------------------
        with tabs[2]:
            st.markdown("#### Filtros")
            colf1, colf2, colf3 = st.columns([1, 1, 2])

            with colf1:
                dfrom = st.date_input("Desde", value=None, format="YYYY-MM-DD", key="rep_mov_from")
            with colf2:
                dto = st.date_input("Hasta", value=None, format="YYYY-MM-DD", key="rep_mov_to")
            with colf3:
                ev_opts = _event_types()
                selected_types = st.multiselect("Tipos de evento", ev_opts, default=[], key="rep_mov_types")

            date_from = dfrom.isoformat() if dfrom else None
            date_to = dto.isoformat() if dto else None

            dfm = movements(company_id, date_from, date_to, selected_types).copy()

            # --- Normalización nombres/orden de columnas para la vista ---
            # Renombrar correlativo si existe
            if "correlativo" in dfm.columns:
                dfm.rename(columns={"correlativo": "Nº asiento"}, inplace=True)

            # Ocultamos columnas internas
            ocultas = {"id", "company_id", "correlativo"}

            # Orden recomendado
            prefer = [
                "Nº asiento", "fecha", "tipo",
                "socio_transmite", "socio_adquiere",
                "rango_desde", "rango_hasta",
                "n_participaciones", "nuevo_valor_nominal",
                "documento", "observaciones",
                "hora", "orden_del_dia",
                "created_at", "updated_at",
            ]

            # Construye la lista final sin duplicados
            cols = []
            seen = set()
            for c in prefer + [c for c in dfm.columns if c not in prefer]:
                if c in ocultas:
                    continue
                if c not in dfm.columns:
                    continue
                if c not in seen:
                    cols.append(c)
                    seen.add(c)

            st.dataframe(dfm[cols], hide_index=True, width="stretch")

            c1, _ = st.columns(2)
            with c1:
                if st.button("⤵️ Exportar movimientos (Excel)", key="rep_mov_export_xlsx"):
                    xls = export_movements_excel(company_id, date_from, date_to, selected_types or None)
                    st.download_button(
                        label="Descargar Movimientos.xlsx",
                        data=xls.getvalue(),
                        file_name=f"movimientos_{company_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width="stretch"
                    )

    # --------------------------------------------------------
    # 4) Certificaciones
    # --------------------------------------------------------
    with tabs[3]:
        with st.expander(f"📜 Certificado de titularidad (a {as_of_global})", expanded=False):
            #st.markdown(f"#### Certificación de titularidad (a {as_of_global})")

            # Selector con Nº socio
            with get_connection() as conn:
                conn.row_factory = sqlite3.Row
                have = {r["name"] for r in conn.execute("PRAGMA table_info(partners)")}
                if "partner_no" in have:
                    dfp = pd.read_sql_query(
                        """SELECT id, nombre, nif, partner_no
                        FROM partners
                        WHERE company_id = ?
                        ORDER BY CASE WHEN partner_no IS NULL THEN 1 ELSE 0 END, partner_no, nombre""",
                        conn, params=(company_id,)
                    )
                    labels = [
                        f"{int(r['id'])} – Nº {int(r['partner_no']) if pd.notna(r['partner_no']) else '—'} – {r['nombre']} ({r['nif'] or ''})"
                        for _, r in dfp.iterrows()
                    ]
                else:
                    dfp = pd.read_sql_query(
                        "SELECT id, nombre, nif FROM partners WHERE company_id = ? ORDER BY nombre",
                        conn, params=(company_id,)
                    )
                    labels = [f"{int(r['id'])} – {r['nombre']} ({r['nif'] or ''})" for _, r in dfp.iterrows()]

            if dfp.empty:
                st.info("No hay socios en esta sociedad.")
            else:
                selection = st.selectbox("Socio", labels, index=0, key="rep_cert_partner_pick")
                partner_id = int(selection.split("–", 1)[0].strip())

                pos = partner_position(company_id, partner_id, as_of_global)

                cA, cB, cC, cD = st.columns(4)
                cA.metric("Participaciones", f"{pos.get('shares',0):,}".replace(",", "."))
                cB.metric("Porcentaje", f"{pos.get('pct',0.0):.4f} %")
                cC.metric("Clase", pos.get("classes") or "—")
                cD.metric("NIF", pos.get("nif") or "—")

                if st.button("⤵️ Exportar certificado (PDF)", key="rep_cert_export_pdf"):
                    pdf = export_partner_certificate_pdf(company_id, partner_id, as_of=as_of_global)
                    st.download_button(
                        label="Descargar Certificado.pdf",
                        data=pdf.getvalue(),
                        file_name=f"certificado_partner_{partner_id}_{as_of_global}.pdf",
                        mime="application/pdf",
                        width="stretch"
                    )

        # === Certificado histórico (trayectoria socio en PDF) ===
        with st.expander("📜 Certificado histórico (trayectoria del socio)", expanded=False):
            from app.core.services.partners_service import list_partners
            from app.core.services.export_service import export_partner_history_pdf

            partners = list_partners(company_id)
            opts = [(p["id"], f'{p["id"]} – {p["nombre"]} ({p.get("nif") or "-"})') for p in partners]
            if not opts:
                st.info("No hay socios en esta sociedad.")
            else:
                sel = st.selectbox("Socio", opts, index=0, format_func=lambda t: t[1])  # (id, label)
                pid = sel[0]

                c1, c2 = st.columns(2)
                with c1:
                    d_from = st.date_input("Desde (opcional)", value=None, format="YYYY-MM-DD", key="hist_from")
                with c2:
                    d_to = st.date_input("Hasta (opcional)", value=None, format="YYYY-MM-DD", key="hist_to")

                if st.button("🖨️ Generar PDF", key="btn_hist_pdf", width="stretch"):
                    try:
                        pdf = export_partner_history_pdf(
                            company_id=company_id,
                            partner_id=int(pid),
                            date_from=(d_from.isoformat() if d_from else None),
                            date_to=(d_to.isoformat() if d_to else None),
                        )
                        st.success("Certificado generado.")
                        st.download_button(
                            "⬇️ Descargar certificado",
                            data=pdf.getvalue(),
                            file_name=f"certificado_historico_partner_{pid}.pdf",
                            mime="application/pdf",
                            width="stretch"
                        )
                    except Exception as e:
                        st.error(f"Error generando el PDF: {e}")

    # --------------------------------------------------------
    # 5) Gráficas
    # --------------------------------------------------------
    with tabs[4]:
        st.markdown(f"#### Evolución de participaciones y capital social (hasta {as_of_global})")

        tl = event_timeline(company_id, as_of_global)
        if tl.empty:
            st.info("No hay datos para graficar participación acumulada.")
        else:
            st.subheader("Participaciones acumuladas")
            tl_plot = tl.copy()
            tl_plot["date"] = pd.to_datetime(tl_plot["date"])
            # Built-in chart: no pasar width ni use_container_width (Altair valida width numérico)
            st.line_chart(tl_plot.set_index("date")["total_shares_acum"])

        st.divider()

        cl = capital_timeline(company_id, as_of_global)
        if cl.empty:
            st.info("No hay datos para graficar capital social.")
        else:
            st.subheader("Capital social (€)")
            cl_plot = cl.copy()
            cl_plot["date"] = pd.to_datetime(cl_plot["date"])
            # Built-in chart: no pasar width ni use_container_width
            st.line_chart(cl_plot.set_index("date")["capital_social"])

# Hook para routing
def main():
    render()

if __name__ == "__main__":
    render()