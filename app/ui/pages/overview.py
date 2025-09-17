# app/ui/pages/overview.py

from __future__ import annotations
import logging
from datetime import date
import math
import streamlit as st

from app.core.services.reporting_service import kpis as kpis_service

log = logging.getLogger(__name__)

# --------- helpers de formato (estilo ES) ---------
def _fmt_int_es(n: int | None) -> str:
    if n is None:
        return "—"
    # miles con punto
    return f"{int(n):,}".replace(",", ".")

def _fmt_eur2_es(x: float | None) -> str:
    if x is None:
        return "—"
    s = f"{x:,.2f}"
    # pasar 1,234,567.89 -> 1.234.567,89
    s = s.replace(",", "·").replace(".", ",").replace("·", ".")
    return f"{s} €"

def _fmt_eur_compacto(x: float | None) -> str:
    """Formatea € en unidades compactas: K / M / B (con coma decimal)."""
    if x is None:
        return "—"
    a = abs(x)
    if a < 1_000:
        s = f"{x:.0f}"
        return _fmt_eur2_es(float(s)).replace(",00 €", " €")  # 999 €
    units = [(1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")]
    for base, suf in units:
        if a >= base:
            val = x / base
            s = f"{val:.2f}"
            s = s.replace(".", ",")  # coma decimal
            # quitar ceros finales innecesarios
            s = s.rstrip("0").rstrip(",")
            return f"{s} {suf}€"
    return _fmt_eur2_es(x)

def render(company_id: int | None):
    st.subheader("Resumen")

    if not company_id:
        st.write("Elige una sociedad en la barra lateral para comenzar.")
        return

    # Filtro de fecha simple para los KPIs
    as_of = st.date_input("A fecha", value=date.today(), format="YYYY-MM-DD").isoformat()

    # KPIs principales
    try:
        _k = kpis_service(company_id, as_of)
    except Exception as e:
        st.error(f"No fue posible calcular los KPIs: {e}")
        return

    st.markdown("### Indicadores clave")
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Socios con saldo", str(_k.num_partners))
    c2.metric("Participaciones", _fmt_int_es(_k.total_shares))
    c3.metric("Nominal", _fmt_eur2_es(_k.share_nominal).replace(" €", ""))  # queda “1,00” limpio
    c4.metric("Capital", _fmt_eur_compacto(_k.share_capital))
    c5.metric("Último apunte", _k.last_event_date or "—")

    # Nota con el valor exacto de capital para quien lo necesite
    if _k.share_capital is not None:
        st.caption(f"Capital exacto: {_fmt_eur2_es(_k.share_capital)}")
    st.caption(f"Datos calculados a {as_of}.")