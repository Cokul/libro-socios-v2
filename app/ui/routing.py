# app/ui/routing.py
from __future__ import annotations
from typing import Optional

from .pages import overview, governance, partners, events, reports, utilities, companies

def render_page(section: str, company_id: Optional[int]):
    """
    Debe coincidir EXACTAMENTE con las opciones del sidebar:
    ["Overview","Gobernanza","Socios","Eventos","Reports","Utilidades"]
    """
    match section:
        case "Overview":
            overview.render(company_id)
        case "Sociedades":
            companies.render(company_id)
        case "Gobernanza":
            governance.render(company_id)
        case "Socios":
            partners.render(company_id)
        case "Eventos":
            events.render(company_id)
        case "Reports":
            reports.render(company_id)
        case "Utilidades":
            utilities.render(company_id)
        case _:
            # Fallback explícito por si llegase un valor no esperado
            overview.render(company_id)