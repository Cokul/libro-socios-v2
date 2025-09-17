# app/core/services/companies_service.py
from __future__ import annotations
from typing import Optional
import logging
from ..repositories import companies_repo
from ..validators import normalize_nif_cif

log = logging.getLogger(__name__)

def list_companies() -> list[dict]:
    rows = companies_repo.list_companies()
    # Normaliza CIF/NIF por consistencia visual
    for r in rows:
        r["cif"] = normalize_nif_cif(r.get("cif"))
    return rows

def get_company(company_id: int) -> dict | None:
    row = companies_repo.get_company(company_id)
    if row:
        row["cif"] = normalize_nif_cif(row.get("cif"))
    return row

def save_company(
    *,
    id: Optional[int],
    name: str,
    cif: str,
    domicilio: Optional[str],
    fecha_constitucion: Optional[str],
    valor_nominal: float,
    participaciones_totales: int,
) -> int:
    """
    Crea o actualiza una sociedad. Devuelve el ID.
    - fecha_constitucion en ISO 'YYYY-MM-DD' o None.
    - valor_nominal > 0 y participaciones_totales >= 1.
    """
    cif_norm = normalize_nif_cif(cif)

    if not name or not cif_norm:
        raise ValueError("Nombre y CIF/NIF son obligatorios.")
    try:
        vn = float(valor_nominal)
    except Exception:
        raise ValueError("Valor nominal debe ser numérico.")
    if vn <= 0:
        raise ValueError("Valor nominal debe ser mayor que cero.")
    try:
        pt = int(participaciones_totales)
    except Exception:
        raise ValueError("Participaciones totales debe ser entero.")
    if pt < 1:
        raise ValueError("Participaciones totales debe ser >= 1.")

    if id:
        companies_repo.update_company(
            id=id,
            name=name,
            cif=cif_norm,
            domicilio=(domicilio or None),
            fecha_constitucion=(fecha_constitucion or None),
            valor_nominal=vn,
            participaciones_totales=pt,
        )
        log.info("Company updated id=%s name='%s' cif='%s' vnom=%.4f ptot=%d", id, name, cif_norm, vn, pt)
        return id
    else:
        new_id = companies_repo.insert_company(
            name=name,
            cif=cif_norm,
            domicilio=(domicilio or None),
            fecha_constitucion=(fecha_constitucion or None),
            valor_nominal=vn,
            participaciones_totales=pt,
        )
        log.info("Company created id=%s name='%s' cif='%s' vnom=%.4f ptot=%d", new_id, name, cif_norm, vn, pt)
        return new_id

def delete_company(company_id: int) -> None:
    companies_repo.delete_company(company_id)
    log.warning("Company deleted id=%s", company_id)