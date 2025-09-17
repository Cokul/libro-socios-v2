#app/core/services/partners_services.py

from typing import Optional
from ..repositories import partners_repo
from ..validators import normalize_nif_cif

def list_partners(company_id: int) -> list[dict]:
    rows = partners_repo.list_by_company(company_id)
    for r in rows:
        r["nif"] = normalize_nif_cif(r.get("nif"))
    return rows

def save_partner(*, id: Optional[int], company_id: int, nombre: str, nif: str,
                 domicilio: Optional[str], nacionalidad: Optional[str],
                 fecha_nacimiento_constitucion: Optional[str]) -> int:
    nif = normalize_nif_cif(nif)
    return partners_repo.upsert_partner(
        id=id, company_id=company_id, nombre=nombre, nif=nif,
        domicilio=domicilio, nacionalidad=nacionalidad,
        fecha_nacimiento_constitucion=fecha_nacimiento_constitucion
    )