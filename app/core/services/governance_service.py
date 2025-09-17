# app/core/services/governance_service.py

from __future__ import annotations

import json
from typing import Optional

from ..repositories import governance_repo
from ..validators import normalize_nif_cif, normalize_phone, validate_email
from ..enums import GOVERNANCE_ROLES, GOVERNANCE_ROLE_ALIASES


# Si en algún momento quieres permitir varios Presidentes, cambia a False.
ENFORCE_UNIQUE_PRESIDENT = True


def _normalize_role(value: str | None) -> str | None:
    if not value:
        return value
    v = value.strip()
    # 1) si ya es exactamente uno de los canónicos, devuelve tal cual
    if v in GOVERNANCE_ROLES:
        return v
    # 2) prueba con alias (lower y sin espacios/guiones bajos)
    key = v.lower().replace(" ", "_").replace("-", "_")
    mapped = GOVERNANCE_ROLE_ALIASES.get(key)
    return mapped or v  # si no está, deja lo que vino (mejor que perder el dato)


def list_board(company_id: int) -> list[dict]:
    rows = governance_repo.list_board(company_id)
    for r in rows:
        r["nif"] = normalize_nif_cif(r.get("nif"))
        r["telefono"] = normalize_phone(r.get("telefono"))
        r["cargo"] = _normalize_role(r.get("cargo"))
    return rows


def get_governance(company_id: int) -> dict:
    """
    Devuelve:
      - organo: str | None
      - board: lista de consejeros (prioriza board_members; si está vacío, usa firmantes_json)
      - source: 'board_members' | 'firmantes_json'
    """
    board = list_board(company_id)
    if board:
        meta = governance_repo.get_company_governance(company_id) or {}
        return {"organo": meta.get("organo"), "board": board, "source": "board_members"}

    # Fallback firmantes_json
    meta = governance_repo.get_company_governance(company_id) or {}
    organo = meta.get("organo")
    raw = meta.get("firmantes_json") or "[]"
    try:
        items = json.loads(raw)
    except Exception:
        items = []

    parsed = []
    for it in items:
        nombre = (it.get("nombre") or "").strip()
        rol = _normalize_role(it.get("rol"))
        if nombre:
            parsed.append({
                "id": None,
                "company_id": company_id,
                "nombre": nombre,
                "cargo": rol or "Firmante",
                "nif": None,
                "direccion": None,
                "telefono": None,
                "email": None,
            })
    return {"organo": organo, "board": parsed, "source": "firmantes_json"}


def migrate_firmantes_to_board(company_id: int) -> int:
    current = governance_repo.list_board(company_id)
    if current:
        return 0
    meta = governance_repo.get_company_governance(company_id) or {}
    raw = meta.get("firmantes_json") or "[]"
    try:
        items = json.loads(raw)
    except Exception:
        items = []
    count = 0
    for it in items:
        nombre = (it.get("nombre") or "").strip()
        cargo = _normalize_role(it.get("rol")) or "Firmante"
        if not nombre:
            continue
        governance_repo.upsert_board_member(
            id=None, company_id=company_id, nombre=nombre, cargo=cargo, nif="",
            direccion=None, telefono=None, email=None
        )
        count += 1
    return count


# ============================
# Validaciones reforzadas
# ============================

def _norm_txt(s: str | None) -> str:
    """Normalización suave para comparaciones."""
    return (s or "").strip().lower()


def _assert_role_present(cargo: str | None):
    cargo_norm = (cargo or "").strip()
    if not cargo_norm:
        raise ValueError("El rol/cargo es obligatorio.")
    return cargo_norm


def _assert_no_duplicates(*, company_id: int, member_id: Optional[int], nombre: str, cargo: str):
    """
    Evita duplicados exactos (mismo nombre + mismo cargo) dentro de la misma sociedad.
    - member_id puede ser None (alta) o un id (edición).
    """
    nombre_k = _norm_txt(nombre)
    cargo_k = _norm_txt(_normalize_role(cargo) or cargo)

    for r in list_board(company_id):
        # excluirse a sí mismo si es edición
        rid = r.get("id")
        if member_id is not None and rid == member_id:
            continue
        if _norm_txt(r.get("nombre")) == nombre_k and _norm_txt(r.get("cargo")) == cargo_k:
            raise ValueError(f"Ya existe un consejero con el mismo nombre y cargo: “{nombre} – {r.get('cargo')}”.")


def _assert_unique_president_if_needed(*, company_id: int, member_id: Optional[int], cargo: str):
    """
    Si ENFORCE_UNIQUE_PRESIDENT=True → valida que no haya más de un Presidente en la sociedad.
    """
    if not ENFORCE_UNIQUE_PRESIDENT:
        return
    cargo_final = _normalize_role(cargo) or cargo
    if cargo_final != "Presidente":
        return
    for r in list_board(company_id):
        rid = r.get("id")
        if member_id is not None and rid == member_id:
            continue
        if (r.get("cargo") or "").strip() == "Presidente":
            raise ValueError("Ya existe un Presidente en esta sociedad. Debes cambiar el rol del actual o del nuevo registro.")


# ============================
# Guardado con validación
# ============================

def save_board_member(
    *,
    id: Optional[int],
    company_id: int,
    nombre: str,
    cargo: str,
    nif: str,
    direccion: Optional[str],
    telefono: Optional[str],
    email: Optional[str]
) -> int:
    nombre = (nombre or "").strip()
    if not nombre:
        raise ValueError("El nombre es obligatorio.")

    # Normalizaciones/validaciones básicas existentes
    nif = normalize_nif_cif(nif)
    if email and not validate_email(email):
        raise ValueError("Email no válido.")
    telefono = normalize_phone(telefono)

    # 1) rol obligatorio + normalizado
    cargo_final = _normalize_role(_assert_role_present(cargo)) or cargo.strip()

    # 2) duplicados exactos (nombre+cargo)
    _assert_no_duplicates(company_id=company_id, member_id=id, nombre=nombre, cargo=cargo_final)

    # 3) unico Presidente (opcional, activado)
    _assert_unique_president_if_needed(company_id=company_id, member_id=id, cargo=cargo_final)

    # Persistencia
    return governance_repo.upsert_board_member(
        id=id, company_id=company_id, nombre=nombre, cargo=cargo_final, nif=nif,
        direccion=(direccion or None), telefono=(telefono or None), email=(email or None)
    )


# NUEVO: recompute correlativo del consejo (board_no) por sociedad
def recompute_board_numbers(company_id: Optional[int] = None) -> int:
    """
    Recalcula board_no por sociedad en board_members (idempotente).
    Si 'company_id' es None, lo hace para todas las compañías.
    """
    return governance_repo.recompute_board_no(company_id)