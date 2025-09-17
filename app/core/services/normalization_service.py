# app/core/services/normalization_service.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Any, Tuple, List, Dict, TypedDict
import sqlite3
import unicodedata
import re

from ...infra.db import get_connection
from ..validators import normalize_nif_cif  # validador existente

# ---------------------------
# Utilidades de normalización
# ---------------------------

# Permite letras, dígitos, espacio y PUNTO (para "s.l.")
_NON_ALNUM_SPACE_RE = re.compile(r"[^a-z0-9 .]+")
_SPACES_RE = re.compile(r"\s+")

# Partículas (minúsculas en title-case)
_LOWER_PARTICLES = {
    "de", "del", "la", "las", "el", "los", "y", "en", "por", "para", "con", "da", "do"
}

# Abreviaturas societarias -> forma canónica (mayúsculas, con puntos)
_ABBR_CANON: Dict[str, str] = {
    "sl": "S.L.",
    "s.l": "S.L.",
    "s.l.": "S.L.",
    "s l": "S.L.",
    "slu": "S.L.U.",
    "s.l.u": "S.L.U.",
    "s.l.u.": "S.L.U.",
    "s l u": "S.L.U.",
    "sa": "S.A.",
    "s.a": "S.A.",
    "s.a.": "S.A.",
    "s a": "S.A.",
    "sau": "S.A.U.",
    "s.a.u": "S.A.U.",
    "s.a.u.": "S.A.U.",
    "s a u": "S.A.U.",
}

_ROMAN_RE = re.compile(
    r"^(?=[MDCLXVI]+$)M{0,4}(CM|CD|D?C{0,3})"
    r"(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$",
    re.I,
)

def _strip_accents(s: str) -> str:
    if not s:
        return s
    return "".join(c for c in unicodedata.normalize("NFKD", s) if unicodedata.category(c) != "Mn")

def build_search_name(name: Optional[str]) -> Optional[str]:
    """
    Nombre para búsquedas: minúsculas, sin tildes, espacios colapsados.
    Conserva '.' para abreviaturas (p.ej., 's.l.'), pero elimina comas y otra
    puntuación irrelevante.
    """
    if not name:
        return None
    s = _strip_accents(str(name)).lower().strip()
    s = _SPACES_RE.sub(" ", s)
    # Eliminar signos comunes de puntuación excepto el punto (.)
    # para respetar 's.l.' / 's.a.' en búsquedas.
    s = re.sub(r"[^a-z0-9. ]+", "", s)
    s = _SPACES_RE.sub(" ", s).strip()
    return s or None

# app/core/services/normalization_service.py

def build_name_ascii(name: Optional[str]) -> Optional[str]:
    """
    Nombre ASCII: minúsculas, sin tildes, solo [a-z0-9 espacio], espacios colapsados.
    Elimina puntuación (incluido el punto) aunque build_search_name la conserve.
    """
    if not name:
        return None
    # Partimos de la versión de búsqueda (minúsculas, sin tildes, espacios normalizados)
    s = build_search_name(name) or ""
    # Eliminar cualquier carácter que NO sea [a-z0-9 espacio] -> quita '.', comas, etc.
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    s = _SPACES_RE.sub(" ", s).strip()
    return s or None

def _normalize_company_token(tok: str) -> str:
    key = tok.replace(",", " ").strip().lower().rstrip(".")
    key = _SPACES_RE.sub(" ", key)
    if key in _ABBR_CANON:
        return _ABBR_CANON[key]
    return tok

def _titlecase_spanish(text: str) -> str:
    """
    Title case en español con:
      - partículas en minúscula
      - números romanos en mayúscula
      - abreviaturas societarias canónicas (S.L., S.A., S.L.U., S.A.U.)
    """
    if not text:
        return text
    s = _SPACES_RE.sub(" ", text.strip())
    tokens = s.split(" ")
    out: List[str] = []
    for raw in tokens:
        if not raw:
            continue
        tok = raw

        canon = _normalize_company_token(tok)
        if canon != tok:
            out.append(canon)
            continue

        if _ROMAN_RE.match(tok):
            out.append(tok.upper())
            continue

        low = tok.lower()
        if low in _LOWER_PARTICLES:
            out.append(low)
            continue

        if len(tok) == 1:
            out.append(tok.upper())
        else:
            out.append(tok[0].upper() + tok[1:].lower())
    return " ".join(out)

def normalize_display_name(name: Optional[str], *, remove_accents: bool = False) -> Optional[str]:
    """
    Normaliza un nombre para mostrar (Title Case español con reglas anteriores).
    Si remove_accents=True, elimina acentos del resultado.
    """
    if not name:
        return None
    s = _SPACES_RE.sub(" ", str(name)).strip()
    s = _titlecase_spanish(s)
    if remove_accents:
        s = _strip_accents(s)
    return s or None


# ============================================================
# RECOMPUTE de columnas denormalizadas (si existen)
# ============================================================

def recompute_denormalized(company_id: Optional[int] = None) -> dict[str, Any]:
    """
    Rellena/actualiza columnas auxiliares si existen en partners:
      - partners.search_name
      - partners.name_ascii
    No crea columnas; si no existen, las ignora.
    """
    out: dict[str, Any] = {"partners": {"examined": 0, "updated": 0, "details": []}}

    with get_connection() as conn:
        conn.row_factory = sqlite3.Row

        cols = {r["name"] for r in conn.execute("PRAGMA table_info(partners)")}
        need_search = "search_name" in cols
        need_ascii = "name_ascii" in cols

        if not (need_search or need_ascii):
            return out

        if company_id is None:
            rows = conn.execute(
                "SELECT id, company_id, nombre, search_name, name_ascii FROM partners"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, company_id, nombre, search_name, name_ascii FROM partners WHERE company_id=?",
                (company_id,),
            ).fetchall()

        out["partners"]["examined"] = len(rows)

        to_update: List[Tuple[str, List[Any]]] = []
        examples: List[dict] = []

        for r in rows:
            pid = int(r["id"])
            nombre = r["nombre"] or ""
            curr_search = r["search_name"] if need_search else None
            curr_ascii = r["name_ascii"] if need_ascii else None

            new_search = build_search_name(nombre) if need_search else None
            new_ascii = build_name_ascii(nombre) if need_ascii else None

            changed = False
            sets: List[str] = []
            vals: List[Any] = []

            if need_search and (new_search != curr_search):
                sets.append("search_name=?")
                vals.append(new_search)
                changed = True
            if need_ascii and (new_ascii != curr_ascii):
                sets.append("name_ascii=?")
                vals.append(new_ascii)
                changed = True

            if changed:
                vals.append(pid)
                to_update.append((f"UPDATE partners SET {', '.join(sets)} WHERE id=?", vals))
                if len(examples) < 25:
                    examples.append(
                        {
                            "id": pid,
                            "before": {"search_name": curr_search, "name_ascii": curr_ascii},
                            "after": {"search_name": new_search, "name_ascii": new_ascii},
                        }
                    )

        for sql, params in to_update:
            conn.execute(sql, params)
        conn.commit()

        out["partners"]["updated"] = len(to_update)
        out["partners"]["details"] = examples

    return out


# ============================================================
# Normalización avanzada – usada por Utilidades (UI)
# ============================================================

class SectionResult(TypedDict):
    changed: int
    samples: List[dict]

class RunResult(TypedDict):
    dry_run: bool
    partners: SectionResult
    governance: SectionResult

@dataclass
class NormalizationOptions:
    scope: str = "partners"                 # "partners" | "governance" | "both"
    company_id: Optional[int] = None
    fix_names: bool = True
    fix_nif: bool = True
    remove_accents: bool = False
    dry_run: bool = True
    sample_limit: int = 30

def _run_normalization_opts(opts: NormalizationOptions) -> RunResult:
    """
    Aplica normalización en tablas soportadas según `opts.scope`.
    Devuelve un resumen con contadores y muestras de cambios.
    """
    result: RunResult = {
        "dry_run": bool(opts.dry_run),
        "partners": {"changed": 0, "samples": []},
        "governance": {"changed": 0, "samples": []},
    }
    sample_limit = int(opts.sample_limit or 30)

    with get_connection() as conn:
        conn.row_factory = sqlite3.Row

        def _maybe_commit() -> None:
            if not opts.dry_run:
                conn.commit()

        # -------- PARTNERS --------
        if opts.scope in {"partners", "both"}:
            if opts.company_id is None:
                rows = conn.execute(
                    "SELECT id, company_id, nombre, nif FROM partners"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, company_id, nombre, nif FROM partners WHERE company_id=?",
                    (opts.company_id,),
                ).fetchall()

            for r in rows:
                pid = int(r["id"])
                nombre_old = r["nombre"] or ""
                nif_old = r["nif"] or ""

                nombre_new = nombre_old
                if opts.fix_names:
                    nombre_new = normalize_display_name(nombre_old, remove_accents=opts.remove_accents) or nombre_old

                nif_new = nif_old
                if opts.fix_nif and nif_old:
                    nif_new = normalize_nif_cif(nif_old)

                if (nombre_new != nombre_old) or (nif_new != nif_old):
                    result["partners"]["changed"] += 1

                    if len(result["partners"]["samples"]) < sample_limit:
                        result["partners"]["samples"].append(
                            {
                                "id": pid,
                                "before": {"nombre": nombre_old, "nif": nif_old},
                                "after": {"nombre": nombre_new, "nif": nif_new},
                            }
                        )

                    if not opts.dry_run:
                        conn.execute(
                            "UPDATE partners SET nombre=?, nif=? WHERE id=?",
                            (nombre_new, nif_new, pid),
                        )

            _maybe_commit()

        # -------- GOVERNANCE (board_members) --------
        if opts.scope in {"governance", "both"}:
            # Si no existe la tabla, ignorar silenciosamente
            have_tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "board_members" in have_tables:
                if opts.company_id is None:
                    rows = conn.execute(
                        "SELECT id, company_id, nombre, nif FROM board_members"
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT id, company_id, nombre, nif FROM board_members WHERE company_id=?",
                        (opts.company_id,),
                    ).fetchall()

                for r in rows:
                    bid = int(r["id"])
                    nombre_old = r["nombre"] or ""
                    nif_old = r["nif"] or ""

                    nombre_new = nombre_old
                    if opts.fix_names:
                        nombre_new = normalize_display_name(nombre_old, remove_accents=opts.remove_accents) or nombre_old

                    nif_new = nif_old
                    if opts.fix_nif and nif_old:
                        nif_new = normalize_nif_cif(nif_old)

                    if (nombre_new != nombre_old) or (nif_new != nif_old):
                        result["governance"]["changed"] += 1

                        if len(result["governance"]["samples"]) < sample_limit:
                            result["governance"]["samples"].append(
                                {
                                    "id": bid,
                                    "before": {"nombre": nombre_old, "nif": nif_old},
                                    "after": {"nombre": nombre_new, "nif": nif_new},
                                }
                            )

                        if not opts.dry_run:
                            conn.execute(
                                "UPDATE board_members SET nombre=?, nif=? WHERE id=?",
                                (nombre_new, nif_new, bid),
                            )

                _maybe_commit()

    return result

def run_normalization(
    scope: str = "partners",
    company_id: Optional[int] = None,
    *,
    fix_names: bool = True,
    fix_nif: bool = True,
    remove_accents: bool = False,
    dry_run: bool = True,
    sample_limit: int = 30,
) -> RunResult:
    """
    Firma pensada para utilities.py:
      run_normalization(scope="partners" | "governance" | "both", company_id=...,
                        fix_names=True, fix_nif=True, remove_accents=False,
                        dry_run=True, sample_limit=30)
    """
    scope_eff = (scope or "partners").lower()
    if scope_eff not in {"partners", "governance", "both"}:
        scope_eff = "partners"

    opts = NormalizationOptions(
        scope=scope_eff,
        company_id=company_id,
        fix_names=bool(fix_names),
        fix_nif=bool(fix_nif),
        remove_accents=bool(remove_accents),
        dry_run=bool(dry_run),
        sample_limit=int(sample_limit),
    )
    return _run_normalization_opts(opts)