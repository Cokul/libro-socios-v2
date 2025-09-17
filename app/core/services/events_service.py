# app/core/services/events_service.py

from __future__ import annotations
from typing import Optional, Any
from datetime import datetime
import sqlite3

from ..repositories import events_repo, partners_repo
from ...infra.db import get_connection
from app.core.enums import normalize_event_type, EVENT_TYPES

# === Asegura triggers tipo V1 al cargar el servicio (idempotente) ===
try:
    from ..repositories.events_repo import ensure_redenominacion_triggers
    ensure_redenominacion_triggers()
except Exception:
    # No bloquear la app si falla en caliente; puedes exponer un botón de "Autochequeo" para relanzarlo.
    pass


# ---------- LISTADOS ----------

def list_events(company_id: int) -> list[dict]:
    """Listado raw (con IDs) para edición."""
    return events_repo.list_events_upto(company_id, None)


def list_events_for_ui(company_id: int) -> list[dict]:
    """Listado preparado para UI (mapea IDs de socios a nombres)."""
    rows = events_repo.list_events_upto(company_id, None)
    partners = {p["id"]: p.get("nombre") for p in partners_repo.list_by_company(company_id)}
    out = []
    for r in rows:
        out.append({
            "id": r.get("id"),
            "correlativo": r.get("correlativo"),
            "fecha": r.get("fecha"),
            "tipo": r.get("tipo"),
            "socio_transmite": partners.get(r.get("socio_transmite")) if r.get("socio_transmite") else None,
            "socio_adquiere": partners.get(r.get("socio_adquiere")) if r.get("socio_adquiere") else None,
            "rango_desde": r.get("rango_desde"),
            "rango_hasta": r.get("rango_hasta"),
            "nuevo_valor_nominal": r.get("nuevo_valor_nominal"),
            "documento": r.get("documento"),
            "observaciones": r.get("observaciones"),
        })
    return out


# ---------- CRUD ----------

_ALLOWED_FIELDS = {
    "company_id", "fecha", "tipo",
    "socio_transmite", "socio_adquiere",
    "rango_desde", "rango_hasta",
    "nuevo_valor_nominal",
    "documento", "observaciones",
    "hora", "orden_del_dia",
    # Columna física para la cantidad:
    "n_participaciones",
    # Si tu tabla tiene timestamps, puedes añadir:
    # "created_at", "updated_at",
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _require(cond: bool, msg: str, errors: list[str]):
    if not cond:
        errors.append(msg)

def _validate_range(rango_desde: Optional[int], rango_hasta: Optional[int], errors: list[str], *, required: bool = True):
    if not required and (rango_desde is None and rango_hasta is None):
        return
    _require(rango_desde is not None and rango_hasta is not None, "Debes indicar rango desde y hasta.", errors)
    if rango_desde is not None and rango_hasta is not None:
        _require(int(rango_desde) > 0 and int(rango_hasta) > 0, "El rango debe ser > 0.", errors)
        _require(int(rango_hasta) >= int(rango_desde), "El rango hasta debe ser ≥ rango desde.", errors)

def _validate_partners(st: Optional[int], sa: Optional[int], errors: list[str], *, need_st: bool, need_sa: bool, can_equal: bool = False):
    if need_st:
        _require(st is not None, "Debes indicar el socio transmite.", errors)
    if need_sa:
        _require(sa is not None, "Debes indicar el socio adquiere.", errors)
    if (st is not None) and (sa is not None) and not can_equal:
        _require(int(st) != int(sa), "Transmite y adquiere no pueden ser el mismo socio.", errors)

def _validate_nvn(nvn: Optional[float], errors: list[str], *, required: bool = False, positive: bool = True):
    if required:
        _require(nvn is not None, "Debes indicar el nuevo valor nominal.", errors)
    if nvn is not None and positive:
        _require(float(nvn) > 0.0, "El nuevo valor nominal debe ser > 0.", errors)

def _validate_event_semantics(*, tipo: str, st, sa, rd, rh, nvn, errors: list[str]) -> None:
    """Valida combinaciones básicas por tipo. Acumula mensajes en errors."""
    t = (tipo or "").upper().strip()

    # Rango consistente
    if (rd is None) ^ (rh is None):
        errors.append("Indica el rango completo RD–RH o deja ambos vacíos.")
    if rd is not None and rh is not None and int(rh) < int(rd):
        errors.append("El rango_hasta no puede ser menor que rango_desde.")

    # Reglas por tipo (simples, en línea con los formularios)
    need_rd_rh = {"TRANSMISION", "SUCESION", "USUFRUCTO", "ALTA", "AMPL_EMISION", "BAJA", "RED_AMORT",
                  "PIGNORACION", "EMBARGO", "CANCELA_PIGNORACION", "CANCELA_EMBARGO", "LEV_GRAVAMEN", "ALZAMIENTO"}
    if t in need_rd_rh and (rd is None or rh is None):
        errors.append(f"{t}: debes indicar RD–RH.")

    if t in {"TRANSMISION", "SUCESION", "USUFRUCTO"}:
        if not st: errors.append(f"{t}: falta 'socio_transmite'.")
        if not sa: errors.append(f"{t}: falta 'socio_adquiere'.")

    if t in {"ALTA", "AMPL_EMISION"}:
        if not sa: errors.append(f"{t}: falta 'socio_adquiere'.")

    if t in {"BAJA", "RED_AMORT"}:
        if not st: errors.append(f"{t}: falta 'socio_transmite'.")

    if t in {"PIGNORACION", "EMBARGO", "CANCELA_PIGNORACION", "CANCELA_EMBARGO", "LEV_GRAVAMEN", "ALZAMIENTO"}:
        if not st: errors.append(f"{t}: falta 'socio_transmite' (titular).")
        if not sa: errors.append(f"{t}: falta 'socio_adquiere' (acreedor/beneficiario).")

    if t in {"AMPL_VALOR", "RED_VALOR"}:
        if not (nvn is not None and float(nvn) > 0):
            errors.append(f"{t}: debes indicar un 'nuevo_valor_nominal' > 0.")
        if rd is not None or rh is not None:
            errors.append(f"{t}: no uses RD–RH (no aplica).")


def update_event(
    *,
    event_id: int,
    company_id: int,
    tipo: Optional[str] = None,
    fecha: Optional[str] = None,
    socio_transmite: Optional[int] = None,
    socio_adquiere: Optional[int] = None,
    rango_desde: Optional[int] = None,
    rango_hasta: Optional[int] = None,
    n_participaciones: Optional[int] = None,
    nuevo_valor_nominal: Optional[float] = None,
    documento: Optional[str] = None,
    observaciones: Optional[str] = None,
    hora: Optional[str] = None,
    orden_del_dia: Optional[int] = None,
) -> int:
    # Normalizaciones/validaciones simples
    if n_participaciones is not None:
        try:
            n_participaciones = int(n_participaciones)
        except Exception:
            raise ValueError("n_participaciones debe ser un entero.")
        if n_participaciones < 0:
            raise ValueError("n_participaciones debe ser un entero ≥ 0.")

    tipo_norm = normalize_event_type(tipo) if (tipo is not None) else None
    if tipo_norm is not None and (tipo_norm not in EVENT_TYPES and tipo_norm != "OTRO"):
        raise ValueError(f"Tipo de evento no reconocido: {tipo_norm}")

    # Validaciones semánticas (solo si nos pasan campos de negocio)
    errors: list[str] = []
    _validate_event_semantics(
        tipo=tipo_norm or (tipo if tipo is not None else ""),
        st=socio_transmite,
        sa=socio_adquiere,
        rd=rango_desde,
        rh=rango_hasta,
        nvn=nuevo_valor_nominal,
        errors=errors,
    )
    if errors:
        raise ValueError(" ".join(errors))

    # Construcción del UPDATE
    fields = {
        "tipo": tipo_norm if tipo is not None else None,
        "fecha": fecha,
        "socio_transmite": socio_transmite,
        "socio_adquiere": socio_adquiere,
        "rango_desde": rango_desde,
        "rango_hasta": rango_hasta,
        "n_participaciones": n_participaciones,
        "nuevo_valor_nominal": nuevo_valor_nominal,
        "documento": documento,
        "observaciones": observaciones,
        "hora": hora,
        "orden_del_dia": orden_del_dia,
        # "updated_at": _now_iso(),
    }
    sets, vals = [], []
    for k, v in fields.items():
        if v is not None:
            sets.append(f"{k}=?")
            vals.append(v)
    if not sets:
        return 0

    vals.extend([event_id, company_id])
    with get_connection() as conn:
        cur = conn.execute(f"UPDATE events SET {', '.join(sets)} WHERE id=? AND company_id=?", vals)
        conn.commit()
        return cur.rowcount

def create_event_generic(
    *,
    company_id: int,
    tipo: str,
    fecha: str,
    socio_transmite: Optional[int] = None,
    socio_adquiere: Optional[int] = None,
    rango_desde: Optional[int] = None,
    rango_hasta: Optional[int] = None,
    # Alias admitidos (cualquiera puede venir o ninguno):
    n_participaciones: Optional[int] = None,
    num_participaciones: Optional[int] = None,
    participaciones: Optional[int] = None,
    cantidad: Optional[int] = None,
    nuevo_valor_nominal: Optional[float] = None,
    documento: Optional[str] = None,
    observaciones: Optional[str] = None,
    **kwargs: Any,
) -> int:
    """
    Inserta un evento en la tabla 'events' aceptando alias de cantidad y normalizando a 'n_participaciones'.
    Evita depender de events_repo.create_event (no existe en V2).
    """
    # 1) Normalización de tipo/fecha
    tipo = (tipo or "").upper().strip()
    fecha = str(fecha)

    # 2) Resolver número de participaciones a partir de los alias recibidos
    canon_num = None
    for candidate in (n_participaciones, num_participaciones, participaciones, cantidad):
        if candidate is not None:
            canon_num = int(candidate)
            break

    # (Opcional) endurecer validación por tipo:
    # requires_qty = tipo in {"ALTA", "BAJA", "AMPL_EMISION", "RED_AMORT", "TRANSMISION"}
    # if requires_qty and canon_num is None:
    #     raise ValueError(f"El tipo {tipo} requiere indicar el número de participaciones.")
    # if canon_num is not None and canon_num <= 0:
    #     raise ValueError("El número de participaciones debe ser un entero positivo.")
    
    # Normalizar tipo con enums
    tipo = normalize_event_type(tipo) or tipo
    if tipo not in EVENT_TYPES and tipo != "OTRO":
        raise ValueError(f"Tipo de evento no reconocido: {tipo}")

    # Validaciones mínimas por tipo (semántica V1)
    errors: list[str] = []
    _validate_event_semantics(
        tipo=tipo,
        st=socio_transmite,
        sa=socio_adquiere,
        rd=rango_desde,
        rh=rango_hasta,
        nvn=nuevo_valor_nominal,
        errors=errors
    )
    if errors:
        # devolvemos todos los errores juntos
        raise ValueError(" · ".join(errors))

    # 3) Construir el diccionario de campos a insertar (sólo los permitidos)
    fields: dict[str, Any] = {
        "company_id": company_id,
        "tipo": tipo,
        "fecha": fecha,
        "socio_transmite": socio_transmite,
        "socio_adquiere": socio_adquiere,
        "rango_desde": rango_desde,
        "rango_hasta": rango_hasta,
        "nuevo_valor_nominal": float(nuevo_valor_nominal) if nuevo_valor_nominal is not None else None,
        "documento": documento,
        "observaciones": observaciones,
        # Si usas timestamps:
        # "created_at": _now_iso(),
        # "updated_at": _now_iso(),
    }

    # Si hay número (canon_num), lo volcamos a la columna física n_participaciones
    if canon_num is not None:
        fields["n_participaciones"] = int(canon_num)

    # 4) Filtrar sólo columnas permitidas y no-None para la INSERT
    cols = []
    vals = []
    for k, v in fields.items():
        if k in _ALLOWED_FIELDS and v is not None:
            cols.append(k)
            vals.append(v)

    if not cols:
        raise ValueError("No hay campos válidos para insertar el evento.")

    placeholders = ", ".join(["?"] * len(cols))
    colnames = ", ".join(cols)

    # 5) Ejecutar la INSERT y devolver el id
    with get_connection() as conn:
        cur = conn.execute(f"INSERT INTO events ({colnames}) VALUES ({placeholders})", vals)
        conn.commit()
        return cur.lastrowid


def get_event(company_id: int, event_id_or_corr: int) -> Optional[dict]:
    """
    Busca primero por ID; si no existe, intenta por correlativo.
    Devuelve el evento como dict o None si no lo encuentra.
    """
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row

        # 1) Buscar por ID
        row = conn.execute(
            "SELECT * FROM events WHERE company_id=? AND id=? LIMIT 1",
            (company_id, int(event_id_or_corr)),
        ).fetchone()

        # 2) Si no hay, buscar por correlativo
        if not row:
            row = conn.execute(
                "SELECT * FROM events WHERE company_id=? AND correlativo=? LIMIT 1",
                (company_id, int(event_id_or_corr)),
            ).fetchone()

    return dict(row) if row else None


def update_event(
    *,
    event_id: int,
    company_id: int,
    tipo: Optional[str] = None,
    fecha: Optional[str] = None,
    socio_transmite: Optional[int] = None,
    socio_adquiere: Optional[int] = None,
    rango_desde: Optional[int] = None,
    rango_hasta: Optional[int] = None,
    n_participaciones: Optional[int] = None,
    nuevo_valor_nominal: Optional[float] = None,
    documento: Optional[str] = None,
    observaciones: Optional[str] = None,
    hora: Optional[str] = None,
    orden_del_dia: Optional[int] = None,
) -> int:
    # (Opcional) Validación suave del número de participaciones
    if n_participaciones is not None:
        try:
            n_participaciones = int(n_participaciones)
        except Exception as _e:
            raise ValueError("n_participaciones debe ser un entero.")
        if n_participaciones < 0:
            raise ValueError("n_participaciones debe ser un entero ≥ 0.")

    fields = {
        "tipo": (tipo or "").upper().strip() if tipo is not None else None,
        "fecha": fecha,
        "socio_transmite": socio_transmite,
        "socio_adquiere": socio_adquiere,
        "rango_desde": rango_desde,
        "rango_hasta": rango_hasta,
        "n_participaciones": n_participaciones,
        "nuevo_valor_nominal": nuevo_valor_nominal,
        "documento": documento,
        "observaciones": observaciones,
        "hora": hora,
        "orden_del_dia": orden_del_dia,
        # "updated_at": _now_iso(),
    }
    sets = []
    vals = []
    for k, v in fields.items():
        if v is not None:
            sets.append(f"{k}=?")
            vals.append(v)
    if not sets:
        return 0
    vals.extend([event_id, company_id])

    with get_connection() as conn:
        cur = conn.execute(f"UPDATE events SET {', '.join(sets)} WHERE id=? AND company_id=?", vals)
        conn.commit()
        return cur.rowcount


def delete_event(*, event_id: int, company_id: int) -> int:
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM events WHERE id=? AND company_id=?", (event_id, company_id))
        conn.commit()
        return cur.rowcount


# ---------- Atajos específicos de negocio (si los usas) ----------

def apply_sucesion(
    *,
    company_id: int,
    fecha: str,
    socio_causante_id: int,
    socio_heredero_id: int,
    n_participaciones: int,
    referencia: Optional[str] = None
) -> int:
    """
    Sucesión: transmisión. Si trabajas por rangos, la UI debería aportar RD–RH concretos;
    aquí dejamos None (depende de tu editor por rangos).
    """
    return create_event_generic(
        company_id=company_id,
        tipo="SUCESION",
        fecha=fecha,
        socio_transmite=socio_causante_id,
        socio_adquiere=socio_heredero_id,
        rango_desde=None,
        rango_hasta=None,
        n_participaciones=n_participaciones,
        observaciones=referencia,
    )


def apply_reduccion_amortizacion(
    *,
    company_id: int,
    fecha: str,
    modalidad: str,
    n_participaciones: int,
    referencia: Optional[str] = None,
    socio_afectado_id: Optional[int] = None
) -> int:
    """
    Reducción por amortización. Si operas por rangos, aporta RD–RH desde la UI.
    """
    return create_event_generic(
        company_id=company_id,
        tipo="RED_AMORT",
        fecha=fecha,
        socio_transmite=socio_afectado_id,
        socio_adquiere=None,
        rango_desde=None,
        rango_hasta=None,
        n_participaciones=n_participaciones,
        observaciones=referencia,
    )


# ---------- CREACIÓN ESPECÍFICA: REDENOMINACION ----------

def create_redenominacion(
    *,
    company_id: int,
    fecha: str,
    por_bloque: bool,
    socio_id: Optional[int] = None,
    rango_desde: Optional[int] = None,
    rango_hasta: Optional[int] = None,
    recalcular_numero: bool = False,
    nuevo_valor_nominal: Optional[float] = None,
    documento: Optional[str] = None,
    observaciones: Optional[str] = None,
) -> int:
    """
    Replica reglas de V1:
      - GLOBAL: sin socio/rangos. Si 'recalcular_numero' => VN obligatorio > 0 (capital debe ser múltiplo de VN).
      - POR BLOQUE: requiere socio y RD–RH; NO recalcula nº total; VN opcional (sólo constancia).
      - Validaciones legales previas (mensajes claros).
    """
    fecha = str(fecha)

    if recalcular_numero and (nuevo_valor_nominal is None or float(nuevo_valor_nominal) <= 0.0):
        raise ValueError("Para recalcular el número de participaciones debes indicar un VN > 0.")

    if por_bloque:
        if not socio_id:
            raise ValueError("En redenominación por bloque debes indicar el socio titular del bloque.")
        if not (rango_desde and rango_hasta and int(rango_hasta) >= int(rango_desde)):
            raise ValueError("En redenominación por bloque debes indicar un rango RD–RH válido.")
        if recalcular_numero:
            raise ValueError("El recálculo del número de participaciones sólo aplica en modo global.")

        socio_transmite = int(socio_id)
        socio_adquiere = None
        rd, rh = int(rango_desde), int(rango_hasta)
        nv = float(nuevo_valor_nominal) if nuevo_valor_nominal else None
    else:
        # GLOBAL: sin socios ni rangos
        socio_transmite = None
        socio_adquiere = None
        rd = rh = None
        # Sólo guardamos VN si estamos en recálculo; en constancia global VN es opcional y puede omitirse
        nv = float(nuevo_valor_nominal) if (recalcular_numero and nuevo_valor_nominal) else None

    # Inserta el evento; el compute_service aplicará la lógica y validará múltiplos/rounding como en V1.
    new_id = create_event_generic(
        company_id=company_id,
        tipo="REDENOMINACION",
        fecha=fecha,
        socio_transmite=socio_transmite,
        socio_adquiere=socio_adquiere,
        rango_desde=rd,
        rango_hasta=rh,
        # No indicamos n_participaciones en redenominación
        nuevo_valor_nominal=nv,
        documento=documento or None,
        observaciones=observaciones or None,
    )
    return new_id