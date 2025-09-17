# app/core/services/reporting_service.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
from datetime import datetime
import sqlite3

from app.core.services.compute_service import compute_snapshot
from app.infra.db import get_connection
from app.core.repositories import events_repo

# ----------------------------- utils introspección ----------------------------
def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1", (name,)
    ).fetchone()
    return bool(row)

def _columns(conn, table: str) -> set:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}
    except Exception:
        return set()

def _has_column(conn, table: str, col: str) -> bool:
    return col in _columns(conn, table)

# Posibles nombres de tabla puente y columnas de clase
LINK_TABLE_CANDIDATES = ["event_partners", "events_partners", "event_lines", "event_legs"]
CLASS_COL_CANDIDATES  = ["class", "share_class", "serie", "series"]

# ------------------------------ modelos dto -----------------------------------
@dataclass
class KPIs:
    num_partners: int
    total_shares: int
    share_nominal: float | None
    share_capital: float | None
    last_event_date: str | None
    num_classes: int | None

# --------------------------- CAP TABLE (multi-esquema) ------------------------
# --- CAP TABLE a fecha ---
def cap_table(company_id: int, as_of: str | None = None) -> pd.DataFrame:
    """
    Devuelve la cap table a fecha 'as_of' (YYYY-MM-DD) a partir de compute_snapshot,
    coherente con Overview. Columnas de salida:
      partner_id, partner_name, nif, classes, shares, pct, capital_socio
    """
    ref_date = as_of
    snap = compute_snapshot(company_id, ref_date)
    socios = pd.DataFrame(snap.get("socios_vigentes") or [])

    if socios.empty:
        return pd.DataFrame(columns=["partner_id","partner_name","nif","classes","shares","pct","capital_socio"])

    socios = socios.rename(columns={
        "partner_id": "partner_id",
        "nombre": "partner_name",
        "participaciones": "shares",
        "porcentaje": "pct"
    })
    socios["classes"] = ""  # no manejas clases/series en tu esquema V2

    # Añadimos NIF desde partners
    with get_connection() as conn:
        partners = pd.read_sql_query(
            "SELECT id AS partner_id, COALESCE(nif,'') AS nif FROM partners WHERE company_id = ?",
            conn, params=(company_id,)
        )
    df = socios.merge(partners, on="partner_id", how="left")
    if "nif" not in df.columns:
        df["nif"] = ""
    df["nif"] = df["nif"].fillna("")
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    df["pct"] = pd.to_numeric(df["pct"], errors="coerce").fillna(0.0)

    # capital_socio = shares × valor_nominal (si disponible)
    snap2 = compute_snapshot(company_id, as_of)
    meta = snap2.get("meta", {}) if isinstance(snap2, dict) else {}
    valor_nominal = float(meta.get("valor_nominal")) if meta.get("valor_nominal") is not None else None
    if valor_nominal is not None:
        df["capital_socio"] = (df["shares"] * valor_nominal)
    else:
        df["capital_socio"] = None

    return df[["partner_id","partner_name","nif","classes","shares","pct","capital_socio"]]

# ------------------------------------ KPIs ------------------------------------
def kpis(company_id: int, as_of: str | None = None) -> KPIs:
    ref_date = as_of or datetime.today().strftime("%Y-%m-%d")
    snap = compute_snapshot(company_id, ref_date)
    meta = snap.get("meta", {}) if isinstance(snap, dict) else {}

    df_cap = cap_table(company_id, ref_date)
    num_partners = int((df_cap["shares"] > 0).sum())

    total_shares = int(meta.get("total_participaciones") or 0)
    share_nominal = float(meta["valor_nominal"]) if meta.get("valor_nominal") is not None else None
    share_capital = float(meta["capital_social"]) if meta.get("capital_social") is not None else None

    with get_connection() as conn:
        row = conn.execute(
            "SELECT MAX(fecha) FROM events WHERE company_id = ? AND (? IS NULL OR fecha <= ?)",
            (company_id, ref_date, ref_date)
        ).fetchone()
    last_event_date = row[0] if row and row[0] else None
    num_classes = 0

    return KPIs(num_partners, total_shares, share_nominal, share_capital, last_event_date, num_classes)

# ---------------------------- Movimientos (flex) ------------------------------
def movements(company_id: int,
              date_from: Optional[str] = None,
              date_to: Optional[str] = None,
              event_types: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Devuelve movimientos con columnas REALES:
      id, company_id, correlativo, fecha, tipo,
      socio_transmite, socio_adquiere,
      rango_desde, rango_hasta,
      nuevo_valor_nominal, documento, observaciones,
      hora, orden_del_dia, created_at, updated_at
    """
    rows = events_repo.list_events_upto(company_id, date_to)  # corte superior
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if date_from:
        df = df[df["fecha"] >= date_from]
    if event_types:
        df = df[df["tipo"].isin(event_types)]
    df = df.sort_values(by=["fecha","id"], ascending=[True, True]).reset_index(drop=True)
    return df

# --- Timeline a fecha ---
def event_timeline(company_id: int, as_of: str | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT fecha FROM events WHERE company_id=? AND (? IS NULL OR fecha <= ?) ORDER BY fecha ASC",
            (company_id, as_of, as_of)
        ).fetchall()
    dates = [r[0] for r in rows]
    if not dates:
        return pd.DataFrame(columns=["date","total_shares_acum"])

    data = []
    for d in dates:
        snap = compute_snapshot(company_id, d)
        tot = int((snap.get("meta") or {}).get("total_participaciones") or 0)
        data.append({"date": d, "total_shares_acum": tot})
    return pd.DataFrame(data)

def capital_timeline(company_id: int, as_of: str | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT fecha FROM events WHERE company_id=? AND (? IS NULL OR fecha <= ?) ORDER BY fecha ASC",
            (company_id, as_of, as_of)
        ).fetchall()
    dates = [r[0] for r in rows]
    if not dates:
        return pd.DataFrame(columns=["date", "capital_social"])

    data = []
    for d in dates:
        snap = compute_snapshot(company_id, d)
        meta = snap.get("meta", {}) if isinstance(snap, dict) else {}
        cap = meta.get("capital_social")
        cap = float(cap) if cap is not None else None
        data.append({"date": d, "capital_social": cap})
    return pd.DataFrame(data)

# --- Posición socio a fecha ---
def partner_position(company_id: int, partner_id: int, as_of: str | None = None) -> dict:
    df = cap_table(company_id, as_of)
    row = df[df["partner_id"] == partner_id]
    if row.empty:
        return {"partner_id": partner_id, "partner_name": "", "nif": "", "shares": 0, "pct": 0.0, "classes": ""}
    r = row.iloc[0]
    return {
        "partner_id": int(r["partner_id"]),
        "partner_name": str(r["partner_name"]),
        "nif": str(r["nif"] or ""),
        "shares": int(r["shares"]),
        "pct": float(r["pct"]),
        "classes": ""  # no usas clases
    }

# -------------------- Últimos apuntes de un socio (flex) ----------------------
def last_entries_for_partner(company_id: int, partner_id: int, limit: int = 10, as_of: str | None = None) -> pd.DataFrame:
    """
    Últimos eventos donde el socio aparece como transmite o adquiere.
    Columnas: id, fecha, tipo, documento, observaciones, socio_transmite, socio_adquiere
    """
    with get_connection() as conn:
        sql = """
            SELECT id, fecha, tipo, documento, observaciones,
                   socio_transmite, socio_adquiere
            FROM events
            WHERE company_id=?
              AND (socio_transmite=? OR socio_adquiere=?)
              AND (? IS NULL OR fecha <= ?)
            ORDER BY fecha DESC, id DESC
            LIMIT ?
        """
        df = pd.read_sql_query(sql, conn, params=(company_id, partner_id, partner_id, as_of, as_of, limit))
    return df

# --- RANGOS de participaciones vigentes por socio (a fecha) ---
def partner_holdings_ranges(company_id: int, partner_id: int, as_of: str) -> pd.DataFrame:
    """
    Devuelve bloques vigentes del socio a 'as_of' desde compute_snapshot:
      columnas: partner_id, rango_desde, rango_hasta, participaciones
    """
    snap = compute_snapshot(company_id, as_of)
    hv = pd.DataFrame(snap.get("holdings_vigentes") or [])
    if hv.empty:
        return pd.DataFrame(columns=["rango_desde","rango_hasta","participaciones"])
    hv = hv[hv.get("partner_id") == partner_id].copy()
    hv["participaciones"] = pd.to_numeric(hv.get("participaciones"), errors="coerce").fillna(0).astype(int)
    for col in ("rango_desde","rango_hasta"):
        if col in hv.columns:
            hv[col] = pd.to_numeric(hv[col], errors="coerce")
    sort_cols = [c for c in ["rango_desde","rango_hasta"] if c in hv.columns]
    if sort_cols:
        hv = hv.sort_values(by=sort_cols, ascending=True)
    return hv[["rango_desde","rango_hasta","participaciones"]]

# ======================= GRAVÁMENES (Altas / Cancelaciones) ===================
# Compatibilidad de literales:
#  - Altas: PIGNORACION / EMBARGO
#  - Cancelaciones: LEV_GRAVAMEN / ALZAMIENTO  (y admitimos CANCELA_* si existieran)
_ENC_START = ("PIGNORACION", "EMBARGO")
_ENC_CANCEL = ("LEV_GRAVAMEN", "ALZAMIENTO", "CANCELA_PIGNORACION", "CANCELA_EMBARGO")

def _partners_min_map(company_id: int) -> dict[int, dict]:
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, nombre, nif FROM partners WHERE company_id=?",
            (company_id,)
        ).fetchall()
    return {int(r["id"]): {"nombre": r["nombre"] or "", "nif": r["nif"] or ""} for r in rows}

def encumbrance_events(company_id: int, as_of: Optional[str] = None, partner_id: Optional[int] = None) -> pd.DataFrame:
    """
    Todos los eventos de gravamen (altas/cancelaciones), opcionalmente
    filtrados por titular (partner_id) y truncados a fecha 'as_of'.
    Columnas: fecha, tipo, titular_id, acreedor_id, rango_desde, rango_hasta, documento, observaciones
    """
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        sql = """
            SELECT id, fecha, tipo,
                   socio_transmite   AS titular_id,
                   socio_adquiere    AS acreedor_id,
                   rango_desde, rango_hasta,
                   documento, observaciones
            FROM events
            WHERE company_id=?
              AND tipo IN ({})
        """.format(",".join(["?"] * (len(_ENC_START) + len(_ENC_CANCEL))))
        params: List = [company_id, *list(_ENC_START + _ENC_CANCEL)]
        if as_of:
            sql += " AND fecha<=?"
            params.append(as_of)
        if partner_id:
            # por diseño, el titular debe ir en socio_transmite; mantenemos compatibilidad
            sql += " AND (socio_transmite=? OR socio_adquiere=?)"
            params.extend([partner_id, partner_id])
        sql += " ORDER BY fecha, id"
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        return pd.DataFrame(columns=[
            "fecha","tipo","titular_id","acreedor_id","rango_desde","rango_hasta","documento","observaciones"
        ])
    return pd.DataFrame([{
        "fecha": r["fecha"],
        "tipo":  r["tipo"],
        "titular_id": r["titular_id"],
        "acreedor_id": r["acreedor_id"],
        "rango_desde": r["rango_desde"],
        "rango_hasta": r["rango_hasta"],
        "documento": r["documento"] or "",
        "observaciones": r["observaciones"] or "",
    } for r in rows])

# ---- utilidades de rangos (enteros inclusivos [a,b]) -------------------------
Range = Tuple[int, int]

def _norm_range(a, b) -> Optional[Range]:
    if a is None or b is None:
        return None
    try:
        a, b = int(a), int(b)
    except Exception:
        return None
    if b < a:
        a, b = b, a
    return (a, b)

def _merge_ranges(ranges: List[Range]) -> List[Range]:
    if not ranges: return []
    ranges = sorted(ranges)
    merged = [ranges[0]]
    for a, b in ranges[1:]:
        la, lb = merged[-1]
        if a <= lb + 1:
            merged[-1] = (la, max(lb, b))
        else:
            merged.append((a, b))
    return merged

def _subtract_one(base: List[Range], cut: Range) -> List[Range]:
    if not base: return []
    out: List[Range] = []
    ca, cb = cut
    for a, b in base:
        if cb < a or ca > b:
            out.append((a, b))
        else:
            if a < ca: out.append((a, ca - 1))
            if cb < b: out.append((cb + 1, b))
    return _merge_ranges([r for r in out if r[0] <= r[1]])

def _subtract_many(base: List[Range], cuts: List[Range]) -> List[Range]:
    cur = _merge_ranges(base)
    for c in _merge_ranges(cuts):
        cur = _subtract_one(cur, c)
        if not cur:
            break
    return cur

def _encumbrances_for_titular_and_acreedor(events_df: pd.DataFrame, titular_id: int, acreedor_id: int) -> List[Range]:
    """Rangos vigentes para (titular, acreedor) a la fecha tope del DF."""
    if events_df is None or events_df.empty:
        return []
    df = events_df[
        (events_df["titular_id"] == titular_id) &
        (events_df["acreedor_id"] == acreedor_id)
    ].copy()
    if df.empty:
        return []

    altas: List[Range] = []
    bajas: List[Range] = []
    for _, r in df.iterrows():
        rng = _norm_range(r.get("rango_desde"), r.get("rango_hasta"))
        if not rng:
            continue
        t = str(r.get("tipo") or "").upper()
        if t in _ENC_START:
            altas.append(rng)
        elif t in _ENC_CANCEL:
            bajas.append(rng)

    if not altas:
        return []
    return _subtract_many(_merge_ranges(altas), bajas)

def active_encumbrances(company_id: int, partner_id: int, as_of: str) -> pd.DataFrame:
    """
    Rangos de gravamen VIGENTES a 'as_of' para el partner_id (titular).
    Columnas: fecha, tipo, rango_desde, rango_hasta, documento, observaciones
    """
    ev = encumbrance_events(company_id, as_of=as_of, partner_id=partner_id)
    if ev is None or ev.empty:
        return pd.DataFrame(columns=["fecha","tipo","rango_desde","rango_hasta","documento","observaciones"])

    acreedores = sorted([int(x) for x in ev["acreedor_id"].dropna().unique().tolist()])
    rows = []
    for acre_id in acreedores:
        tramos = _encumbrances_for_titular_and_acreedor(ev, partner_id, acre_id)
        if not tramos:
            continue
        # fecha de referencia ≈ mínima fecha de alta que contribuye al tramo
        ev_altas = ev[(ev["acreedor_id"] == acre_id) & (ev["tipo"].str.upper().isin(_ENC_START))]
        ref_fecha = None
        if not ev_altas.empty:
            try:
                ref_fecha = str(sorted(ev_altas["fecha"].astype(str).tolist())[0])
            except Exception:
                ref_fecha = None
        for a, b in tramos:
            rows.append({
                "fecha": ref_fecha or "",
                "tipo": "GRAVAMEN",
                "rango_desde": a,
                "rango_hasta": b,
                "documento": "",
                "observaciones": "",
            })
    if not rows:
        return pd.DataFrame(columns=["fecha","tipo","rango_desde","rango_hasta","documento","observaciones"])
    df = pd.DataFrame(rows)
    df.sort_values(by=["rango_desde","rango_hasta"], inplace=True, ignore_index=True)
    return df

def _substract_intervals(base: tuple[int,int], cuts: list[tuple[int,int]]) -> list[tuple[int,int]]:
    """Resta a [a,b] una lista de recortes; devuelve lista de residuos ordenados y no solapados."""
    if base is None:
        return []
    a, b = base
    if a is None or b is None:
        return [(a, b)]  # si no hay rango numérico definido, no partimos
    segs = [(a, b)]
    for ca, cb in sorted(cuts):
        new_segs = []
        for x0, x1 in segs:
            if cb < x0 or ca > x1:             # sin solape
                new_segs.append((x0, x1))
            else:
                if ca > x0:
                    new_segs.append((x0, ca-1))
                if cb < x1:
                    new_segs.append((cb+1, x1))
        segs = new_segs
        if not segs:
            break
    # limpia segmentos vacíos
    return [(x0, x1) for (x0, x1) in segs if x0 is not None and x1 is not None and x0 <= x1]


def active_encumbrances_affecting_partner(company_id: int, partner_id: int, as_of: str) -> pd.DataFrame:
    """
    Gravámenes vigentes a 'as_of' cuyos rangos se solapan con los bloques vigentes del socio.
    Aplica cancelaciones parciales (p.ej. ALZAMIENTO 50–75 resta al tramo de inicio).
    Columnas: fecha, tipo, rango_desde, rango_hasta, documento, observaciones,
              acreedor_id, acreedor_nombre, acreedor_nif
    """
    # 1) Bloques vigentes del socio
    rng = partner_holdings_ranges(company_id, partner_id, as_of)
    if rng is None or rng.empty:
        return pd.DataFrame(columns=[
            "fecha","tipo","rango_desde","rango_hasta","documento","observaciones",
            "acreedor_id","acreedor_nombre","acreedor_nif"
        ])
    rng["rango_desde"] = pd.to_numeric(rng["rango_desde"], errors="coerce").astype("Int64")
    rng["rango_hasta"] = pd.to_numeric(rng["rango_hasta"], errors="coerce").astype("Int64")

    with get_connection() as conn:
        conn.row_factory = sqlite3.Row

        # Inicios (pign./emb.) hasta as_of
        sql_start = f"""
            SELECT id, fecha, tipo, rango_desde, rango_hasta,
                   socio_transmite, socio_adquiere, documento, observaciones
            FROM events
            WHERE company_id=? AND fecha<=? AND tipo IN ({",".join("?"*len(_ENC_START))})
            ORDER BY fecha, id
        """
        starts = conn.execute(sql_start, (company_id, as_of, *_ENC_START)).fetchall()

        # Cancelaciones hasta as_of
        sql_cancel = f"""
            SELECT id, fecha, tipo, rango_desde, rango_hasta,
                   socio_transmite, socio_adquiere
            FROM events
            WHERE company_id=? AND fecha<=? AND tipo IN ({",".join("?"*len(_ENC_CANCEL))})
            ORDER BY fecha, id
        """
        cancels = conn.execute(sql_cancel, (company_id, as_of, *_ENC_CANCEL)).fetchall()

        # Mapa de socios para mostrar acreedor
        pmap = {
            int(r["id"]): {"nombre": r["nombre"] or "", "nif": r["nif"] or ""}
            for r in conn.execute("SELECT id, nombre, nif FROM partners WHERE company_id=?", (company_id,)).fetchall()
        }

    # Prepara recortes por acreedor (clave: socio_adquiere del inicio)
    cuts_by_creditor: dict[int, list[tuple[int,int]]] = {}
    for c in cancels:
        ca = pd.to_numeric(c["rango_desde"], errors="coerce")
        cb = pd.to_numeric(c["rango_hasta"], errors="coerce")
        if pd.isna(ca) or pd.isna(cb):
            # Cancelación sin rango numérico: por simplicidad, tratamos como nada que recortar.
            # (Si quieres que anule todo lo del acreedor, convierte esto en (-inf, +inf).)
            continue
        cred = c["socio_adquiere"] or c["socio_transmite"]
        if cred is None:
            continue
        cuts_by_creditor.setdefault(int(cred), []).append((int(ca), int(cb)))

    out_rows = []

    for e in starts:
        ea = pd.to_numeric(e["rango_desde"], errors="coerce")
        eb = pd.to_numeric(e["rango_hasta"], errors="coerce")

        # 2) ¿Se solapa con algún bloque vigente del socio?
        overlaps_partner = False
        if pd.isna(ea) or pd.isna(eb):
            overlaps_partner = True
        else:
            for _, rr in rng.iterrows():
                d = rr["rango_desde"]; h = rr["rango_hasta"]
                if pd.isna(d) or pd.isna(h):
                    continue
                if int(ea) <= int(h) and int(eb) >= int(d):
                    overlaps_partner = True
                    break
        if not overlaps_partner:
            continue

        # 3) Sustracción de cancelaciones del MISMO acreedor
        cred_id = e["socio_adquiere"] or e["socio_transmite"]
        base = None if (pd.isna(ea) or pd.isna(eb)) else (int(ea), int(eb))
        cuts = cuts_by_creditor.get(int(cred_id)) or []
        residuals = _substract_intervals(base, cuts)

        # Si no hay rango numérico, mantenlo tal cual (no partimos)
        if not residuals and (pd.isna(ea) or pd.isna(eb)):
            residuals = [(None, None)]

        # 4) Emitir una fila por cada tramo residual
        cred_nombre = cred_nif = ""
        if cred_id and int(cred_id) in pmap:
            cred_nombre = pmap[int(cred_id)]["nombre"]
            cred_nif    = pmap[int(cred_id)]["nif"]

        for ra, rb in residuals:
            # Si tras recortes no queda nada, saltamos
            if ra is not None and rb is not None and ra > rb:
                continue
            out_rows.append({
                "fecha": e["fecha"],
                "tipo": e["tipo"],
                "rango_desde": ra,
                "rango_hasta": rb,
                "documento": e["documento"] or "",
                "observaciones": e["observaciones"] or "",
                "acreedor_id": cred_id,
                "acreedor_nombre": cred_nombre,
                "acreedor_nif": cred_nif,
            })

    return pd.DataFrame(out_rows, columns=[
        "fecha","tipo","rango_desde","rango_hasta","documento","observaciones",
        "acreedor_id","acreedor_nombre","acreedor_nif"
    ])