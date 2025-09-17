# app/core/services/export_service.py
from __future__ import annotations

import logging
import sqlite3
from typing import Optional, List, Iterable
from io import BytesIO
from datetime import datetime
from reportlab.pdfbase import pdfmetrics

import pandas as pd
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.lib import colors

from app.infra.pdf_fonts import register_fonts as ensure_pdf_base_fonts
from app.infra.db import get_connection
from app.core.services.reporting_service import (
    cap_table, movements, partner_position, last_entries_for_partner,
    partner_holdings_ranges, active_encumbrances, active_encumbrances_affecting_partner,
)

log = logging.getLogger(__name__)

# ============================================================
#  Constantes visuales comunes (PDF)
# ============================================================
MARGIN_X = 18 * mm
LINE_GAP = 5 * mm
SECTION_GAP = 7 * mm
CONTENT_GAP = 6 * mm


# ============================================================
#  Utilidades PDF comunes
# ============================================================
def _hr(c: canvas.Canvas, y: float, x0: float = MARGIN_X, x1: float = A4[0] - MARGIN_X):
    c.setStrokeColor(colors.lightgrey)
    c.setLineWidth(0.7)
    c.line(x0, y, x1, y)


def _kv(c: canvas.Canvas, y: float, key: str, value: str) -> float:
    c.setFont("DejaVuSans", 9.5)
    c.drawString(MARGIN_X, y, f"{key}: ")
    c.setFont("DejaVuSans-Oblique", 9.5)
    c.drawString(MARGIN_X + 70 * mm, y, value or "—")
    return y - LINE_GAP


def _col(c: canvas.Canvas, x: float, y: float, text: str, size: float = 9.0, maxw: float | None = None):
    c.setFont("DejaVuSans", size)
    t = ("" if text is None else str(text))
    if maxw is None:
        c.drawString(x, y, t)
    else:
        while c.stringWidth(t, "DejaVuSans", size) > maxw and len(t) > 3:
            t = t[:-4] + "…"
        c.drawString(x, y, t)


def _section_title(c: canvas.Canvas, title: str, y: float) -> float:
    c.setFillColorRGB(0.95, 0.95, 0.95)
    c.rect(MARGIN_X, y - 6 * mm, (A4[0] - 2 * MARGIN_X), 8 * mm, stroke=0, fill=1)
    c.setFillColor(colors.black)
    c.setFont("DejaVuSans", 10)
    c.drawString(MARGIN_X + 2 * mm, y - 4 * mm, title.upper())
    return y - SECTION_GAP - 6 * mm


def _draw_paragraph(c, text: str, x: float, y: float, max_width: float, leading: float = 12.0, font="DejaVuSans", font_size=9):
    """Dibuja un párrafo con ajuste de línea y devuelve la nueva Y."""
    if not text:
        return y
    c.setFont(font, font_size)
    space_w = pdfmetrics.stringWidth(" ", font, font_size)
    line = ""
    line_w = 0.0

    def flush(curr_line, yy):
        if curr_line:
            c.drawString(x, yy, curr_line)
            yy -= leading
        return yy

    yy = y
    for word in text.split():
        w = pdfmetrics.stringWidth(word, font, font_size)
        if line and (line_w + space_w + w) > max_width:
            yy = flush(line, yy)
            line = word
            line_w = w
        else:
            if line:
                line += " " + word
                line_w += space_w + w
            else:
                line = word
                line_w = w
    yy = flush(line, yy)
    return yy


def _ledger_use_correlativo(df: pd.DataFrame) -> pd.DataFrame:
    """Si existe 'correlativo', lo coloca como primera columna visible y oculta 'id'."""
    d = df.copy()
    if "correlativo" in d.columns:
        d.rename(columns={"correlativo": "Nº asiento"}, inplace=True)
        first = ["Nº asiento"]
        rest = [c for c in d.columns if c not in ("id", "Nº asiento", "correlativo")]
        d = d[first + rest]
    if "id" in d.columns:
        d.drop(columns=["id"], inplace=True)
    return d


# ============================================================
#  Lookups / cabeceras reales
# ============================================================
def _partners_lookup(company_id: int) -> dict[int, dict]:
    """id -> {'nombre','nif','nacionalidad','domicilio','partner_no'(opcional)}"""
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cols = {row[1] for row in conn.execute("PRAGMA table_info(partners)").fetchall()}
        if "partner_no" in cols:
            rows = conn.execute(
                "SELECT id, nombre, nif, nacionalidad, domicilio, partner_no "
                "FROM partners WHERE company_id=? ORDER BY id",
                (company_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, nombre, nif, nacionalidad, domicilio "
                "FROM partners WHERE company_id=? ORDER BY id",
                (company_id,)
            ).fetchall()

    out: dict[int, dict] = {}
    for r in rows:
        out[int(r["id"])] = {
            "nombre": r["nombre"] or "",
            "nif": r["nif"] or "",
            "nacionalidad": r["nacionalidad"] or "",
            "domicilio": r["domicilio"] or "",
            "partner_no": (r["partner_no"] if ("partner_no" in r.keys()) else None),
        }
    return out


def _partner_no_map(company_id: int) -> dict[int, int | None]:
    """Devuelve {partner_id -> partner_no} si la columna existe; si no, dict vacío."""
    with get_connection() as conn:
        have = {row[1] for row in conn.execute("PRAGMA table_info(partners)").fetchall()}
        if "partner_no" not in have:
            return {}
        rows = conn.execute(
            "SELECT id, partner_no FROM partners WHERE company_id=?",
            (company_id,)
        ).fetchall()
        return {int(r[0]): (None if r[1] is None else int(r[1])) for r in rows}


def _company_header(company_id: int) -> dict:
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT name, cif, domicilio, fecha_constitucion FROM companies WHERE id=?",
            (company_id,)
        ).fetchone()
    if not row:
        return {"name": "", "cif": "", "domicilio": "", "fecha_constitucion": ""}
    return {
        "name": row["name"] or "",
        "cif": row["cif"] or "",
        "domicilio": row["domicilio"] or "",
        "fecha_constitucion": str(row["fecha_constitucion"] or "") or "",
    }


def _partner_id_by_nif_or_name(company_id: int, nombre: str | None, nif: str | None) -> int | None:
    if not nombre and not nif:
        return None
    with get_connection() as conn:
        if nif:
            r = conn.execute(
                "SELECT id FROM partners WHERE company_id=? AND nif=? LIMIT 1",
                (company_id, nif)
            ).fetchone()
            if r:
                return int(r[0])
        if nombre:
            r = conn.execute(
                "SELECT id FROM partners WHERE company_id=? AND nombre=? LIMIT 1",
                (company_id, nombre)
            ).fetchone()
            if r:
                return int(r[0])
    return None


# ============================================================
#  Construcción de filas del LIBRO (usando columnas reales)
# ============================================================
def _ledger_rows(company_id: int,
                 date_from: str | None,
                 date_to: str | None,
                 event_types: list[str] | None) -> pd.DataFrame:
    """
    DataFrame con: fecha, correlativo, tipo, documento, socio_transmite/adquiere,
    rango_desde/hasta, participaciones (derivadas), nuevo_valor_nominal, observaciones.
    """
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        sql = """
        SELECT id, correlativo, fecha, tipo,
               socio_transmite, socio_adquiere,
               rango_desde, rango_hasta,
               nuevo_valor_nominal,
               documento, observaciones
        FROM events
        WHERE company_id=?
        """
        params: list = [company_id]
        if date_from:
            sql += " AND fecha>=?"
            params.append(date_from)
        if date_to:
            sql += " AND fecha<=?"
            params.append(date_to)
        if event_types:
            sql += f" AND tipo IN ({','.join(['?']*len(event_types))})"
            params.extend(event_types)
        sql += " ORDER BY fecha, id"

        rows = conn.execute(sql, params).fetchall()

    pmap = _partners_lookup(company_id)
    out = []
    for r in rows:
        st_id = r["socio_transmite"]
        sa_id = r["socio_adquiere"]
        rd = r["rango_desde"]
        rh = r["rango_hasta"]

        # nº participaciones como (hasta - desde + 1) cuando haya rangos
        n_parts = None
        if rd is not None and rh is not None:
            try:
                n_parts = int(rh) - int(rd) + 1
            except Exception:
                n_parts = None

        out.append({
            "correlativo": r["correlativo"],
            "fecha": r["fecha"],
            "tipo": r["tipo"],
            "documento": r["documento"] or "",
            "socio_transmite_id": st_id,
            "socio_transmite_nombre": pmap.get(int(st_id), {}).get("nombre", "") if st_id else "",
            "socio_transmite_nif":    pmap.get(int(st_id), {}).get("nif", "")    if st_id else "",
            "socio_adquiere_id": sa_id,
            "socio_adquiere_nombre": pmap.get(int(sa_id), {}).get("nombre", "") if sa_id else "",
            "socio_adquiere_nif":    pmap.get(int(sa_id), {}).get("nif", "")    if sa_id else "",
            "rango_desde": rd,
            "rango_hasta": rh,
            "participaciones": n_parts,
            "nuevo_valor_nominal": r["nuevo_valor_nominal"],
            "observaciones": r["observaciones"] or "",
        })
    return pd.DataFrame(out)


# ============================================================
#  EXCEL: Cap table & Movimientos
# ============================================================
def export_cap_table_excel(company_id: int, as_of: str | None = None) -> BytesIO:
    """
    Genera un Excel con la cap table a fecha, añadiendo 'Nº socio' si partners.partner_no existe.
    """
    try:
        df = cap_table(company_id, as_of).copy()
        # Añadir "Nº socio" si podemos
        pno = _partner_no_map(company_id)
        if pno and ("partner_id" in df.columns):
            df.insert(0, "Nº socio", df["partner_id"].map(pno))
        elif "partner_no" in df.columns:
            df.insert(0, "Nº socio", df["partner_no"])
        # Escribir Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="CapTable")
            wb = writer.book
            ws = writer.sheets["CapTable"]
            fmt_int = wb.add_format({"num_format": "#,##0"})
            fmt_pct = wb.add_format({"num_format": "0.0000"})
            # Ajuste columnas
            for i, col in enumerate(df.columns):
                try:
                    width = max(12, min(40, int(df[col].astype(str).str.len().quantile(0.9)) + 2))
                except Exception:
                    width = 16
                ws.set_column(i, i, width)
            # Formatos
            if "shares" in df.columns:
                col_idx = df.columns.get_loc("shares")
                ws.set_column(col_idx, col_idx, 14, fmt_int)
            if "pct" in df.columns:
                col_idx = df.columns.get_loc("pct")
                ws.set_column(col_idx, col_idx, 10, fmt_pct)
        output.seek(0)
        log.info("Export CapTable.xlsx company_id=%s as_of=%s rows=%s",
                 company_id, as_of, len(df))
        return output
    except Exception as e:
        log.error("Error exportando CapTable.xlsx company_id=%s as_of=%s: %s",
                  company_id, as_of, e, exc_info=True)
        raise


def export_movements_excel(company_id: int,
                           date_from: Optional[str],
                           date_to: Optional[str],
                           event_types: Optional[List[str]]) -> BytesIO:
    """
    Genera un Excel con los movimientos filtrados.
    Si viene 'correlativo', lo muestra como 'Nº asiento' y oculta 'id'.
    """
    try:
        df = movements(company_id, date_from, date_to, event_types).copy()
        # Normalizar correlativo
        if "correlativo" in df.columns:
            df.rename(columns={"correlativo": "Nº asiento"}, inplace=True)
            first = ["Nº asiento"]
            rest = [c for c in df.columns if c not in ("id", "Nº asiento", "correlativo")]
            df = df[first + rest]
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Movimientos")
            wb = writer.book
            ws = writer.sheets["Movimientos"]
            fmt_int = wb.add_format({"num_format": "#,##0"})
            # Auto ancho básico
            for i, col in enumerate(df.columns):
                try:
                    width = max(12, min(40, int(df[col].astype(str).str.len().quantile(0.9)) + 2))
                except Exception:
                    width = 16
                ws.set_column(i, i, width)
            # Campo de cantidad si existe
            for qty_col in ("shares_delta", "participaciones", "n_participaciones"):
                if qty_col in df.columns:
                    idx = df.columns.get_loc(qty_col)
                    ws.set_column(idx, idx, 14, fmt_int)
                    break
        output.seek(0)
        log.info("Export Movimientos.xlsx company_id=%s from=%s to=%s types=%s rows=%s",
                 company_id, date_from, date_to,
                 ",".join(event_types) if event_types else "", len(df))
        return output
    except Exception as e:
        log.error("Error exportando Movimientos.xlsx company_id=%s: %s",
                  company_id, e, exc_info=True)
        raise


# ============================================================
#  EXCEL: Libro legalizable
# ============================================================
def export_ledger_excel_legalizable(
    company_id: int,
    date_from: Optional[str],
    date_to: Optional[str],
    event_types: Optional[List[str]],
    diligencia_apertura: Optional[str] = None,
    diligencia_cierre: Optional[str] = None,
    as_of: Optional[str] = None,
) -> BytesIO:
    """
    Libro Registro – Excel legalizable.
    Pestañas:
      - Resumen
      - Socios a fecha (con Nº socio)
      - Cap table a fecha (con Nº socio)
      - Rangos a fecha (con Nº socio)
      - Gravámenes a fecha
      - Movimientos del período (con Nº asiento si existe)
    """
    try:
        as_of_final = as_of or date_to or datetime.now().strftime("%Y-%m-%d")
        pmap   = _partners_lookup(company_id)
        pno    = _partner_no_map(company_id)

        # --- Cap table / socios vigentes a fecha ---
        df_cap = _vigentes_cap_table(company_id, as_of_final)
        vigentes_ids = _vigentes_ids_from_cap(df_cap, company_id)

        # -- Socios a fecha (usar partner_no si existe; fallback al id)
        socios_rows = []
        for pid in vigentes_ids:
            info = pmap.get(int(pid), {})
            numero = pmap.get(int(pid), {}).get("partner_no")
            socios_rows.append({
                "Nº socio": int(numero) if numero is not None else int(pid),
                "Nombre / Razón social": info.get("nombre", ""),
                "NIF/CIF": info.get("nif", ""),
                "Nacionalidad": info.get("nacionalidad", ""),
                "Domicilio": info.get("domicilio", ""),
            })
        df_socios = pd.DataFrame(socios_rows)

        # -- Cap table a fecha (añadir Nº socio)
        df_cap_x = df_cap.copy()
        # asegurar partner_id para mapear
        if "partner_id" not in df_cap_x.columns:
            df_cap_x["partner_id"] = None
        for i, r in df_cap_x.iterrows():
            pid = r.get("partner_id")
            if pd.isna(pid) or pid is None:
                pid = _partner_id_by_nif_or_name(company_id, r.get("partner_name"), r.get("nif"))
            df_cap_x.at[i, "partner_id"] = None if pid is None else int(pid)

        df_cap_x.insert(0, "Nº socio",
                        df_cap_x["partner_id"].map(pno) if pno else df_cap_x["partner_id"])
        df_cap_x.rename(columns={
            "partner_name": "Socio",
            "nif": "NIF/CIF",
            "shares": "Participaciones",
            "pct": "% (0–100)",
            "capital_socio": "Capital del socio (€)",
        }, inplace=True)
        cols_cap = ["Nº socio", "Socio", "NIF/CIF", "Participaciones", "% (0–100)", "Capital del socio (€)"]
        df_cap_x = df_cap_x[[c for c in cols_cap if c in df_cap_x.columns]]

        # -- Rangos vigentes por socio a fecha (con Nº socio)
        rows_ranges = []
        for _, r in df_cap.iterrows():
            pid = r.get("partner_id")
            if pd.isna(pid) or pid is None:
                pid = _partner_id_by_nif_or_name(company_id, r.get("partner_name"), r.get("nif"))
            if pid is None:
                continue
            rng = partner_holdings_ranges(company_id, int(pid), as_of_final)
            if rng is None or rng.empty:
                continue
            numero = pmap.get(int(pid), {}).get("partner_no")
            for _, rr in rng.iterrows():
                rows_ranges.append({
                    "Nº socio": int(numero) if numero is not None else int(pid),
                    "Socio": r.get("partner_name",""),
                    "NIF/CIF": r.get("nif",""),
                    "Desde": rr.get("rango_desde"),
                    "Hasta": rr.get("rango_hasta"),
                    "Participaciones": rr.get("participaciones"),
                })
        df_rng = pd.DataFrame(rows_ranges)

        # -- Gravámenes a fecha (igual que PDF)
        df_grav = _encumbrances_all(company_id, as_of_final, vigentes_ids, pmap).copy()

        def _pick(row: dict, *keys: str) -> str:
            for k in keys:
                val = row.get(k)
                if val is not None and str(val).strip() != "":
                    return str(val).strip()
            return ""

        def _tipo_txt(row: dict) -> str:
            raw = _pick(row, "tipo_evento", "tipo_origen", "tipo_base", "tipo", "subtipo", "evento_tipo").upper()
            if "PIGNOR" in raw: return "Pignoración"
            if "EMBARGO" in raw: return "Embargo"
            return (raw.title() if raw else "—")

        def _afavor_txt(row: dict) -> str:
            nom = _pick(row, "a_favor_de", "acreedor_nombre", "beneficiario_nombre", "tercero_nombre", "acreedor", "beneficiario")
            nif = _pick(row, "acreedor_nif", "beneficiario_nif", "tercero_nif")
            if nom and nif: return f"{nom} ({nif})"
            return nom or nif or ""

        if df_grav is None or df_grav.empty:
            df_grav_x = pd.DataFrame(columns=["Fecha","Socio titular","Tipo","A favor de","Desde","Hasta"])
        else:
            df_grav_x = df_grav.copy()
            df_grav_x["Tipo (normalizado)"] = df_grav_x.apply(lambda r: _tipo_txt(r.to_dict()), axis=1)
            df_grav_x["A favor de"]         = df_grav_x.apply(lambda r: _afavor_txt(r.to_dict()), axis=1)
            cols_g = ["fecha","socio_titular","Tipo (normalizado)","A favor de","rango_desde","rango_hasta","tipo"]
            df_grav_x = df_grav_x[[c for c in cols_g if c in df_grav_x.columns]].rename(columns={
                "fecha":"Fecha","socio_titular":"Socio titular","rango_desde":"Desde","rango_hasta":"Hasta","tipo":"Tipo (original)"
            })

        # -- Movimientos del período (con correlativo si existe)
        df_mov = _ledger_rows(company_id, date_from, date_to, event_types)
        vn_steps = _nominal_timeline(company_id)

        def _vn_row(r):
            try:
                nv = r.get("nuevo_valor_nominal")
                if pd.notna(nv) and float(nv or 0) > 0:
                    return float(nv)
            except Exception:
                pass
            return _vn_on_date(vn_steps, str(r.get("fecha") or ""))

        TYPE_SHORT = {
            "ALTA": "ALTA",
            "TRANSMISION": "TRANS",
            "AMPL_EMISION": "AMPL_EMI",
            "AMPL_VALOR": "AMPL_VAL",
            "REDENOMINACION": "REDENOM",
            "PIGNORACION": "PIGNOR",
            "CANCELA_PIGNORACION": "CANC_PIG",
        }
        def _short(t):
            t0 = (str(t) or "").upper().strip()
            return TYPE_SHORT.get(t0, (t0[:10] if t0 else ""))

        if df_mov is None or df_mov.empty:
            df_mov_x = pd.DataFrame(columns=[
                "correlativo","fecha","tipo","tipo_corto",
                "socio_transmite_nombre","socio_transmite_nif",
                "socio_adquiere_nombre","socio_adquiere_nif",
                "rango_desde","rango_hasta","participaciones","nuevo_valor_nominal","vn_vigente"
            ])
        else:
            df_mov_x = df_mov.copy()
            df_mov_x["tipo_corto"] = df_mov_x["tipo"].map(_short)
            df_mov_x["vn_vigente"] = df_mov_x.apply(_vn_row, axis=1)
            # Normalizar correlativo como "Nº asiento"
            df_mov_x = _ledger_use_correlativo(df_mov_x)

        # --------- Escribir Excel ----------
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            # Resumen
            meta = {
                "Sociedad": _company_header(company_id).get("name",""),
                "A fecha": as_of_final,
                "Periodo (desde)": date_from or "inicio",
                "Periodo (hasta)": date_to or "hoy",
                "Tipos de evento (filtro)": ", ".join(event_types or []) or "—",
                "Diligencia apertura": diligencia_apertura or "—",
                "Diligencia cierre": diligencia_cierre or "—",
            }
            df_meta = pd.DataFrame(list(meta.items()), columns=["Campo","Valor"])
            df_meta.to_excel(writer, index=False, sheet_name="Resumen")

            # Socios
            df_socios.to_excel(writer, index=False, sheet_name="Socios a fecha")
            # Cap table
            df_cap_x.to_excel(writer, index=False, sheet_name="Cap table a fecha")
            # Rangos
            df_rng.to_excel(writer, index=False, sheet_name="Rangos a fecha")
            # Gravámenes
            df_grav_x.to_excel(writer, index=False, sheet_name="Gravámenes a fecha")
            # Movimientos
            df_mov_x.to_excel(writer, index=False, sheet_name="Movimientos")

            # Formatos y auto-ancho
            wb = writer.book
            fmt_int = wb.add_format({"num_format": "#,##0"})
            fmt_pct = wb.add_format({"num_format": "0.0000"})
            fmt_money = wb.add_format({"num_format": "#,##0.00"})

            def _autowidth(ws_name: str, df_: pd.DataFrame):
                ws = writer.sheets[ws_name]
                for i, col in enumerate(df_.columns):
                    try:
                        max_len = int(df_[col].astype(str).str.len().quantile(0.9))
                    except Exception:
                        max_len = 16
                    width = max(12, min(50, max_len + 2))
                    ws.set_column(i, i, width)
                return ws

            _autowidth("Resumen", df_meta)
            _autowidth("Socios a fecha", df_socios)
            ws_cap = _autowidth("Cap table a fecha", df_cap_x)
            if "Participaciones" in df_cap_x.columns:
                idx = df_cap_x.columns.get_loc("Participaciones")
                ws_cap.set_column(idx, idx, 14, fmt_int)
            if "% (0–100)" in df_cap_x.columns:
                idx = df_cap_x.columns.get_loc("% (0–100)")
                ws_cap.set_column(idx, idx, 12, fmt_pct)
            if "Capital del socio (€)" in df_cap_x.columns:
                idx = df_cap_x.columns.get_loc("Capital del socio (€)")
                ws_cap.set_column(idx, idx, 16, fmt_money)

            _autowidth("Rangos a fecha", df_rng)
            _autowidth("Gravámenes a fecha", df_grav_x)
            ws_mov = _autowidth("Movimientos", df_mov_x)
            for qty_col in ("participaciones", "shares_delta", "n_participaciones"):
                if qty_col in df_mov_x.columns:
                    idx = df_mov_x.columns.get_loc(qty_col)
                    ws_mov.set_column(idx, idx, 14, fmt_int)
                    break
            if "vn_vigente" in df_mov_x.columns:
                idx = df_mov_x.columns.get_loc("vn_vigente")
                ws_mov.set_column(idx, idx, 12, fmt_money)

        output.seek(0)
        log.info(
            "Export LibroRegistro.xlsx company_id=%s as_of=%s from=%s to=%s types=%s",
            company_id, as_of_final, date_from, date_to, ",".join(event_types or [])
        )
        return output

    except Exception as e:
        log.error("Error exportando LibroRegistro.xlsx company_id=%s: %s", company_id, e, exc_info=True)
        raise


# ============================================================
#  PDF: Certificado de titularidad (socio)
# ============================================================
def export_partner_certificate_pdf(company_id: int, partner_id: int, as_of: Optional[str] = None) -> BytesIO:
    try:
        ensure_pdf_base_fonts()
        ref_date = as_of or datetime.now().strftime("%Y-%m-%d")

        data    = partner_position(company_id, partner_id, ref_date)
        rangos  = partner_holdings_ranges(company_id, partner_id, ref_date)
        enc     = active_encumbrances_affecting_partner(company_id, partner_id, ref_date)
        _ = last_entries_for_partner(company_id, partner_id, limit=10, as_of=ref_date)  # no usado, pero conservamos la llamada

        # Orden estable para la tabla
        if enc is not None and not enc.empty:
            enc = enc.sort_values(by=["fecha", "rango_desde", "rango_hasta"], na_position="last").reset_index(drop=True)

        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        W, H = A4
        y = H - 20 * mm
        content_width = W - 2 * MARGIN_X  # ancho útil entre márgenes

        # ---------------- Cabecera ----------------
        comp_corte = datetime.strptime(ref_date, "%Y-%m-%d").strftime("%d/%m/%Y")

        c.setTitle("Certificado de titularidad")
        c.setFont("DejaVuSans", 14)
        c.drawString(MARGIN_X, y, "Certificado de titularidad"); y -= 6 * mm

        c.setFont("DejaVuSans", 9)
        c.drawString(MARGIN_X, y, f"A fecha de certificación: {comp_corte}")
        y -= 8 * mm

        _hr(c, y); y -= SECTION_GAP

        # ---------------- Resumen del socio ----------------
        y = _section_title(c, "Resumen del socio", y)
        y = _kv(c, y, "Socio", data.get("partner_name", ""))
        y = _kv(c, y, "NIF", data.get("nif", "") or "—")
        y = _kv(c, y, "Participaciones", f"{int(data.get('shares', 0)):,}".replace(",", "."))
        y = _kv(c, y, "Porcentaje", f"{float(data.get('pct', 0.0)):.4f} %")
        y = _kv(c, y, "Clases/Series", data.get("classes") or "—")
        y -= 2 * mm

        # ---------------- Detalle de participaciones ----------------
        y = _section_title(c, "Detalle de participaciones a la fecha", y)
        c.setFont("DejaVuSans", 9.5)
        _col(c, MARGIN_X, y, "Desde")
        _col(c, MARGIN_X + 35 * mm, y, "Hasta")
        _col(c, MARGIN_X + 70 * mm, y, "Participaciones")
        y -= 4 * mm
        _hr(c, y); y -= 3 * mm
        c.setFont("DejaVuSans", 9)

        total_bloques = 0
        if rangos is None or rangos.empty:
            _col(c, MARGIN_X, y, "(Sin bloques vigentes)"); y -= CONTENT_GAP
        else:
            for _, r in rangos.iterrows():
                rd = r.get("rango_desde"); rh = r.get("rango_hasta"); part = int(r.get("participaciones") or 0)
                total_bloques += part
                _col(c, MARGIN_X, y, "" if pd.isna(rd) else str(int(rd)))
                _col(c, MARGIN_X + 35 * mm, y, "" if pd.isna(rh) else str(int(rh)))
                _col(c, MARGIN_X + 70 * mm, y, f"{part:,}".replace(",", "."))
                y -= CONTENT_GAP
                if y < 30 * mm:
                    c.showPage(); y = H - 20 * mm

        c.setFont("DejaVuSans-Oblique", 9)
        check = f"Suma de bloques: {total_bloques:,}".replace(",", ".") + \
                f"   •   Total socio: {int(data.get('shares', 0)):,}".replace(",", ".")
        _col(c, MARGIN_X, y, check); y -= CONTENT_GAP

        # ---------------- Gravámenes a la fecha ----------------
        y = _section_title(c, "Gravámenes a la fecha (pignoraciones/embargos)", y)

        c.setFont("DejaVuSans", 9)
        if enc is None or enc.empty:
            para = (
                "A la fecha de esta certificación no constan gravámenes vigentes "
                "(pignoraciones ni embargos) sobre las participaciones titularidad del socio, "
                "según el Libro Registro."
            )
            y = _draw_paragraph(c, para, MARGIN_X, y, content_width, leading=12.0)
            y -= 2 * mm
        else:
            para = (
                "A la fecha de esta certificación, constan sobre las participaciones titularidad "
                "del socio uno o varios gravámenes (pignoraciones y/o embargos) vigentes conforme a los "
                "asientos del Libro Registro, según se detalla en la tabla a continuación:"
            )
            y = _draw_paragraph(c, para, MARGIN_X, y, content_width, leading=12.0)
            y -= 2 * mm

            # Posiciones de columnas
            x_fecha = MARGIN_X
            x_tipo  = MARGIN_X + 28 * mm
            x_afav  = MARGIN_X + 60 * mm
            x_desde = MARGIN_X + 135 * mm
            x_hasta = MARGIN_X + 155 * mm

            def draw_enc_header(y0: float) -> float:
                c.setFont("DejaVuSans", 9.5)
                _col(c, x_fecha, y0, "Fecha")
                _col(c, x_tipo,  y0, "Tipo")
                _col(c, x_afav,  y0, "A favor de")
                _col(c, x_desde, y0, "Desde")
                _col(c, x_hasta, y0, "Hasta")
                y1 = y0 - 4 * mm
                _hr(c, y1)
                c.setFont("DejaVuSans", 9)
                return y1 - 3 * mm

            y = draw_enc_header(y)

            # Filas de la tabla de gravámenes
            for _, r in enc.iterrows():
                fecha = str(r.get("fecha") or "")
                tipo  = str(r.get("tipo") or "")
                nom   = (r.get("acreedor_nombre") or "").strip()
                nif   = (r.get("acreedor_nif") or "").strip()
                acre  = f"{nom} ({nif})" if nif else nom

                rd    = r.get("rango_desde"); rh = r.get("rango_hasta")
                d_txt = "" if pd.isna(rd) else str(int(rd))
                h_txt = "" if pd.isna(rh) else str(int(rh))

                _col(c, x_fecha, y, fecha)
                _col(c, x_tipo,  y, tipo)
                _col(c, x_afav,  y, acre, maxw=(x_desde - x_afav - 3 * mm))
                _col(c, x_desde, y, d_txt)
                _col(c, x_hasta, y, h_txt)
                y -= CONTENT_GAP

                if y < 30 * mm:
                    c.showPage(); y = H - 20 * mm
                    y = _section_title(c, "Gravámenes a la fecha (pignoraciones/embargos)", y)
                    y = _draw_paragraph(c, para, MARGIN_X, y, content_width, leading=12.0)
                    y -= 2 * mm
                    y = draw_enc_header(y)

        # Cierre
        c.showPage()
        c.save()
        buffer.seek(0)

        log.info("Export Certificado.pdf company_id=%s partner_id=%s as_of=%s partner='%s' shares=%s",
                 company_id, partner_id, as_of, data.get("partner_name", ""),
                 int(data.get("shares", 0)))

        return buffer

    except Exception as e:
        log.error("Error exportando Certificado.pdf company_id=%s partner_id=%s as_of=%s: %s",
                  company_id, partner_id, as_of, e, exc_info=True)
        raise


# ============================================================
#  LIBRO REGISTRO – LEGALIZABLE (PDF & Excel)
# ============================================================
def _nominal_timeline(company_id: int) -> list[tuple[str, float]]:
    """
    [(fecha ISO, nuevo_valor_nominal>0)] de events.nuevo_valor_nominal para saber VN vigente.
    """
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT fecha, nuevo_valor_nominal
            FROM events
            WHERE company_id=?
              AND nuevo_valor_nominal IS NOT NULL
              AND nuevo_valor_nominal > 0
            ORDER BY fecha, id
            """,
            conn, params=(company_id,)
        )
    if df.empty:
        return []
    out: list[tuple[str, float]] = []
    for _, r in df.iterrows():
        out.append((str(r["fecha"]), float(r["nuevo_valor_nominal"])))
    return out


def _vn_on_date(vn_steps: list[tuple[str, float]], fecha_iso: str) -> float | None:
    """Devuelve VN vigente a una fecha (lista ordenada ascendente)."""
    if not vn_steps:
        return None
    last = None
    for f, vn in vn_steps:
        if f <= fecha_iso:
            last = vn
        else:
            break
    return last


def _normalize_pct(p):
    try:
        v = float(p)
    except Exception:
        return None
    # En tu cap_table ya viene 0..100; si viniera 0..1, normalizamos.
    return v * 100.0 if v <= 1.000001 else v


def _vigentes_cap_table(company_id: int, as_of: str) -> pd.DataFrame:
    df = cap_table(company_id, as_of).copy()
    if df is None or df.empty:
        return pd.DataFrame(columns=["partner_id", "partner_name", "nif", "shares", "pct", "capital_socio"])
    if "pct" in df.columns:
        df["pct"] = df["pct"].map(_normalize_pct)
    # Filtra solo socios con saldo
    if "shares" in df.columns:
        df = df[df["shares"].fillna(0) > 0]
    # Asegura columnas esperadas
    for col in ("partner_id", "partner_name", "nif", "shares", "pct"):
        if col not in df.columns:
            df[col] = None
    if "capital_socio" not in df.columns:
        # si no viene, intenta derivarlo con VN vigente (opcional)
        vn = _vn_on_date(_nominal_timeline(company_id), as_of) or 0.0
        try:
            df["capital_socio"] = (df["shares"].fillna(0).astype(float) * float(vn)).round(2)
        except Exception:
            df["capital_socio"] = None
    return df


def _vigentes_ids_from_cap(df_cap: pd.DataFrame, company_id: int) -> list[int]:
    ids: list[int] = []
    for _, r in df_cap.iterrows():
        pid = r.get("partner_id")
        if pd.isna(pid) or pid is None:
            pid = _partner_id_by_nif_or_name(company_id, r.get("partner_name"), r.get("nif"))
        if pid is not None:
            ids.append(int(pid))
    # únicos y ordenados
    return sorted(list(dict.fromkeys(ids)))


def _encumbrances_all(company_id: int, as_of: str, ids: Iterable[int], pmap: dict[int, dict]) -> pd.DataFrame:
    """
    Devuelve un DF unificado con columnas al menos:
    ['fecha','socio_titular','tipo','a_favor_de','rango_desde','rango_hasta', (extras de tipo...)]
    Usamos active_encumbrances_affecting_partner para traer también beneficiario/subtipo.
    """
    def _pick(row: pd.Series, *keys: str) -> str:
        for k in keys:
            if k in row and pd.notna(row[k]):
                v = str(row[k]).strip()
                if v:
                    return v
        return ""

    def _compose_benef_row(r: pd.Series) -> str:
        # 1) claves “canónicas”
        nom = _pick(r, "a_favor_de", "acreedor_nombre", "beneficiario_nombre", "tercero_nombre", "acreedor", "beneficiario")
        nif = _pick(r, "acreedor_nif", "beneficiario_nif", "tercero_nif")
        if nom or nif:
            return f"{nom} ({nif})" if (nom and nif) else (nom or nif or "")

        # 2) sniff: busca columnas que contengan palabras clave
        cols = {c.lower(): c for c in r.index}
        # nombre
        name_col = next((cols[c] for c in cols if any(k in c for k in ("acreedor", "benef", "tercero")) and "nif" not in c), None)
        # nif
        nif_col  = next((cols[c] for c in cols if "nif" in c and any(k in c for k in ("acreedor", "benef", "tercero"))), None)
        nom2 = str(r[name_col]).strip() if name_col and pd.notna(r.get(name_col)) else ""
        nif2 = str(r[nif_col]).strip()  if nif_col and pd.notna(r.get(nif_col))  else ""
        if nom2 or nif2:
            return f"{nom2} ({nif2})" if (nom2 and nif2) else (nom2 or nif2)
        return ""

    frames: list[pd.DataFrame] = []

    for pid in ids:
        # IMPORTANTE: esta trae más detalle que active_encumbrances
        df = active_encumbrances_affecting_partner(company_id, pid, as_of)
        if df is None or df.empty:
            continue

        info = pmap.get(int(pid), {})
        socio_tit = f"{info.get('nombre','')} ({info.get('nif','')})".strip()

        dfx = df.copy()
        dfx["socio_titular"] = socio_tit
        dfx["a_favor_de"]    = dfx.apply(_compose_benef_row, axis=1)

        # columnas extra de tipología para normalizar “Tipo”
        extra_tipo_cols = [c for c in ["tipo_evento","subtipo","evento_tipo","tipo_base","tipo_origen"] if c in dfx.columns]

        base_cols = ["fecha", "socio_titular", "tipo", "a_favor_de", "rango_desde", "rango_hasta"]
        keep_cols = base_cols + extra_tipo_cols
        keep_cols = [c for c in keep_cols if c in dfx.columns]
        frames.append(dfx[keep_cols])

    if not frames:
        return pd.DataFrame(columns=["fecha","socio_titular","tipo","a_favor_de","rango_desde","rango_hasta"])

    out = pd.concat(frames, ignore_index=True)
    out.sort_values(by=["fecha", "socio_titular", "a_favor_de"], inplace=True, na_position="last")
    return out.reset_index(drop=True)


def export_ledger_pdf_legalizable(
    company_id: int,
    date_from: str | None,
    date_to: str | None,
    event_types: list[str] | None,
    as_of: str | None = None,
    diligencia_apertura: str | None = None,
    diligencia_cierre: str | None = None,
) -> BytesIO:
    """
    PDF legalizable (apaisado) con:
      1) Relación de socios con participaciones a la fecha
      2) Relación de participaciones a la fecha
      3) Rangos vigentes por socio a la fecha
      4) Gravámenes a la fecha
      5) Movimientos del período
    """
    ensure_pdf_base_fonts()

    comp = _company_header(company_id)
    as_of_final = as_of or date_to or datetime.now().strftime("%Y-%m-%d")

    df_cap = _vigentes_cap_table(company_id, as_of_final)
    vigentes_ids = _vigentes_ids_from_cap(df_cap, company_id)
    pmap = _partners_lookup(company_id)

    # ========= Canvas
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=landscape(A4))
    W, H = landscape(A4)
    left = 15 * mm
    right = W - 15 * mm

    def _header_block(title: str, meta_lines: list[str] | None = None) -> float:
        y = H - 12 * mm
        c.setFont("DejaVuSans-Bold", 12)
        c.drawString(left, y, title); y -= 5.5 * mm

        c.setFont("DejaVuSans", 9.5)
        c.drawString(left, y, f"Sociedad: {comp['name']}    •    CIF: {comp['cif']}"); y -= 4.2 * mm
        c.drawString(left, y, f"Domicilio: {comp['domicilio']}"); y -= 4.2 * mm
        if comp.get("fecha_constitucion"):
            c.drawString(left, y, f"Fecha constitución: {comp['fecha_constitucion']}"); y -= 4.2 * mm

        if meta_lines:
            for line in meta_lines:
                if line:
                    c.drawString(left, y, line); y -= 4.2 * mm

        c.setFont("DejaVuSans", 8.5); c.setFillColor(colors.grey)
        c.drawString(left, y, f"Emitido: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        c.setFillColor(colors.black); y -= 4.2 * mm
        c.setStrokeColor(colors.lightgrey); c.setLineWidth(0.6); c.line(left, y, right, y)
        return y - 6 * mm

    # ========= 1) Relación de socios =========
    y = _header_block(
        "Libro registro de socios – Relación de socios",
        [f"A fecha: {as_of_final}", f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
    )
    c.setFont("DejaVuSans-Bold", 9)
    cols_soc = [("#", 12*mm), ("Nombre / Razón social", 85*mm), ("NIF/CIF", 35*mm),
                ("Nacionalidad", 35*mm), ("Domicilio", right - left - (12+85+35+35)*mm)]
    x = left
    for t, w in cols_soc: c.drawString(x, y, t); x += w
    y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm

    c.setFont("DejaVuSans", 8.6)
    for pid in vigentes_ids:
        info = pmap.get(int(pid), {})
        if y < 18 * mm:
            c.showPage()
            y = _header_block(
                "Libro registro de socios – Relación de socios",
                [f"A fecha: {as_of_final}", f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
            )
            c.setFont("DejaVuSans-Bold", 9); x = left
            for t, w in cols_soc: c.drawString(x, y, t); x += w
            y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
            c.setFont("DejaVuSans", 8.6)

        x = left
        _col(c, x, y, str(pid), maxw=12*mm); x += 12*mm
        _col(c, x, y, info.get("nombre",""), maxw=85*mm); x += 85*mm
        _col(c, x, y, info.get("nif",""), maxw=35*mm); x += 35*mm
        _col(c, x, y, info.get("nacionalidad",""), maxw=35*mm); x += 35*mm
        _col(c, x, y, info.get("domicilio",""), maxw=(right-left - (12+85+35+35)*mm))
        y -= 5.2 * mm

    c.showPage()

    # ========= 2) Relación de participaciones a fecha =========
    y = _header_block(
        "Libro registro de socios – Relación de participaciones a fecha",
        [f"A fecha: {as_of_final}", f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
    )
    c.setFont("DejaVuSans-Bold", 9)
    cols_cap = [
        ("#", 12*mm), ("Socio", 88*mm), ("NIF/CIF", 35*mm),
        ("Participaciones", 35*mm), ("% (0–100)", 25*mm), ("Capital del socio (€)", 40*mm),
    ]
    x = left
    for title, width in cols_cap: c.drawString(x, y, title); x += width
    y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm

    c.setFont("DejaVuSans", 8.7)
    for _, r in df_cap.iterrows():
        if y < 18 * mm:
            c.showPage()
            y = _header_block(
                "Libro registro de socios – Relación de participaciones a fecha",
                [f"A fecha: {as_of_final}", f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
            )
            c.setFont("DejaVuSans-Bold", 9); x = left
            for title, width in cols_cap: c.drawString(x, y, title); x += width
            y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
            c.setFont("DejaVuSans", 8.7)

        pid = r.get("partner_id")
        if pd.isna(pid) or pid is None:
            pid = _partner_id_by_nif_or_name(company_id, r.get("partner_name"), r.get("nif"))
        x = left
        _col(c, x, y, "" if pid is None else str(int(pid)), maxw=12*mm); x += 12*mm
        _col(c, x, y, r.get("partner_name",""), maxw=88*mm); x += 88*mm
        _col(c, x, y, r.get("nif",""), maxw=35*mm); x += 35*mm
        c.drawRightString(x + 35*mm - 1.5*mm, y, f"{int(r.get('shares',0)):,}".replace(",", ".")); x += 35*mm
        pct = r.get("pct"); pct = ("" if pd.isna(pct) else f"{float(pct):.4f}")
        c.drawRightString(x + 25*mm - 1.5*mm, y, pct); x += 25*mm
        cap = r.get("capital_socio")
        cap_txt = "" if pd.isna(cap) or cap is None else f"{float(cap):,.2f}".replace(",", ".")
        c.drawRightString(x + 40*mm - 1.5*mm, y, cap_txt)
        y -= 5.3 * mm

    c.showPage()

    # ========= 3) Rangos vigentes por socio =========
    y = _header_block(
        "Libro registro de socios – Rangos vigentes por socio a la fecha",
        [f"A fecha: {as_of_final}"]
    )
    rows_ranges = []
    for _, r in df_cap.iterrows():
        pid = r.get("partner_id")
        if pd.isna(pid) or pid is None:
            pid = _partner_id_by_nif_or_name(company_id, r.get("partner_name"), r.get("nif"))
        if pid is None:
            continue
        rng = partner_holdings_ranges(company_id, int(pid), as_of_final)
        if rng is None or rng.empty:
            continue
        for _, rr in rng.iterrows():
            rows_ranges.append({
                "pid": int(pid),
                "socio": r.get("partner_name",""),
                "nif": r.get("nif",""),
                "desde": rr.get("rango_desde"),
                "hasta": rr.get("rango_hasta"),
                "participaciones": rr.get("participaciones"),
            })
    df_rng = pd.DataFrame(rows_ranges)

    c.setFont("DejaVuSans-Bold", 9)
    cols_rng = [("#", 12*mm), ("Socio", 88*mm), ("NIF/CIF", 35*mm),
                ("Desde", 25*mm), ("Hasta", 25*mm), ("Participaciones", 35*mm)]
    x = left
    for t, w in cols_rng: c.drawString(x, y, t); x += w
    y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
    c.setFont("DejaVuSans", 8.6)

    if df_rng.empty:
        _col(c, left, y, "(Sin rangos vigentes a la fecha)")
        y -= 6 * mm
    else:
        for _, r in df_rng.iterrows():
            if y < 18 * mm:
                c.showPage()
                y = _header_block(
                    "Libro registro de socios – Rangos vigentes por socio a la fecha",
                    [f"A fecha: {as_of_final}"]
                )
                c.setFont("DejaVuSans-Bold", 9); x = left
                for t, w in cols_rng: c.drawString(x, y, t); x += w
                y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
                c.setFont("DejaVuSans", 8.6)

            x = left
            _col(c, x, y, "" if pd.isna(r.get("pid")) else str(int(r.get("pid"))), maxw=12*mm); x += 12*mm
            _col(c, x, y, r.get("socio",""), maxw=88*mm); x += 88*mm
            _col(c, x, y, r.get("nif",""), maxw=35*mm); x += 35*mm
            c.drawRightString(x + 25*mm - 1.5*mm, y, "" if pd.isna(r.get("desde")) else str(int(r.get("desde")))); x += 25*mm
            c.drawRightString(x + 25*mm - 1.5*mm, y, "" if pd.isna(r.get("hasta")) else str(int(r.get("hasta")))); x += 25*mm
            c.drawRightString(x + 35*mm - 1.5*mm, y,
                "" if pd.isna(r.get("participaciones")) else f"{int(r.get('participaciones')):,}".replace(",", "."))
            y -= 5.2 * mm

    c.showPage()

    # ========= 4) Gravámenes a la fecha =========
    y = _header_block(
        "Libro registro de socios – Gravámenes sobre participaciones sociales",
        [f"A fecha: {as_of_final}"]
    )

    df_grav = _encumbrances_all(company_id, as_of_final, vigentes_ids, pmap)

    c.setFont("DejaVuSans-Bold", 9)
    cols_g = [
        ("Fecha", 24*mm),
        ("Socio titular", 70*mm),
        ("Tipo", 24*mm),
        ("A favor de", 80*mm),
        ("Desde", 18*mm),
        ("Hasta", 18*mm),
    ]
    x = left
    for title, width in cols_g: c.drawString(x, y, title); x += width
    y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
    c.setFont("DejaVuSans", 8.6)

    def _ensure_page_grav(y0: float) -> float:
        if y0 < 18 * mm:
            c.showPage()
            return _header_block(
                "Libro registro de socios – Gravámenes sobre participaciones sociales",
                [f"A fecha: {as_of_final}"]
            )
        return y0

    def _pick(row: dict, *keys: str) -> str:
        for k in keys:
            val = row.get(k)
            if val is not None and str(val).strip() != "":
                return str(val).strip()
        return ""

    def _tipo_txt(row: dict) -> str:
        raw = _pick(row, "tipo_evento", "tipo_origen", "tipo_base", "tipo", "subtipo", "evento_tipo").upper()
        if "PIGNOR" in raw: return "Pignoración"
        if "EMBARGO" in raw: return "Embargo"
        return (raw.title() if raw else "—")

    def _afavor_txt(row: dict) -> str:
        nom = _pick(row, "a_favor_de", "acreedor_nombre", "beneficiario_nombre", "tercero_nombre", "acreedor", "beneficiario")
        nif = _pick(row, "acreedor_nif", "beneficiario_nif", "tercero_nif")
        if nom and nif: return f"{nom} ({nif})"
        return nom or nif or ""

    def _draw_row_grav(y0: float, fila: dict) -> float:
        x = left
        _col(c, x, y0, str(fila.get("fecha","")), maxw=24*mm); x += 24*mm
        _col(c, x, y0, fila.get("socio_titular",""), maxw=70*mm); x += 70*mm
        _col(c, x, y0, _tipo_txt(fila), maxw=24*mm); x += 24*mm
        _col(c, x, y0, _afavor_txt(fila), maxw=80*mm); x += 80*mm
        dsd = "" if pd.isna(fila.get("rango_desde")) else str(int(fila.get("rango_desde")))
        hst = "" if pd.isna(fila.get("rango_hasta")) else str(int(fila.get("rango_hasta")))
        c.drawRightString(x + 18*mm - 1.5*mm, y0, dsd); x += 18*mm
        c.drawRightString(x + 18*mm - 1.5*mm, y0, hst)
        return y0 - 5.2 * mm

    if df_grav is None or df_grav.empty:
        _col(c, left, y, "(Sin gravámenes vigentes a la fecha)")
        y -= 6 * mm
    else:
        for _, row in df_grav.iterrows():
            if y < 18 * mm:
                y = _ensure_page_grav(y)
                c.setFont("DejaVuSans-Bold", 9); x = left
                for title, width in cols_g: c.drawString(x, y, title); x += width
                y -= 3.6 * mm; _hr(c, y, left, right); y -= 2.8 * mm
                c.setFont("DejaVuSans", 8.6)
            y = _draw_row_grav(y, row.to_dict())

    c.showPage()

    # ========= 5) Movimientos del período =========
    y = _header_block(
        "Libro registro de socios – Movimientos del período",
        [f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
    )

    df_mov = _ledger_rows(company_id, date_from, date_to, event_types)

    # VN vigente por fila (si no viene explícito)
    vn_steps = _nominal_timeline(company_id)
    def _vn_row(r):
        if pd.notna(r.get("nuevo_valor_nominal")) and float(r["nuevo_valor_nominal"] or 0) > 0:
            return float(r["nuevo_valor_nominal"])
        return _vn_on_date(vn_steps, str(r.get("fecha") or ""))

    if df_mov is None or df_mov.empty:
        df_mov = pd.DataFrame(columns=[
            "correlativo","fecha","tipo",
            "socio_transmite_nombre","socio_transmite_nif",
            "socio_adquiere_nombre","socio_adquiere_nif",
            "rango_desde","rango_hasta","participaciones","nuevo_valor_nominal"
        ])
    df_mov = df_mov.copy()
    df_mov["vn_vigente"] = df_mov.apply(_vn_row, axis=1)

    TYPE_SHORT = {
        "ALTA": "ALTA",
        "TRANSMISION": "TRANS",
        "AMPL_EMISION": "AMPL_EMI",
        "AMPL_VALOR": "AMPL_VAL",
        "REDENOMINACION": "REDENOM",
        "PIGNORACION": "PIGNOR",
        "CANCELA_PIGNORACION": "CANC_PIG",
    }
    TYPE_DESC = {
        "ALTA": "Alta de socio / primera anotación",
        "TRANSMISION": "Transmisión de participaciones entre socios/terceros",
        "AMPL_EMISION": "Ampliación de capital por emisión de nuevas participaciones",
        "AMPL_VALOR": "Ampliación mediante aumento del valor nominal",
        "REDENOMINACION": "Cambio del valor nominal",
        "PIGNORACION": "Constitución de gravamen (pignoración/embargo)",
        "CANCELA_PIGNORACION": "Cancelación total o parcial de pignoración/embargo",
    }
    def _short(t):
        t0 = (str(t) or "").upper().strip()
        return TYPE_SHORT.get(t0, (t0[:10] if t0 else ""))

    df_mov["tipo_corto"] = df_mov["tipo"].map(_short)

    # ---- Config tabla
    FONT        = "DejaVuSans"
    FONT_BOLD   = "DejaVuSans-Bold"
    SIZE_HDR    = 8.9
    SIZE_TXT    = 8.4
    LINE_H      = 4.8 * mm
    PAD_Y       = 1.2 * mm
    GUT         = 2.2 * mm

    COLS = [
        ("Orden",   12),
        ("Fecha",   20),
        ("Tipo",    30),
        ("Transmite (Nombre / NIF)", 60),
        ("Adquiere (Nombre / NIF)",  60),
        ("Desde",   16),
        ("Hasta",   16),
        ("# Parts.",18),
        ("VN (€)",  18),
    ]

    x = left
    COL_X, COL_W = [], []
    for _, w in COLS:
        COL_X.append(x)
        COL_W.append(w * mm)
        x += w * mm + GUT

    def draw_mov_header(y0: float) -> float:
        c.setFont(FONT_BOLD, SIZE_HDR)
        for (title, _), x0 in zip(COLS, COL_X):
            c.drawString(x0, y0, title)
        y1 = y0 - 3.6 * mm
        _hr(c, y1, left, right)
        return y1 - 2.8 * mm

    def wrap(txt: str, max_w_px: float, max_lines: int = 3) -> list[str]:
        c.setFont(FONT, SIZE_TXT)
        t = ("" if txt is None else str(txt)).strip()
        if not t: return [""]
        words, lines, cur = t.split(), [], ""
        for w in words:
            trial = (cur + " " + w).strip()
            if c.stringWidth(trial, FONT, SIZE_TXT) <= max_w_px:
                cur = trial
            else:
                if cur: lines.append(cur)
                cur = w
            if len(lines) >= max_lines: break
        if len(lines) >= max_lines:
            last = (cur if cur else "").strip()
            while c.stringWidth(last + " …", FONT, SIZE_TXT) > max_w_px and len(last) > 3:
                last = last[:-1]
            if lines: lines[-1] = lines[-1] + " …"
            else: lines = [last + " …"]
            return lines
        if cur: lines.append(cur)
        return lines or [""]

    def ensure_page(y0: float) -> float:
        if y0 < (18 * mm + LINE_H + 2 * PAD_Y):
            c.showPage()
            y1 = _header_block(
                "Libro registro de socios – Movimientos del período",
                [f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
            )
            return draw_mov_header(y1)
        return y0

    y = draw_mov_header(y)
    c.setFont(FONT, SIZE_TXT)

    for _, r in df_mov.iterrows():
        orden = "" if pd.isna(r.get("correlativo")) else str(int(r.get("correlativo")))
        fecha = str(r.get("fecha") or "")
        tipo  = str(r.get("tipo_corto") or "")

        st_txt = " / ".join([s for s in [r.get("socio_transmite_nombre",""), r.get("socio_transmite_nif","")] if s])
        sa_txt = " / ".join([s for s in [r.get("socio_adquiere_nombre",""), r.get("socio_adquiere_nif","")] if s])

        dsd = "" if pd.isna(r.get("rango_desde")) else str(int(r.get("rango_desde")))
        hst = "" if pd.isna(r.get("rango_hasta")) else str(int(r.get("rango_hasta")))
        npp = "" if pd.isna(r.get("participaciones")) else f"{int(r.get('participaciones')):,}".replace(",", ".")
        vn  = r.get("vn_vigente")
        vn_txt = None if (vn in (None, float("nan"))) else f"{float(vn):,.2f}".replace(",", ".")

        w_tipo = wrap(tipo, COL_W[2] - 2.5 * mm)
        w_st   = wrap(st_txt, COL_W[3] - 2.5 * mm)
        w_sa   = wrap(sa_txt, COL_W[4] - 2.5 * mm)
        lines  = max(len(w_tipo), len(w_st), len(w_sa), 1)
        row_h  = lines * LINE_H + 2 * PAD_Y

        if y - row_h < 18 * mm:
            y = ensure_page(y)

        y_top = y
        c.drawRightString(COL_X[0] + COL_W[0] - 1.5 * mm, y_top, orden)
        c.drawString(COL_X[1], y_top, fecha)

        for i in range(lines):
            yy = y_top - PAD_Y - i * LINE_H
            _col(c, COL_X[2], yy, w_tipo[i] if i < len(w_tipo) else "", SIZE_TXT, maxw=COL_W[2] - 2.5 * mm)
            _col(c, COL_X[3], yy, w_st[i]   if i < len(w_st)   else "", SIZE_TXT, maxw=COL_W[3] - 2.5 * mm)
            _col(c, COL_X[4], yy, w_sa[i]   if i < len(w_sa)   else "", SIZE_TXT, maxw=COL_W[4] - 2.5 * mm)

        yy0 = y_top - PAD_Y
        c.drawRightString(COL_X[5] + COL_W[5] - 1.5*mm, yy0, dsd)
        c.drawRightString(COL_X[6] + COL_W[6] - 1.5*mm, yy0, hst)
        c.drawRightString(COL_X[7] + COL_W[7] - 1.5*mm, yy0, npp)
        if vn_txt is None or vn_txt.strip() == "":
            c.drawCentredString(COL_X[8] + COL_W[8] / 2.0, yy0, "–")
        else:
            c.drawRightString(COL_X[8] + COL_W[8] - 1.5*mm, yy0, vn_txt)

        y = y - row_h
        c.setStrokeColor(colors.whitesmoke); c.setLineWidth(0.4)
        c.line(left, y + 0.9 * mm, right, y + 0.9 * mm)
        c.setStrokeColor(colors.black)

    # ======= Leyenda =======
    try:
        tipos_presentes = sorted([str(x) for x in df_mov["tipo"].dropna().unique().tolist()])
    except Exception:
        tipos_presentes = []

    def _legend_header(y0: float) -> float:
        c.setFont("DejaVuSans-Bold", 9)
        c.drawString(left, y0, "Leyenda")
        y0 -= 3.0 * mm
        _hr(c, y0, left, right)
        return y0 - 2.4 * mm

    need = 30 * mm
    if y < (18 * mm + need):
        c.showPage()
        y = _header_block(
            "Libro registro de socios – Movimientos del período",
            [f"Periodo: {date_from or 'inicio'} → {date_to or 'hoy'}"]
        )

    y = _legend_header(y)
    c.setFont("DejaVuSans", 8)
    c.drawString(left, y, "VN (€): 'nan' indica que en ese asiento no hubo cambio de valor nominal (se mantiene el vigente).")
    y -= 4.2 * mm

    if tipos_presentes:
        pairs = []
        for t in tipos_presentes:
            t_up = (t or "").upper()
            short = TYPE_SHORT.get(t_up, t_up)
            desc  = TYPE_DESC.get(t_up, "Asiento según estatutos u operación registrada.")
            pairs.append(f"{short} = {desc}")
        texto = "Tipos de evento en el periodo: " + "; ".join(pairs) + "."
        y = _draw_paragraph(c, texto, left, y, max_width=(right - left), leading=11.0, font="DejaVuSans", font_size=8)

    c.showPage()
    c.save()
    buf.seek(0)

    log.info("Export Libro legalizable PDF company_id=%s from=%s to=%s as_of=%s types=%s",
             company_id, date_from, date_to, as_of_final, ",".join(event_types or []))
    return buf

# === PDF: Certificado histórico (trayectoria del socio) ===
# === Sustituye íntegramente la función por esta ===
def export_partner_history_pdf(
    company_id: int,
    partner_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
    max_rows: int = 500,
) -> BytesIO:
    """
    Certificado histórico del socio (A4 vertical):
    - Fuente de datos: movements(company_id, date_from, date_to, event_types=None)
      filtrado por filas donde el socio participa (transmite o adquiere).
    - Columnas: Fecha · Tipo · Nº · RD–RH · # Parts. · VN (€) · Contraparte
    """
    ensure_pdf_base_fonts()
    from_date = date_from or "0001-01-01"
    to_date = date_to or datetime.now().strftime("%Y-%m-%d")

    # -------- Datos base
    comp = _company_header(company_id)
    pos  = partner_position(company_id, partner_id, to_date) or {}

    # 1) Trae todos los movimientos del periodo y filtra por socio afectado
    df = movements(company_id, from_date, to_date, event_types=None)
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "id","correlativo","fecha","tipo",
            "socio_transmite","socio_adquiere",
            "rango_desde","rango_hasta","n_participaciones",
            "nuevo_valor_nominal","documento","observaciones"
        ])
    else:
        df = df.copy()

    # Normaliza columnas de IDs (si no existen, toma las "planas")
    if "socio_transmite_id" not in df.columns:
        df["socio_transmite_id"] = df["socio_transmite"] if "socio_transmite" in df.columns else None
    if "socio_adquiere_id" not in df.columns:
        df["socio_adquiere_id"] = df["socio_adquiere"] if "socio_adquiere" in df.columns else None

    # Filtra filas donde participa el socio
    def _eq_pid(x) -> bool:
        try:
            return str(int(x)) == str(int(partner_id))
        except Exception:
            return False

    df = df[(df["socio_transmite_id"].apply(_eq_pid)) | (df["socio_adquiere_id"].apply(_eq_pid))]

    # Orden y límite
    if not df.empty:
        if "fecha" in df.columns and "id" in df.columns:
            df = df.sort_values(by=["fecha", "id"], na_position="last")
        df = df.head(max_rows)

    # Enriquecer nombres de contrapartes si no vienen
    pmap = _partners_lookup(company_id)
    if "socio_transmite_nombre" not in df.columns:
        df["socio_transmite_nombre"] = df["socio_transmite_id"].map(
            lambda x: (pmap.get(int(x), {}).get("nombre", "") if pd.notna(x) else "")
        )
    if "socio_adquiere_nombre" not in df.columns:
        df["socio_adquiere_nombre"] = df["socio_adquiere_id"].map(
            lambda x: (pmap.get(int(x), {}).get("nombre", "") if pd.notna(x) else "")
        )

    # Asegura columnas esperadas para el render
    for col in ("correlativo","rango_desde","rango_hasta","n_participaciones","nuevo_valor_nominal"):
        if col not in df.columns:
            df[col] = None

    # 2) Valores nominales – línea de tiempo
    vn_steps = _nominal_timeline(company_id)

    def _vn_vigente(r: pd.Series):
        nv = r.get("nuevo_valor_nominal")
        try:
            if nv is not None and not (isinstance(nv, float) and pd.isna(nv)) and float(nv) > 0:
                return float(nv)
        except Exception:
            pass
        return _vn_on_date(vn_steps, str(r.get("fecha") or ""))

    df["vn_vigente"] = df.apply(_vn_vigente, axis=1)

    # 3) Texto de contraparte (nombres, sin IDs)
    def _counterparty(r: pd.Series) -> str:
        st_name = str(r.get("socio_transmite_nombre") or "").strip()
        sa_name = str(r.get("socio_adquiere_nombre") or "").strip()
        st_id   = r.get("socio_transmite_id")
        sa_id   = r.get("socio_adquiere_id")

        # Si el socio es una de las partes, mostrar el nombre de la otra (si existe).
        if _eq_pid(st_id):
            return sa_name
        if _eq_pid(sa_id):
            return st_name
        # Como fallback, muestra ambos nombres (si existen)
        parts = [x for x in (st_name, sa_name) if x]
        return " / ".join(parts)

    # 4) Cálculo robusto de RD–RH y #Parts.
    def _range_txt(r: pd.Series) -> str:
        rd, rh = r.get("rango_desde"), r.get("rango_hasta")
        def itxt(x):
            try:    return str(int(x))
            except: return ""
        rd_txt, rh_txt = itxt(rd), itxt(rh)
        return f"{rd_txt}–{rh_txt}".strip("–")

    def _qty_txt(r: pd.Series) -> str:
        n = r.get("n_participaciones")
        try:
            if n is not None and not (isinstance(n, float) and pd.isna(n)):
                return f"{int(n):,}".replace(",", ".")
        except Exception:
            pass
        # si no viene, intenta RD–RH
        rd, rh = r.get("rango_desde"), r.get("rango_hasta")
        try:
            if rd is not None and rh is not None and not (pd.isna(rd) or pd.isna(rh)):
                val = int(rh) - int(rd) + 1
                if val > 0:
                    return f"{val:,}".replace(",", ".")
        except Exception:
            pass
        return ""

    # ================== Render PDF (A4 vertical) ==================
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    MARG_X = 18 * mm
    MARG_Y = 18 * mm
    content_w = W - 2 * MARG_X
    y = H - MARG_Y

    # Cabecera
    c.setTitle("Certificado histórico del socio")
    c.setFont("DejaVuSans-Bold", 13)
    c.drawString(MARG_X, y, "Certificado histórico del socio"); y -= 6 * mm
    c.setFont("DejaVuSans", 9.5)
    c.drawString(MARG_X, y, f"Sociedad: {comp.get('name','')}  •  CIF: {comp.get('cif','')}")
    y -= 4.2 * mm
    c.drawString(MARG_X, y, f"Periodo: {from_date} → {to_date}")
    y -= 7 * mm
    _hr(c, y); y -= 6 * mm

    # Resumen socio
    y = _section_title(c, "Resumen a la fecha de corte", y)
    y = _kv(c, y, "Socio", pos.get("partner_name",""))
    y = _kv(c, y, "NIF", pos.get("nif","") or "—")
    y = _kv(c, y, "Participaciones a fecha", f"{int(pos.get('shares',0)):,}".replace(",", "."))
    y = _kv(c, y, "Porcentaje a fecha", f"{float(pos.get('pct',0.0)):.4f} %")
    y -= 2 * mm

    # Tabla
    y = _section_title(c, "Asientos que afectan al socio", y)

    # Distribución de columnas
    c.setFont("DejaVuSans-Bold", 9.2)
    w_fecha = 24 * mm
    w_tipo  = 30 * mm
    w_nro   = 12 * mm
    w_rng   = 24 * mm
    w_qty   = 18 * mm
    w_vn    = 20 * mm
    gap     = 2 * mm

    x_fecha = MARG_X
    x_tipo  = x_fecha + w_fecha + gap
    x_nro   = x_tipo  + w_tipo  + gap
    x_rng   = x_nro   + w_nro   + gap
    x_qty   = x_rng   + w_rng   + gap
    x_vn    = x_qty   + w_qty   + gap
    x_cp    = x_vn    + w_vn    + gap
    w_cp    = (MARG_X + content_w) - x_cp

    def _wrap(text: str, max_w: float, font="DejaVuSans", size=8.6, max_lines: int = 2) -> list[str]:
        t = (text or "").strip()
        if not t:
            return [""]
        words, lines, cur = t.split(), [], ""
        for w in words:
            trial = (cur + " " + w).strip()
            if pdfmetrics.stringWidth(trial, font, size) <= max_w:
                cur = trial
            else:
                if cur:
                    lines.append(cur)
                cur = w
            if len(lines) >= max_lines:
                break
        if cur:
            lines.append(cur)
        if len(lines) > max_lines:
            lines = lines[:max_lines]
        if len(lines[-1]) > 3:
            while pdfmetrics.stringWidth(lines[-1] + " …", font, size) > max_w and len(lines[-1]) > 3:
                lines[-1] = lines[-1][:-1]
            lines[-1] += " …"
        return lines

    def draw_header(yy: float) -> float:
        _col(c, x_fecha, yy, "Fecha")
        _col(c, x_tipo,  yy, "Tipo")
        _col(c, x_nro,   yy, "Nº")
        _col(c, x_rng,   yy, "RD–RH")
        _col(c, x_qty,   yy, "# Parts.")
        _col(c, x_vn,    yy, "VN (€)")
        _col(c, x_cp,    yy, "Contraparte")
        y1 = yy - 4 * mm
        _hr(c, y1)
        return y1 - 3 * mm

    y = draw_header(y)
    c.setFont("DejaVuSans", 8.6)

    if df.empty:
        _col(c, MARG_X, y, "(No hay asientos en el periodo)")
        y -= 6 * mm
    else:
        for _, r in df.iterrows():
            # saltar de página si no cabe la fila
            if y < (MARG_Y + 22 * mm):
                c.showPage()
                y = H - MARG_Y
                c.setFont("DejaVuSans-Bold", 13)
                _col(c, MARG_X, y, "Certificado histórico del socio (continuación)"); y -= 6 * mm
                _hr(c, y); y -= 6 * mm
                c.setFont("DejaVuSans-Bold", 9.2)
                y = draw_header(y)
                c.setFont("DejaVuSans", 8.6)

            fecha = str(r.get("fecha") or "")
            tipo  = str(r.get("tipo") or "")
            # Nº asiento (si existe)
            nro = ""
            try:
                v = r.get("correlativo")
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    nro = str(int(v))
            except Exception:
                nro = ""

            rng_txt = _range_txt(r)
            qty_txt = _qty_txt(r)

            vn = r.get("vn_vigente")
            vn_txt = ""
            try:
                if vn is not None and not (isinstance(vn, float) and pd.isna(vn)):
                    vn_txt = f"{float(vn):,.2f}".replace(",", ".")
            except Exception:
                vn_txt = ""

            cp_txt = _counterparty(r)
            cp_lines = _wrap(cp_txt, w_cp)

            # Dibujo de fila (multi-línea en "Contraparte")
            line_h = 4.8 * mm
            row_h = max(line_h * len(cp_lines), line_h)

            _col(c, x_fecha, y, fecha)
            _col(c, x_tipo,  y, tipo)
            c.drawRightString(x_nro + w_nro - 1.5*mm, y, nro or "")
            _col(c, x_rng,   y, rng_txt or "")
            c.drawRightString(x_qty + w_qty - 1.5*mm, y, qty_txt or "")
            c.drawRightString(x_vn  + w_vn  - 1.5*mm, y, vn_txt or "–")

            yy = y
            for i, ln in enumerate(cp_lines):
                _col(c, x_cp, yy, ln, maxw=w_cp)
                yy -= line_h

            y = y - row_h
            c.setStrokeColor(colors.whitesmoke); c.setLineWidth(0.4)
            c.line(MARG_X, y + 0.9 * mm, MARG_X + content_w, y + 0.9 * mm)
            c.setStrokeColor(colors.black)

    # Nota legal
    y -= 2 * mm
    txt = (
        "Este certificado se emite a efectos informativos, reflejando los asientos del Libro Registro "
        "que afectaron al socio en el periodo indicado. El valor nominal mostrado corresponde al vigente "
        "en cada fecha de asiento, salvo que en dicho asiento se hubiera modificado explícitamente."
    )
    y = _draw_paragraph(c, txt, MARG_X, y, content_w, leading=11.0, font="DejaVuSans", font_size=8)

    c.showPage(); c.save()
    buf.seek(0)
    log.info("Export CertificadoHistorico.pdf company_id=%s partner_id=%s from=%s to=%s rows=%s",
             company_id, partner_id, from_date, to_date, len(df))
    return buf