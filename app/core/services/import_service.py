# app/core/services/import_service.py
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Optional, Any

import pandas as pd

from app.infra.db import get_connection
from app.core.enums import EVENT_TYPES, normalize_event_type
from app.core.services.events_service import create_event_generic


# ============================================================
# Tipos & resultados
# ============================================================

@dataclass
class DryRunRow:
    rownum: int
    data: dict[str, Any]
    normalized: dict[str, Any] | None
    errors: list[str]


@dataclass
class DryRunReport:
    kind: str                 # "partners" | "events"
    total_rows: int
    ok_rows: int
    error_rows: int
    errors: list[str]         # errores globales (encabezados, etc.)
    rows: list[DryRunRow]     # detalle fila a fila (primeras N filas en UI)


@dataclass
class CommitSummary:
    kind: str
    inserted: int
    updated: int
    errors: list[str]


# ============================================================
# Plantillas CSV
# ============================================================

def get_csv_template(kind: str) -> bytes:
    """
    Devuelve un CSV (bytes UTF-8) con encabezados + 2 filas de ejemplo.
    """
    kind = (kind or "").strip().lower()
    buf = io.StringIO()
    wr = csv.writer(buf)

    if kind == "partners":
        wr.writerow(["nombre", "nif", "nacionalidad", "domicilio", "partner_no"])
        wr.writerow(["Alice S.L.", "B12345678", "España", "C/ Mayor 1, Madrid", "1"])
        wr.writerow(["Bob S.A.", "A11111111", "España", "Av. Libertad 10, Bilbao", "2"])

    elif kind == "events":
        wr.writerow([
            "fecha", "tipo",
            "socio_transmite_nif", "socio_adquiere_nif",
            "rango_desde", "rango_hasta",
            "n_participaciones", "nuevo_valor_nominal",
            "documento", "observaciones",
        ])
        wr.writerow(["2024-01-10", "ALTA", "", "B12345678", "1", "100", "", "", "", "Ampliación"])
        wr.writerow(["2024-02-05", "TRANSMISION", "B12345678", "A11111111", "1", "50", "", "", "", "Traspaso parcial"])

    else:
        raise ValueError("Tipo de plantilla desconocido. Usa 'partners' o 'events'.")

    return buf.getvalue().encode("utf-8")


# ============================================================
# Helpers
# ============================================================

def _read_csv_to_df(file: bytes | io.BytesIO) -> pd.DataFrame:
    """
    Lee CSV (UTF-8, separador coma) a DataFrame sin transformar tipos.
    """
    if isinstance(file, (bytes, bytearray)):
        bio = io.BytesIO(file)
    else:
        bio = file
    bio.seek(0)
    return pd.read_csv(bio, dtype=str).fillna("")


def _partner_id_by_nif(nif: str, company_id: int) -> Optional[int]:
    if not nif:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM partners WHERE company_id=? AND nif=? LIMIT 1",
            (company_id, nif.strip()),
        ).fetchone()
    return (int(row[0]) if row else None)


def _ensure_event_type(t: str) -> tuple[str | None, list[str]]:
    """
    Normaliza el tipo y valida contra EVENT_TYPES (permitimos 'OTRO').
    """
    errs: list[str] = []
    t_norm = normalize_event_type(t) if t is not None else None
    if t_norm and (t_norm not in EVENT_TYPES) and t_norm != "OTRO":
        errs.append(f"Tipo no reconocido: {t}")
    return t_norm, errs


# ============================================================
# Validaciones por tipo de import
# ============================================================

def _validate_partners_df(df: pd.DataFrame, company_id: int) -> list[DryRunRow]:
    """
    Reglas:
      - nombre (obligatorio), NIF (recomendado para idempotencia)
      - partner_no opcional (se sobreescribe)
      - Idempotencia: si existe NIF -> UPDATE, si no -> INSERT
    """
    required = ["nombre"]
    for col in required:
        if col not in df.columns:
            return [DryRunRow(0, {}, None, [f"Falta la columna obligatoria '{col}'."])]

    rows: list[DryRunRow] = []
    for idx, raw in df.iterrows():
        i = int(idx) + 2  # +2 por encabezado y base 1
        data = {c: str(raw.get(c, "")).strip() for c in df.columns}
        errs: list[str] = []

        nombre = data.get("nombre", "").strip()
        nif = data.get("nif", "").strip()
        nacionalidad = data.get("nacionalidad", "").strip() or None
        domicilio = data.get("domicilio", "").strip() or None
        partner_no = data.get("partner_no", "").strip()
        partner_no_val: Optional[int] = None
        if partner_no:
            try:
                partner_no_val = int(partner_no)
                if partner_no_val < 0:
                    raise ValueError()
            except Exception:
                errs.append("partner_no debe ser entero ≥ 0.")

        if not nombre:
            errs.append("El campo 'nombre' es obligatorio.")

        # Componer normalized
        normalized = {
            "nombre": nombre,
            "nif": nif or None,
            "nacionalidad": nacionalidad,
            "domicilio": domicilio,
            "partner_no": partner_no_val,
        }

        rows.append(DryRunRow(i, data, (None if errs else normalized), errs))

    return rows


def _validate_events_df(df: pd.DataFrame, company_id: int) -> list[DryRunRow]:
    """
    Reglas básicas MVP:
      - fecha obligatoria (YYYY-MM-DD)
      - tipo válido (EVENT_TYPES u OTRO)
      - si hay rangos -> rango_desde <= rango_hasta
      - socio_* por NIF si vienen (se traducen a IDs)
      - AMPL_VALOR / RED_VALOR requieren nuevo_valor_nominal > 0
    """
    needed = [
        "fecha", "tipo",
        "socio_transmite_nif", "socio_adquiere_nif",
        "rango_desde", "rango_hasta",
        "n_participaciones", "nuevo_valor_nominal",
        "documento", "observaciones",
    ]
    for col in needed:
        if col not in df.columns:
            return [DryRunRow(0, {}, None, [f"Falta la columna '{col}'."])]

    rows: list[DryRunRow] = []
    for idx, raw in df.iterrows():
        i = int(idx) + 2
        data = {c: str(raw.get(c, "")).strip() for c in df.columns}
        errs: list[str] = []

        # fecha
        fecha = data.get("fecha", "")
        if not fecha:
            errs.append("fecha es obligatoria (YYYY-MM-DD).")
        else:
            # validación simple de formato
            ok_fmt = True
            if len(fecha) != 10 or fecha[4] != "-" or fecha[7] != "-":
                ok_fmt = False
            if not ok_fmt:
                errs.append("fecha inválida. Usa YYYY-MM-DD.")

        # tipo
        tipo_norm, t_errs = _ensure_event_type(data.get("tipo", ""))
        errs.extend(t_errs or [])
        tipo_norm = tipo_norm or data.get("tipo", "").upper()

        # socios por NIF
        st_nif = data.get("socio_transmite_nif") or ""
        sa_nif = data.get("socio_adquiere_nif") or ""
        st_id = _partner_id_by_nif(st_nif, company_id) if st_nif else None
        sa_id = _partner_id_by_nif(sa_nif, company_id) if sa_nif else None

        if st_nif and st_id is None:
            errs.append(f"No existe socio con NIF '{st_nif}' (transmite).")
        if sa_nif and sa_id is None:
            errs.append(f"No existe socio con NIF '{sa_nif}' (adquiere).")

        # rangos
        rd = data.get("rango_desde") or ""
        rh = data.get("rango_hasta") or ""
        rd_val = rh_val = None
        if rd:
            try:
                rd_val = int(float(rd))
                if rd_val <= 0:
                    raise ValueError()
            except Exception:
                errs.append("rango_desde debe ser entero > 0.")
        if rh:
            try:
                rh_val = int(float(rh))
                if rh_val <= 0:
                    raise ValueError()
            except Exception:
                errs.append("rango_hasta debe ser entero > 0.")
        if (rd_val is not None) and (rh_val is not None) and (rh_val < rd_val):
            errs.append("rango_hasta no puede ser menor que rango_desde.")

        # nº participaciones
        np_txt = data.get("n_participaciones") or ""
        np_val: Optional[int] = None
        if np_txt:
            try:
                np_val = int(float(np_txt))
                if np_val < 0:
                    raise ValueError()
            except Exception:
                errs.append("n_participaciones debe ser entero ≥ 0.")

        # valor nominal
        nvn_txt = data.get("nuevo_valor_nominal") or ""
        nvn_val: Optional[float] = None
        if nvn_txt:
            try:
                nvn_val = float(nvn_txt)
                if nvn_val < 0:
                    raise ValueError()
            except Exception:
                errs.append("nuevo_valor_nominal debe ser numérico ≥ 0.")

        if tipo_norm in ("AMPL_VALOR", "RED_VALOR") and (not nvn_val or nvn_val <= 0):
            errs.append(f"{tipo_norm} requiere nuevo_valor_nominal > 0.")

        normalized = {
            "company_id": company_id,
            "fecha": fecha,
            "tipo": tipo_norm,
            "socio_transmite": st_id,
            "socio_adquiere": sa_id,
            "rango_desde": rd_val,
            "rango_hasta": rh_val,
            "n_participaciones": np_val,
            "nuevo_valor_nominal": nvn_val,
            "documento": (data.get("documento") or None),
            "observaciones": (data.get("observaciones") or None),
        }

        rows.append(DryRunRow(i, data, (None if errs else normalized), errs))

    return rows


# ============================================================
# API pública
# ============================================================

def dry_run(kind: str, company_id: int, file: bytes | io.BytesIO) -> DryRunReport:
    """
    No escribe en BD. Devuelve un informe por fila con normalización/errores.
    """
    kind = (kind or "").strip().lower()
    df = _read_csv_to_df(file)

    # Validar por tipo
    if kind == "partners":
        rows = _validate_partners_df(df, company_id)
    elif kind == "events":
        rows = _validate_events_df(df, company_id)
    else:
        return DryRunReport(kind, 0, 0, 0, [f"Tipo desconocido '{kind}'."], [])

    total = len(rows)
    ok = sum(1 for r in rows if r.errors == [])
    err = total - ok

    return DryRunReport(
        kind=kind,
        total_rows=total,
        ok_rows=ok,
        error_rows=err,
        errors=[],            # errores globales (si hubiera)
        rows=rows
    )


def commit(kind: str, company_id: int, rows: list[dict]) -> CommitSummary:
    """
    Escribe en BD de forma transaccional. 'rows' debe venir ya normalizado (p.ej. de dry_run).
    - partners: upsert por NIF (si NIF vacío -> INSERT siempre)
    - events: inserta vía create_event_generic(...)
    """
    kind = (kind or "").strip().lower()
    errors: list[str] = []
    inserted = 0
    updated = 0

    if kind == "partners":
        with get_connection() as conn:
            try:
                conn.execute("BEGIN")
                for r in rows:
                    nombre = r.get("nombre")
                    nif = r.get("nif")
                    nacionalidad = r.get("nacionalidad")
                    domicilio = r.get("domicilio")
                    partner_no = r.get("partner_no")

                    if nif:
                        row = conn.execute(
                            "SELECT id FROM partners WHERE company_id=? AND nif=? LIMIT 1",
                            (company_id, nif)
                        ).fetchone()
                        if row:
                            # UPDATE
                            conn.execute(
                                """
                                UPDATE partners
                                SET nombre=?, nacionalidad=?, domicilio=?, partner_no=?
                                WHERE id=? AND company_id=?
                                """,
                                (nombre, nacionalidad, domicilio, partner_no, int(row[0]), company_id)
                            )
                            updated += 1
                        else:
                            # INSERT
                            conn.execute(
                                """
                                INSERT INTO partners(company_id, nombre, nif, nacionalidad, domicilio, partner_no)
                                VALUES (?,?,?,?,?,?)
                                """,
                                (company_id, nombre, nif, nacionalidad, domicilio, partner_no)
                            )
                            inserted += 1
                    else:
                        # Sin NIF: INSERT siempre
                        conn.execute(
                            """
                            INSERT INTO partners(company_id, nombre, nif, nacionalidad, domicilio, partner_no)
                            VALUES (?,?,?,?,?,?)
                            """,
                            (company_id, nombre, None, nacionalidad, domicilio, partner_no)
                        )
                        inserted += 1

                conn.commit()
            except Exception as e:
                conn.rollback()
                errors.append(str(e))

    elif kind == "events":
        # Insertamos uno a uno; si falla, abortamos todo.
        with get_connection() as conn:
            try:
                conn.execute("BEGIN")
                for r in rows:
                    # Todos los campos en r están normalizados por dry_run
                    create_event_generic(**r)
                    inserted += 1
                conn.commit()
            except Exception as e:
                conn.rollback()
                errors.append(str(e))
    else:
        errors.append(f"Tipo desconocido '{kind}'.")

    return CommitSummary(kind=kind, inserted=inserted, updated=updated, errors=errors)