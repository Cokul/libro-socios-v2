# app/ui/pages/imports.py
from __future__ import annotations
import io
import logging
from typing import List, Dict, Any

import pandas as pd
import streamlit as st

# Usamos SOLO commit del backend (tu lógica actual)
from app.core.services.import_service import commit  # type: ignore

log = logging.getLogger(__name__)
MAX_PREVIEW_ROWS = 200

# ------------------------
# Esquemas “oficiales”
# ------------------------
PARTNERS_COLS = [
    "nombre",
    "nif",
    "domicilio",
    "nacionalidad",
    "fecha_nacimiento_constitucion",
    "partner_no",
]

EVENTS_COLS = [
    "fecha",
    "tipo",
    "socio_transmite",
    "socio_adquiere",
    "rango_desde",
    "rango_hasta",
    "nuevo_valor_nominal",
    "documento",
    "observaciones",
]

# Alias flexibles para normalizar encabezados habituales
HEADER_ALIASES = {
    # partners
    "razon social": "nombre",
    "razón social": "nombre",
    "nombre": "nombre",
    "nif": "nif",
    "nie": "nif",
    "cif": "nif",
    "domicilio": "domicilio",
    "direccion": "domicilio",
    "dirección": "domicilio",
    "nacionalidad": "nacionalidad",
    "fecha_nacimiento": "fecha_nacimiento_constitucion",
    "fecha nacimiento": "fecha_nacimiento_constitucion",
    "fecha_constitucion": "fecha_nacimiento_constitucion",
    "fecha constitucion": "fecha_nacimiento_constitucion",
    "fecha_nacimiento_constitucion": "fecha_nacimiento_constitucion",
    "partner_no": "partner_no",
    "nº socio": "partner_no",
    "no socio": "partner_no",
    "num socio": "partner_no",

    # events
    "fecha": "fecha",
    "tipo_evento": "tipo",
    "tipo": "tipo",
    "socio transmite": "socio_transmite",
    "socio_transmite": "socio_transmite",
    "transmite": "socio_transmite",
    "socio adquiere": "socio_adquiere",
    "socio_adquiere": "socio_adquiere",
    "adquiere": "socio_adquiere",
    "rango desde": "rango_desde",
    "rango_desde": "rango_desde",
    "desde": "rango_desde",
    "rango hasta": "rango_hasta",
    "rango_hasta": "rango_hasta",
    "hasta": "rango_hasta",
    "nuevo_valor_nominal": "nuevo_valor_nominal",
    "valor nominal": "nuevo_valor_nominal",
    "documento": "documento",
    "observaciones": "observaciones",
}

# ------------------------
# Utilidades generales
# ------------------------
def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("\u00A0", " ")

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza y mapea con alias
    out_cols: List[str] = []
    for c in df.columns:
        k = _norm(str(c))
        out_cols.append(HEADER_ALIASES.get(k, k))
    d = df.copy()
    d.columns = out_cols
    return d

def _read_any(upload) -> pd.DataFrame:
    """Lee XLSX o CSV de forma robusta (todo como string)."""
    raw = upload.getvalue()
    name = (upload.name or "").lower()

    if name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(raw), dtype=str)
    else:
        # CSV: intenta UTF-8 y luego latin-1
        try:
            df = pd.read_csv(io.BytesIO(raw), dtype=str, engine="python", keep_default_na=False)
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(raw), dtype=str, engine="python", keep_default_na=False, encoding="latin-1")
    return df.fillna("")

def _only_allowed(df: pd.DataFrame, allowed: List[str]) -> pd.DataFrame:
    """Devuelve solo las columnas permitidas, creando en blanco las que falten y en el orden oficial."""
    cols_present = [c for c in df.columns if c in allowed]
    d = df[cols_present].copy()
    # crea vacías para las que falten
    for c in allowed:
        if c not in d.columns:
            d[c] = ""
    # orden final
    return d[allowed]

def _download_xlsx(filename: str, data: List[Dict[str, Any]], columns: List[str], caption: str):
    """Botón de descarga de plantilla XLSX con columnas oficiales y 2 filas ejemplo."""
    buf = io.BytesIO()
    df = pd.DataFrame(data, columns=columns)
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Plantilla")
    buf.seek(0)
    st.download_button(
        label=caption,
        data=buf.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

# ---- Limpiezas de tipado/valor --------------------------------
def _as_string_clean(v) -> str:
    """Texto robusto:
    - bytes -> utf-8
    - None/'none'/'nan'/'nat' -> ''
    - strip()
    """
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "ignore")
        except Exception:
            v = str(v)
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "nan", "nat"}:
        return ""
    return s

def _to_date_iso_or_blank(v) -> str:
    """Convierte a fecha ISO YYYY-MM-DD o ''."""
    s = _as_string_clean(v)
    if s == "":
        return ""
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return ""
        return pd.to_datetime(dt).date().isoformat()
    except Exception:
        return ""

def _to_int_or_blank(v) -> str:
    s = _as_string_clean(v)
    if s == "":
        return ""
    try:
        return str(int(float(s.replace(",", "."))))
    except Exception:
        return ""  # deja vacío si no es interpretable

def _to_float_or_blank(v) -> str:
    s = _as_string_clean(v)
    if s == "":
        return ""
    try:
        return f"{float(s.replace(',', '.')):.2f}"
    except Exception:
        return ""

# ------------------------
# Render principal
# ------------------------
def render(company_id: int):
    st.subheader("📥 Importación de datos (XLSX recomendado)")

    kind = st.selectbox("¿Qué quieres importar?", ["partners", "events"], index=0)
    st.caption("Rellena la plantilla, guárdala en **XLSX**, y súbela. Ignoraremos columnas extra para evitar errores.")
    st.divider()

    # ==== Plantillas ====
    c1, c2 = st.columns(2)
    with c1:
        if kind == "partners":
            sample = [
                {
                    "nombre": "Ejemplo, S.L.",
                    "nif": "B12345678",
                    "domicilio": "C/ Mayor 1, Madrid",
                    "nacionalidad": "Española",
                    "fecha_nacimiento_constitucion": "",
                    "partner_no": "1",
                },
                {
                    "nombre": "Persona Nombre Apellido",
                    "nif": "12345678Z",
                    "domicilio": "",
                    "nacionalidad": "Española",
                    "fecha_nacimiento_constitucion": "1980-07-12",
                    "partner_no": "2",
                },
            ]
            _download_xlsx(
                "plantilla_partners.xlsx",
                sample,
                PARTNERS_COLS,
                "⬇️ Descargar plantilla XLSX (partners)",
            )
        else:
            sample = [
                {
                    "fecha": "2025-01-15",
                    "tipo": "ALTA",
                    "socio_transmite": "",
                    "socio_adquiere": "1",
                    "rango_desde": "1",
                    "rango_hasta": "100",
                    "nuevo_valor_nominal": "1.00",
                    "documento": "Escritura 1/2025",
                    "observaciones": "",
                },
                {
                    "fecha": "2025-03-01",
                    "tipo": "TRANSMISION",
                    "socio_transmite": "1",
                    "socio_adquiere": "2",
                    "rango_desde": "1",
                    "rango_hasta": "10",
                    "nuevo_valor_nominal": "",
                    "documento": "Contrato privado",
                    "observaciones": "Ejemplo",
                },
            ]
            _download_xlsx(
                "plantilla_events.xlsx",
                sample,
                EVENTS_COLS,
                "⬇️ Descargar plantilla XLSX (events)",
            )

    with c2:
        st.caption("Las plantillas incluyen 2 filas de ejemplo. Puedes borrar las filas de ejemplo si no las necesitas.")

    st.divider()

    # ==== Subida ====
    up = st.file_uploader(
        "Sube tu archivo (.xlsx o .csv)",
        type=["xlsx", "csv"],
        accept_multiple_files=False,
        key=f"upload_{kind}_file",
    )
    if not up:
        st.info("Selecciona un archivo para continuar.")
        return

    # ==== Lectura + normalización de encabezados/columnas ====
    try:
        df_raw = _read_any(up)
        df_norm = _normalize_headers(df_raw)
        allowed = PARTNERS_COLS if kind == "partners" else EVENTS_COLS
        df = _only_allowed(df_norm, allowed)
    except Exception as e:
        log.error("No se pudo leer el archivo: %s", e, exc_info=True)
        st.error(f"No se pudo leer el archivo: {e}")
        return

    # ==== Limpieza de tipos para evitar errores en BD y al pintar ====
    if kind == "partners":
        # Texto limpio
        for c in ["nombre", "nif", "domicilio", "nacionalidad"]:
            if c in df.columns:
                df[c] = df[c].map(_as_string_clean).astype(str)
        # Nº socio entero (como texto) o vacío
        if "partner_no" in df.columns:
            df["partner_no"] = df["partner_no"].map(_to_int_or_blank)
        # Fecha nacimiento/constitución en ISO (texto) o vacío
        if "fecha_nacimiento_constitucion" in df.columns:
            df["fecha_nacimiento_constitucion"] = df["fecha_nacimiento_constitucion"].map(_to_date_iso_or_blank)
    else:
        # Fecha ISO (texto) o vacío
        if "fecha" in df.columns:
            df["fecha"] = df["fecha"].map(_to_date_iso_or_blank)
        # IDs y rangos como enteros (texto) o vacío
        for c in ["socio_transmite", "socio_adquiere", "rango_desde", "rango_hasta"]:
            if c in df.columns:
                df[c] = df[c].map(_to_int_or_blank)
        # VN a decimal con 2 (texto) o vacío
        if "nuevo_valor_nominal" in df.columns:
            df["nuevo_valor_nominal"] = df["nuevo_valor_nominal"].map(_to_float_or_blank)
        # Texto limpio resto
        for c in ["tipo", "documento", "observaciones"]:
            if c in df.columns:
                df[c] = df[c].map(_as_string_clean).astype(str)

    # ==== Validación mínima ====
    errs: List[str] = []
    if kind == "partners":
        if df.empty:
            errs.append("El archivo no contiene filas.")
        if df["nombre"].map(_as_string_clean).eq("").all():
            errs.append("Todas las filas tienen 'nombre' vacío.")
    else:
        if df.empty:
            errs.append("El archivo no contiene filas.")
        if "fecha" in df.columns and df["fecha"].map(_as_string_clean).eq("").any():
            errs.append("Hay filas de events con 'fecha' vacía.")
        if "tipo" in df.columns and df["tipo"].map(_as_string_clean).eq("").any():
            errs.append("Hay filas de events con 'tipo' vacío.")

    if errs:
        st.error("Corrige estos puntos y vuelve a intentarlo:")
        for e in errs:
            st.write(f"• {e}")
        st.stop()

    # ==== Previsualización ====
    st.success(f"Archivo leído correctamente. Filas detectadas: {len(df)}")
    st.dataframe(df.head(MAX_PREVIEW_ROWS), width="stretch", hide_index=True)

    # ==== Commit ====
    label = "✅ Importar partners" if kind == "partners" else "✅ Importar events"
    if st.button(label, width="stretch", key=f"btn_commit_{kind}"):
        try:
            rows_ok = df.to_dict(orient="records")
            summary = commit(kind, company_id, rows_ok)  # tu backend hace la transacción
            if summary.errors:
                st.error("Se produjo un error y no se importó nada.")
                st.code("\n".join(summary.errors))
            else:
                if kind == "partners":
                    st.success(f"Completado: insertados {summary.inserted}, actualizados {summary.updated}.")
                else:
                    st.success(f"Completado: insertados {summary.inserted}.")
        except Exception as e:
            log.error("Error commit %s: %s", kind, e, exc_info=True)
            st.error(str(e))