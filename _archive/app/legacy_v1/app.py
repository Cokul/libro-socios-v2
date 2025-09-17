# app.py

import streamlit as st
import pandas as pd
import json
import services as svc
import os
import constants as C
import app_helpers as AH

from datetime import date, datetime
from pathlib import Path
from db import init_db, get_connection
from config import BACKUP_DIR, EXPORT_DIR

AH.setup_logging()  # crea logs/app.log si no existe

# --- Inicialización de historial de "deshacer" ---
if "undo_stack" not in st.session_state:
    st.session_state["undo_stack"] = []  # cada item será una tupla ("sql", sentencia_sql, params_tuple)

# ----- Rango global de fechas permitido -----
MIN_DATE = date(1900, 1, 1)
MAX_DATE = date(2100, 12, 31)
# --- Alturas homogéneas para tablas ---
TABLE_H = 260        # altura por defecto
TABLE_H_BIG = 320    # tablas “densas” (p.ej., holdings)

st.set_page_config(page_title="Libro Registro de Socios – MVP", layout="wide")
AH.sticky_headers()

# Asegura directorios configurados
Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)

# Inicializa BD una sola vez
if "db_initialized" not in st.session_state:
    init_db()
    st.session_state["db_initialized"] = True
    
with get_connection() as conn:
    svc.ensure_db_consistency_primitives(conn)
    svc.patch_triggers_redenominacion(conn)

st.title("📘 Libro Registro de Socios – MVP local")

# ===== Adaptadores a constants.py (con fallbacks seguros) =====
# ÓRGANOS de gobierno: mapa clave->etiqueta
ORG_LABELS = getattr(C, "ORGANO_LABELS", {
    "admin_unico": "Administrador único",
    "admins_solidarios": "Administradores solidarios",
    "consejo": "Consejo de administración",
})
ORG_KEYS = list(ORG_LABELS.keys())

# ROLES disponibles
ROLE_OPTIONS = getattr(C, "ROLES", [
    "administrador_unico",
    "administrador_solidario",
    "presidente",
    "vicepresidente",
    "secretario",
    "consejero_delegado",
    "consejero",
])

# Firmantes por defecto según órgano
ORG_DEFAULT_FIRMANTES = getattr(C, "ORGANO_DEFAULT_FIRMANTES", {
    "admin_unico": [{"nombre": "", "rol": "administrador_unico"}],
    "admins_solidarios": [
        {"nombre": "", "rol": "administrador_solidario"},
        {"nombre": "", "rol": "administrador_solidario"},
    ],
    "consejo": [
        {"nombre": "", "rol": "presidente"},
        {"nombre": "", "rol": "secretario"},
    ],
})

def default_firmantes_for(organo: str):
    return ORG_DEFAULT_FIRMANTES.get(organo, [{"nombre": "", "rol": "administrador_unico"}])

# TIPOS de evento (ya los usas en eventos)
TIPOS_EVENTO = getattr(C, "TIPOS_EVENTO", [
    "ALTA","AMPL_EMISION","AMPL_VALOR","TRANSMISION","BAJA","RED_AMORT",
    "RED_VALOR","PIGNORACION","EMBARGO","USUFRUCTO","REDENOMINACION"
])

# TIPOS de socio (si los defines en constants.py). Si no, fallback actual.
PARTNER_TIPOS = getattr(C, "PARTNER_TIPOS", ["socio", "acreedor", "usufructuario", "otros"])

# -------- helpers ----------
def safe_int(val, default=0):
    if val is None:
        return default
    try:
        if isinstance(val, float) and pd.isna(val):
            return default
        return int(val)
    except Exception:
        return default

def safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        if isinstance(val, float) and pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default

def id_to_label(df_partners, pid):
    if pid is None or (isinstance(pid, float) and pd.isna(pid)):
        return "(no aplica)"
    row = df_partners[df_partners.id == int(pid)]
    if row.empty:
        return "(no aplica)"
    return f"{int(row.iloc[0]['id'])} – {row.iloc[0]['nombre']}"

def label_to_id(label):
    return None if label == "(no aplica)" else int(label.split(" – ")[0])

def fmt_company(row):
    """Devuelve 'id – nombre (CIF)' para usar en selectores.
    OJO: en pandas row.name es el índice; por eso usamos row['name'].
    """
    return f"{int(row['id'])} – {row['name']} ({row['cif']})"

tabs = st.tabs([
    "Sociedades",
    "Socios",
    "Eventos (alta / edición / borrado)",
    "Recalcular / Inventario",
    "Exportar",
    "Administración",
])

# -------- Función “Deshacer” + botón global --------
def undo_last_change():
    """Ejecuta la última operación inversa guardada en undo_stack."""
    if not st.session_state["undo_stack"]:
        st.info("No hay cambios para deshacer.")
        return
    kind, sql, params = st.session_state["undo_stack"].pop()
    try:
        conn = get_connection()
        conn.execute(sql, params)
        conn.commit()
        st.toast("↩️ Último cambio deshecho")
        st.rerun()
    except Exception as e:
        st.error(f"No se pudo deshacer: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

# Botón global de deshacer (colócalo donde más te guste: arriba o dentro de cada pestaña)
st.button("↩️ Deshacer último cambio", help="Restaura la última operación realizada", on_click=undo_last_change)

# ================= SOCIEDADES =================
with tabs[0]:
    st.subheader("Sociedades")

    # --- Listado (lo dejamos fuera de expanders para visión rápida) ---
    with get_connection() as conn:
        dfc = pd.read_sql_query("SELECT * FROM companies ORDER BY id", conn)

    st.markdown("**Listado de sociedades**")
    q_soc = st.text_input("Filtro rápido (por texto)", placeholder="Nombre, CIF, domicilio…", key="q_sociedades")
    try:
        dfc_view = AH.filter_df_by_query(dfc.copy(), q_soc, cols=None)
    except Exception:
        dfc_view = dfc

    st.dataframe(dfc_view if 'dfc_view' in locals() else dfc, use_container_width=True, height=TABLE_H)

    # ====== ➕ ALTA ======
    with st.expander("➕ Alta", expanded=True):
                
        def _clean_firmantes(df):
            df = df.copy()
            df["nombre"] = df["nombre"].fillna("").str.strip()
            df["rol"] = df["rol"].fillna("").str.strip().str.lower()
            df = df[(df["nombre"] != "") & (df["rol"] != "")]
            return df
        def _coerce_firmantes(obj) -> pd.DataFrame:
            if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
                df = pd.DataFrame(obj["data"])
            elif isinstance(obj, pd.DataFrame):
                df = obj.copy()
            elif isinstance(obj, list):
                df = pd.DataFrame(obj)
            elif isinstance(obj, dict):
                try:
                    df = pd.DataFrame(obj)
                    if set(obj.keys()) == {"nombre","rol"}:
                        df = pd.DataFrame([obj])
                except Exception:
                    df = pd.DataFrame()
            else:
                df = pd.DataFrame()
            for c in ("nombre","rol"):
                if c not in df.columns:
                    df[c] = ""
            return df[["nombre","rol"]]

        with st.form("form_company_add"):
            name = st.text_input("Nombre", key="comp_add_nombre")
            cif = st.text_input("CIF", key="comp_add_cif")
            dom = st.text_input("Domicilio", key="comp_add_dom")
            fconst = st.date_input(
                "Fecha constitución",
                value=date.today(), min_value=MIN_DATE, max_value=MAX_DATE,
                key="comp_add_fconst"
            )
            vn = st.number_input("Valor nominal (€)", value=5.0, key="comp_add_vn")
            total = st.number_input("Participaciones totales", value=0, key="comp_add_total")

            st.markdown("**Gobernanza inicial**")
            organo_new = st.selectbox("Órgano", ORG_KEYS, format_func=lambda k: ORG_LABELS.get(k, k), key="org_new")

            df_new_firm = pd.DataFrame(default_firmantes_for(organo_new), columns=["nombre", "rol"])
            df_new_edit = st.data_editor(
                df_new_firm, num_rows="dynamic", hide_index=True, use_container_width=True,
                column_config={
                    "nombre": st.column_config.TextColumn("Nombre y apellidos"),
                    "rol": st.column_config.SelectboxColumn("Rol", options=ROLE_OPTIONS),
                },
                key="firm_new_editor"
            )

            # --- SUBMIT (dentro del with st.form("form_company_add"): ) ---
            save_add_company = st.form_submit_button("💾 Guardar sociedad", type="primary")

            if save_add_company:
                import re
                # Normaliza y valida
                name_clean = (name or "").strip()
                cif_clean  = (cif or "").strip().upper()
                cif_clean  = re.sub(r"[\s\-]", "", cif_clean)   # quita espacios y guiones
                dom_clean  = (dom or "").strip()

                errores = []

                if not name_clean:
                    errores.append("• El **Nombre** es obligatorio.")
                if not cif_clean:
                    errores.append("• El **CIF** es obligatorio.")

                # Valor nominal
                try:
                    vn_val = float(vn)
                    if vn_val <= 0:
                        errores.append("• El **Valor nominal** debe ser > 0.")
                except Exception:
                    errores.append("• El **Valor nominal** no es válido.")

                # Participaciones totales
                try:
                    total_val = int(total)
                    if total_val < 0:
                        errores.append("• Las **Participaciones totales** deben ser ≥ 0.")
                except Exception:
                    errores.append("• Las **Participaciones totales** no son válidas.")

                # Fecha
                if not (fconst and MIN_DATE <= fconst <= MAX_DATE):
                    errores.append("• La **Fecha de constitución** es obligatoria y debe estar en rango.")

                if errores:
                    st.error("No se puede guardar:\n\n" + "\n".join(errores))
                else:
                    try:
                        cif_norm = cif_clean
                        with get_connection() as conn:
                            exists = conn.execute(
                                "SELECT 1 FROM companies WHERE UPPER(TRIM(cif)) = ?",
                                (cif_norm,)
                            ).fetchone()
                            if exists:
                                st.error(f"El CIF {cif_norm} ya existe.")
                            else:
                                conn.execute("""
                                    INSERT INTO companies (name,cif,domicilio,fecha_constitucion,valor_nominal,participaciones_totales)
                                    VALUES (?,?,?,?,?,?)
                                """, (name_clean, cif_norm, dom_clean, fconst, vn_val, total_val))
                                company_id_new = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                                # Firmantes
                                raw = df_new_edit
                                df_current = _coerce_firmantes(raw)
                                if df_current.empty:
                                    raw_ss = st.session_state.get("firm_new_editor", [])
                                    df_current = _coerce_firmantes(raw_ss)
                                df_save = _clean_firmantes(df_current)
                                if organo_new == "admin_unico":
                                    if df_save.empty:
                                        df_save = pd.DataFrame([{"nombre": "", "rol": "administrador_unico"}])
                                    else:
                                        df_save = df_save.iloc[:1]
                                        df_save.iloc[0, df_save.columns.get_loc("rol")] = "administrador_unico"

                                svc.set_governance(conn, company_id_new, organo_new, df_save.to_dict(orient="records"))

                                firmantes_json_txt = json.dumps(
                                    df_save.to_dict(orient="records"),
                                    ensure_ascii=False, separators=(",",":")
                                )
                                conn.execute("UPDATE companies SET firmantes_json=? WHERE id=?", (firmantes_json_txt, company_id_new))
                                conn.commit()

                        st.toast("✅ Sociedad creada")
                        st.session_state["undo_stack"].append(("sql", "DELETE FROM companies WHERE id=?", (company_id_new,)))
                        st.rerun()
                    except Exception as e:
                        AH.log_exception(e, where="companies.add", extra={"cif": cif})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    # ====== ✏️ EDITAR ======
    with st.expander("✏️ Editar", expanded=False):
        with get_connection() as conn:
            dfc2 = pd.read_sql_query("SELECT * FROM companies ORDER BY id", conn)
        options = ["(elige)"] + [fmt_company(row) for _, row in dfc2.iterrows()]
        choice = st.selectbox("Sociedad", options, key="company_edit_choice")

        if choice != "(elige)":
            company_id = int(choice.split(" – ")[0])

            with get_connection() as conn:
                row = conn.execute("SELECT * FROM companies WHERE id=?", (company_id,)).fetchone()
                gov = svc.get_governance(conn, company_id)

            # --- Fecha constitución segura (antes del form)
            raw_fconst = row["fecha_constitucion"]
            if isinstance(raw_fconst, (datetime, date)):
                _d = raw_fconst.date() if isinstance(raw_fconst, datetime) else raw_fconst
            else:
                _ts = pd.to_datetime(raw_fconst, errors="coerce")
                _d = _ts.date() if (isinstance(_ts, pd.Timestamp) and not pd.isna(_ts)) else None
            fconst_e_val = max(MIN_DATE, min((_d or date.today()), MAX_DATE))

            # --- Reset seguro para el data_editor de firmantes (sin escribir en session_state del widget)
            ver_key    = f"__firm_edit_ver_{company_id}"         # contador de versión del widget
            flag_key   = f"__reset_firm_flag_{company_id}"       # bandera para pedir reset
            source_key = f"__firm_source_{company_id}"           # 'gov' o 'default'

            if ver_key not in st.session_state:
                st.session_state[ver_key] = 0
            if flag_key not in st.session_state:
                st.session_state[flag_key] = False
            if source_key not in st.session_state:
                st.session_state[source_key] = "gov"

            # Si se pidió reset en el rerun anterior: cambiamos versión y la fuente a 'default'
            if st.session_state[flag_key]:
                st.session_state[flag_key] = False
                st.session_state[ver_key] += 1
                st.session_state[source_key] = "default"
            
            # ---------- FORMULARIO DE EDICIÓN ----------
            with st.form(f"form_company_edit_{company_id}", clear_on_submit=False):
                # Datos básicos
                name_e = st.text_input("Nombre", value=row["name"], key=f"comp_edit_nombre_{company_id}")
                cif_e  = st.text_input("CIF", value=row["cif"], key=f"comp_edit_cif_{company_id}")
                dom_e  = st.text_input("Domicilio", value=row["domicilio"] or "", key=f"comp_edit_dom_{company_id}")

                fconst_e = st.date_input(
                    "Fecha constitución",
                    value=fconst_e_val, min_value=MIN_DATE, max_value=MAX_DATE,
                    key=f"comp_edit_fconst_{company_id}"
                )
                vn_e    = st.number_input("Valor nominal (€)", value=float(row["valor_nominal"]), key=f"comp_edit_vn_{company_id}")
                total_e = st.number_input("Participaciones totales", value=int(row["participaciones_totales"]), key=f"comp_edit_total_{company_id}")

                # ---------- Gobernanza dentro del with st.form(...):
                st.markdown("**Gobernanza**")
                organo_key = st.selectbox(
                    "Órgano de administración",
                    options=ORG_KEYS,
                    index=ORG_KEYS.index(gov["organo"]) if gov and gov.get("organo") in ORG_KEYS else 0,
                    format_func=lambda k: ORG_LABELS.get(k, k),
                    key=f"org_edit_{company_id}",
                )

                # Según 'source', inicializamos con lo guardado ('gov') o con los por defecto
                source = st.session_state[source_key]
                if source == "default":
                    firmantes_ini = default_firmantes_for(organo_key)
                else:
                    firmantes_ini = gov["firmantes"] if gov and gov.get("firmantes") else default_firmantes_for(organo_key)

                df_firm = pd.DataFrame(firmantes_ini, columns=["nombre", "rol"])

                # Cambiamos la key del editor con la versión para forzar una instancia nueva
                editor_key = f"firm_edit_{company_id}_v{st.session_state[ver_key]}"
                df_edit = st.data_editor(
                    df_firm,
                    num_rows="dynamic", hide_index=True, use_container_width=True,
                    column_config={
                        "nombre": st.column_config.TextColumn("Nombre y apellidos", required=False, width="medium"),
                        "rol": st.column_config.SelectboxColumn("Rol", options=ROLE_OPTIONS, required=True, width="small"),
                    },
                    key=editor_key,
                )

                col1, col2 = st.columns([1,1])
                save_edit     = col1.form_submit_button("💾 Guardar cambios", type="primary")
                reset_default = col2.form_submit_button("↺ Rellenar firmantes por defecto")

            # --- Acciones de botones (fuera del with st.form)            
            if reset_default:
                # Pedimos reset para el próximo rerun (no tocamos session_state del editor)
                st.session_state[flag_key] = True
                st.rerun()

            if save_edit:
                # Validación sencilla
                errores = []
                name_clean = (name_e or "").strip()
                cif_clean  = (cif_e  or "").strip().upper()
                import re
                cif_clean = re.sub(r"[\s\-]", "", cif_clean)  # sin espacios ni guiones
                dom_clean_e = (dom_e or "").strip()

                if not name_clean: errores.append("• El **Nombre** es obligatorio.")
                if not cif_clean:  errores.append("• El **CIF** es obligatorio.")

                try:
                    vn_val = float(vn_e)
                    if vn_val <= 0: errores.append("• El **Valor nominal** debe ser > 0.")
                except Exception:
                    errores.append("• El **Valor nominal** no es válido.")

                try:
                    total_val = int(total_e)
                    if total_val < 0: errores.append("• Las **Participaciones totales** deben ser ≥ 0.")
                except Exception:
                    errores.append("• Las **Participaciones totales** no son válidas.")

                if not (fconst_e and MIN_DATE <= fconst_e <= MAX_DATE):
                    errores.append("• La **Fecha de constitución** es obligatoria y debe estar en rango.")

                if errores:
                    st.error("No se puede guardar:\n" + "\n".join(errores))
                else:
                    try:
                        cif_norm_e = cif_clean
                        with get_connection() as conn:
                            dup = conn.execute(
                                "SELECT id FROM companies WHERE UPPER(TRIM(cif))=? AND id<>?",
                                (cif_norm_e, company_id)
                            ).fetchone()
                            if dup:
                                st.error(f"El CIF {cif_norm_e} ya existe en otra sociedad.")
                            else:
                                # Snapshot para deshacer
                                row_before = dict(row)
                                cols = [k for k in row_before.keys() if k != "id"]
                                set_clause = ", ".join([f"{c}=?" for c in cols])
                                params_rev = tuple([row_before[c] for c in cols] + [company_id])
                                st.session_state["undo_stack"].append(("sql", f"UPDATE companies SET {set_clause} WHERE id=?", params_rev))
                                                               
                                raw = df_edit
                                df_current = _coerce_firmantes(raw)
                                if df_current.empty:
                                    raw_ss = st.session_state.get(editor_key, [])
                                    df_current = _coerce_firmantes(raw_ss)
                                df_save = _clean_firmantes(df_current)
                                if organo_key == "admin_unico":
                                    if df_save.empty:
                                        df_save = pd.DataFrame([{"nombre": "", "rol": "administrador_unico"}])
                                    else:
                                        df_save = df_save.iloc[:1]
                                        df_save.iloc[0, df_save.columns.get_loc("rol")] = "administrador_unico"

                                firmantes_json_txt = json.dumps(df_save.to_dict(orient="records"), ensure_ascii=False, separators=(",",":"))

                                with get_connection() as conn:
                                    conn.execute("""
                                        UPDATE companies
                                        SET name=?, cif=?, domicilio=?, fecha_constitucion=?, valor_nominal=?, participaciones_totales=?, firmantes_json=?
                                        WHERE id=?
                                    """, (name_clean, cif_norm_e, dom_clean_e, fconst_e, vn_val, total_val, firmantes_json_txt, company_id))
                                    svc.set_governance(conn, company_id, organo_key, df_save.to_dict(orient="records"))
                                    conn.commit()

                        st.toast("✅ Cambios guardados")
                        st.rerun()
                    except Exception as e:
                        AH.log_exception(e, where="companies.edit", extra={"company_id": company_id})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    # ====== 🗑️ BORRAR ======
    with st.expander("🗑️ Borrar", expanded=False):
        with get_connection() as conn:
            dfc2 = pd.read_sql_query("SELECT * FROM companies ORDER BY id", conn)
        options = ["(elige)"] + [fmt_company(row) for _, row in dfc2.iterrows()]
        choice = st.selectbox("Sociedad a borrar", options, key="company_delete_choice")

        if choice != "(elige)":
            company_id = int(choice.split(" – ")[0])
            if st.checkbox("🛑 Confirmo que deseo borrar esta sociedad"):
                if st.button("🗑️ Borrar definitivamente", type="secondary"):
                    try:
                        with get_connection() as conn:
                            # Snapshot para deshacer NO trivial (borrado en cascada). Lo dejamos sin undo por simplicidad.
                            conn.execute("DELETE FROM companies WHERE id=?", (company_id,))
                            conn.commit()
                        st.toast("🗑️ Sociedad borrada")
                        st.rerun()
                    except Exception as e:
                        AH.log_exception(e, where="companies.delete", extra={"company_id": company_id})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    # ====== 🏛️ GOBERNANZA (edición rápida opcional) ======
    with st.expander("🏛️ Gobernanza", expanded=False):
        with get_connection() as conn:
            dfc2 = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
        options = ["(elige)"] + [fmt_company(row) for _, row in dfc2.iterrows()]
        choice_g = st.selectbox("Sociedad", options, key="gov_company_choice")
        if choice_g != "(elige)":
            company_id_g = int(choice_g.split(" – ")[0])
            with get_connection() as conn:
                gov = svc.get_governance(conn, company_id_g)
            
            with st.form("form_gobernanza_rapida"):
                organo = st.selectbox(
                    "Órgano de administración",
                    options=ORG_KEYS,
                    index=ORG_KEYS.index(gov["organo"]) if gov and gov.get("organo") in ORG_KEYS else 0,
                    format_func=lambda k: ORG_LABELS.get(k, k),
                )

                firmantes_ini = gov["firmantes"] if gov and gov.get("firmantes") else default_firmantes_for(organo)
                df_firm = pd.DataFrame(firmantes_ini, columns=["nombre", "rol"])
                df_edit = st.data_editor(
                    df_firm, num_rows="dynamic", hide_index=True, use_container_width=True,
                    column_config={
                        "nombre": st.column_config.TextColumn("Nombre y apellidos"),
                        "rol": st.column_config.SelectboxColumn("Rol", options=ROLE_OPTIONS),
                    },
                )
                                
                save_g = st.form_submit_button("💾 Guardar", type="primary")

            if save_g:
                try:
                    df_save = df_edit.copy()
                    df_save["nombre"] = df_save["nombre"].fillna("").str.strip()
                    df_save["rol"]    = df_save["rol"].fillna("").str.strip().str.lower()
                    df_save = df_save[(df_save["nombre"] != "") & (df_save["rol"] != "")]
                    if df_save.empty:
                        st.error("Debes indicar al menos un firmante válido.")
                    else:
                        if organo == "admin_unico":
                            df_save = df_save.iloc[:1]
                            df_save.iloc[0, df_save.columns.get_loc("rol")] = "administrador_unico"

                        with get_connection() as conn:
                            svc.set_governance(conn, company_id_g, organo, df_save.to_dict(orient="records"))
                            firmantes_json_txt = json.dumps(df_save.to_dict(orient="records"), ensure_ascii=False, separators=(",",":"))
                            conn.execute("UPDATE companies SET firmantes_json=? WHERE id=?", (firmantes_json_txt, company_id_g))
                            conn.commit()
                    st.toast("✅ Gobernanza actualizada")
                    st.rerun()
                except Exception as e:
                    AH.log_exception(e, where="governance.quick_set", extra={"company_id": company_id_g})
                    st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

# ================= SOCIOS =================
with tabs[1]:
    st.subheader("Socios / Titulares de derechos")

    # Selección de sociedad
    with get_connection() as conn:
        dfc = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
    soc_options = ["(elige)"] + [fmt_company(row) for _, row in dfc.iterrows()]
    soc_choice = st.selectbox("Sociedad", soc_options, key="soc_company_selector")

    if soc_choice != "(elige)":
        company_id = int(soc_choice.split(" – ")[0])

        # Listado rápido
        with get_connection() as conn:
            dfp = pd.read_sql_query(
                "SELECT id, nombre, nif, domicilio, nacionalidad FROM partners WHERE company_id = ? ORDER BY id",
                conn, params=(company_id,)
            )

        st.markdown("**Socios de la sociedad seleccionada**")
        q_part = st.text_input("Filtro rápido (por texto)", placeholder="Nombre, NIF, domicilio…", key="q_socios")
        try:
            dfp_view = AH.filter_df_by_query(dfp.copy(), q_part, cols=None)
        except Exception:
            dfp_view = dfp

        st.dataframe(dfp_view if 'dfp_view' in locals() else dfp, use_container_width=True, height=TABLE_H)

        # ====== ➕ ALTA ======
        with st.expander("➕ Alta", expanded=True):
            with st.form("form_partner_add", clear_on_submit=False):
                col1, col2 = st.columns(2)
                with col1:
                    nombre = st.text_input("Nombre o razón social", key="partner_add_nombre")
                    nif = st.text_input("NIF/CIF", key="partner_add_nif")
                with col2:
                    domicilio = st.text_input("Domicilio", key="partner_add_dom")
                    nacionalidad = st.text_input("Nacionalidad", value="Española", key="partner_add_nat")
                tipo = st.selectbox("Tipo", PARTNER_TIPOS, key="partner_add_tipo")

                # ¡NO deshabilitamos el botón! Validamos después del submit
                submitted = st.form_submit_button("💾 Guardar socio", type="primary")

            if submitted:
                # Normaliza entradas
                nombre_clean = (nombre or "").strip()
                nif_clean = (nif or "").strip().upper()
                import re
                nif_clean = re.sub(r"[\s\-]", "", nif_clean)   # quita espacios y guiones
                domicilio_clean = (domicilio or "").strip()
                nacionalidad_clean = (nacionalidad or "").strip()

                # Validación sencilla
                errores = []
                if not nombre_clean:
                    errores.append("• El **Nombre** es obligatorio.")
                if not nif_clean:
                    errores.append("• El **NIF/CIF** es obligatorio.")

                if errores:
                    st.error("No se puede guardar:\n" + "\n".join(errores))
                else:
                    try:
                        with get_connection() as conn:
                            svc.create_partner(conn, company_id, nombre_clean, nif_clean, domicilio_clean, nacionalidad_clean)
                            conn.commit()
                        st.toast("✅ Socio guardado")
                        st.rerun()
                    except Exception as e:
                        AH.log_exception(e, where="partners.add", extra={"company_id": company_id, "nif": nif_clean})
                        st.error(f"No se pudo guardar el socio: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

        # ====== ✏️ EDITAR ======
        with st.expander("✏️ Editar", expanded=False):
            with get_connection() as conn:
                socios = svc.list_partners(conn, company_id)
            df_soc = pd.DataFrame(socios)

            if not df_soc.empty:
                socio_sel = st.selectbox("Selecciona socio", df_soc["nombre"], key="soc_edit_selector")
                socio_data = df_soc[df_soc["nombre"] == socio_sel].iloc[0]
                sid = int(socio_data["id"])

                with st.form(f"modificar_socio_{sid}", clear_on_submit=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        nombre_m = st.text_input("Nombre o razón social", value=socio_data["nombre"], key=f"partner_edit_nombre_{sid}")
                        nif_m = st.text_input("NIF/CIF", value=socio_data["nif"], key=f"partner_edit_nif_{sid}")
                    with col2:
                        domicilio_m = st.text_input("Domicilio", value=socio_data["domicilio"], key=f"partner_edit_dom_{sid}")
                        nacionalidad_m = st.text_input("Nacionalidad", value=socio_data["nacionalidad"], key=f"partner_edit_nat_{sid}")

                    # Botón SIEMPRE habilitado; validamos después
                    save_edit = st.form_submit_button("💾 Guardar cambios", type="primary")

                if save_edit:
                    # Normaliza entradas
                    nombre_clean = (nombre_m or "").strip()
                    nif_clean = (nif_m or "").strip().upper()
                    nif_clean = re.sub(r"[\s\-]", "", nif_clean)
                    dom_clean = (domicilio_m or "").strip()
                    nat_clean = (nacionalidad_m or "").strip()

                    # Validación
                    errores = []
                    if not nombre_clean:
                        errores.append("• El **Nombre** es obligatorio.")
                    if not nif_clean:
                        errores.append("• El **NIF/CIF** es obligatorio.")

                    if errores:
                        st.error("No se puede guardar:\n" + "\n".join(errores))
                    else:
                        try:
                            with get_connection() as conn:
                                # Snapshot para deshacer
                                row_before = dict(socio_data)
                                cols = [k for k in row_before.keys() if k != "id"]
                                set_clause = ", ".join([f"{c}=?" for c in cols])
                                params_rev = tuple([row_before[c] for c in cols] + [sid])
                                st.session_state["undo_stack"].append((
                                    "sql",
                                    f"UPDATE partners SET {set_clause} WHERE id=?",
                                    params_rev
                                ))

                                svc.update_partner(conn, sid, nombre_clean, nif_clean, dom_clean, nat_clean)
                                conn.commit()
                            st.toast("✅ Datos de socio actualizados")
                            st.rerun()
                        except Exception as e:
                            AH.log_exception(e, where="partners.edit", extra={"partner_id": sid})
                            st.error(f"No se pudo actualizar el socio: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
            else:
                st.info("No hay socios dados de alta todavía")

        # ====== 🗑️ BORRAR ======
        with st.expander("🗑️ Borrar", expanded=False):
            with get_connection() as conn:
                socios = svc.list_partners(conn, company_id)
            df_soc = pd.DataFrame(socios)
            if df_soc.empty:
                st.info("No hay socios para borrar.")
            else:
                socio_sel_b = st.selectbox("Socio a borrar", df_soc["nombre"], key="soc_delete_selector")
                socio_data_b = df_soc[df_soc["nombre"] == socio_sel_b].iloc[0]
                if st.checkbox("🛑 Confirmo que deseo borrar este socio"):
                    if st.button("🗑️ Borrar definitivamente", type="secondary"):
                        try:
                            with get_connection() as conn:
                                # Snapshot para deshacer (reinsert)
                                row_del = dict(socio_data_b)
                                cols = list(row_del.keys())
                                qs = ", ".join(["?"] * len(cols))
                                params_ins = tuple(row_del[c] for c in cols)
                                st.session_state["undo_stack"].append((
                                    "sql",
                                    f"INSERT INTO partners ({', '.join(cols)}) VALUES ({qs})",
                                    params_ins
                                ))

                                svc.delete_partner(conn, socio_data_b["id"])  # Asegúrate de tener esta función
                                conn.commit()
                            st.toast("🗑️ Socio borrado")
                            st.rerun()
                        except Exception as e:
                            AH.log_exception(e, where="partners.delete", extra={"partner_id": int(socio_data_b['id'])})
                            st.error(f"No se pudo borrar: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

        # ====== 📊 Resumen (igual que antes) ======
        st.subheader("📊 Resumen actual de socios")
        with get_connection() as conn:
            snapshot = svc.snapshot_socios_vigentes(conn, company_id)
            socios = svc.list_partners(conn, company_id)
            ultimas = svc.last_annotations(conn, company_id)

        df_snap = pd.DataFrame(snapshot)
        df_meta = pd.DataFrame(socios)

        if not df_meta.empty:
            df = df_meta.merge(
                df_snap[["socio_id", "participaciones"]],
                left_on="id", right_on="socio_id", how="inner"
            ).drop(columns=["socio_id"])

            df["Participaciones"] = df["participaciones"].fillna(0).astype(int)
            df["Fecha última anotación"] = df["id"].map(ultimas)

            total_part = int(df["Participaciones"].sum())
            df["Cuota (%)"] = (df["Participaciones"] / total_part * 100).round(4) if total_part > 0 else 0.0

            df = df[["nombre", "nif", "nacionalidad", "domicilio", "Participaciones", "Cuota (%)", "Fecha última anotación"]]
            df = df.rename(columns={
                "nombre": "Nombre o razón social",
                "nif": "NIF",
                "nacionalidad": "Nacionalidad",
                "domicilio": "Domicilio"
            })

            fila_total = {
                "Nombre o razón social": "TOTAL",
                "NIF": "",
                "Nacionalidad": "",
                "Domicilio": "",
                "Participaciones": total_part,
                "Cuota (%)": 100.0 if total_part > 0 else 0.0,
                "Fecha última anotación": ""
            }
            df = pd.concat([df, pd.DataFrame([fila_total])], ignore_index=True)

            def miles_es(x): 
                try:
                    return f"{int(x):,}".replace(",", ".")
                except:
                    return x
            df["Participaciones"] = df["Participaciones"].map(miles_es)
            df["Cuota (%)"] = df["Cuota (%)"].map(lambda x: f"{x:.4f}%" if isinstance(x, (int, float)) else x)

            st.dataframe(df, use_container_width=True, height=TABLE_H_BIG)
        else:
            st.info("No hay socios dados de alta todavía")
            
# ================= EVENTOS =================
with tabs[2]:
    st.subheader("Eventos societarios")

    # --- Selector de sociedad ---
    with get_connection() as conn:
        dfc_e = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
    ev_options = ["(elige)"] + [fmt_company(row) for _, row in dfc_e.iterrows()]
    ev_choice = st.selectbox("Sociedad", ev_options, key="ev_company_selector")

    if ev_choice == "(elige)":
        st.info("Elige primero una sociedad para gestionar sus eventos.")
        st.stop()

    company_id_ev = int(ev_choice.split(" – ")[0])

    # --- Carga base de eventos (raw y legible) ---
    with get_connection() as conn:
        dfe_raw = pd.read_sql_query(
            "SELECT * FROM events WHERE company_id=? ORDER BY fecha, id",
            conn, params=(company_id_ev,)
        )
        dfe_legible = pd.read_sql_query("""
            SELECT e.*,
                   pt.nombre AS transmite_nombre,
                   pa.nombre AS adquiere_nombre
            FROM events e
            LEFT JOIN partners pt ON pt.id = e.socio_transmite
            LEFT JOIN partners pa ON pa.id = e.socio_adquiere
            WHERE e.company_id = ?
            ORDER BY e.fecha, e.id
        """, conn, params=(company_id_ev,))

    # --- partners para selects ---
    with get_connection() as conn:
        df_partners = pd.read_sql_query(
            "SELECT id, nombre FROM partners WHERE company_id=? ORDER BY nombre",
            conn, params=(company_id_ev,)
        )
    partner_opts = ["(no aplica)"] + [f"{int(r.id)} – {r.nombre}" for _, r in df_partners.iterrows()]

    # --- Tablas arriba con filtro rápido (mejor UX) ---
    st.markdown("**Eventos (datos brutos)**")
    st.dataframe(dfe_raw, use_container_width=True, height=TABLE_H)

    # Vista legible + filtro
    dfe_view = dfe_legible.rename(columns={
        "fecha":"Fecha", "tipo":"Tipo",
        "transmite_nombre":"Socio transmite",
        "adquiere_nombre":"Socio adquiere",
        "rango_desde":"Desde", "rango_hasta":"Hasta",
        "participaciones":"Participaciones",
        "nuevo_valor_nominal":"Nuevo valor nominal",
        "documento":"Documento", "observaciones":"Observaciones"
    })[["Fecha","Tipo","Socio transmite","Socio adquiere","Desde","Hasta","Participaciones","Nuevo valor nominal","Documento","Observaciones"]]

    st.markdown("**Vista legible**")
    q_ev = st.text_input("Filtro rápido (por texto)", placeholder="Escribe para filtrar…", key="ev_quick_filter")
    try:
        df_ev_filtrado = AH.filter_df_by_query(dfe_view.copy(), q_ev, cols=None)
    except Exception:
        df_ev_filtrado = dfe_view
    st.dataframe(df_ev_filtrado, use_container_width=True, height=TABLE_H)

    st.divider()

    # ========== ➕ ALTA ==========
    with st.expander("➕ Alta de evento", expanded=True):
        with st.form("form_event_add", clear_on_submit=False):
            fecha = st.date_input("Fecha", value=date.today(), min_value=MIN_DATE, max_value=MAX_DATE)
            tipo = st.selectbox("Tipo", TIPOS_EVENTO, key="tipo_add")
            socio_t_lbl = st.selectbox("Socio transmite / titular (si aplica)", partner_opts, index=0)
            socio_a_lbl = st.selectbox("Socio adquiere / acreedor (si aplica)", partner_opts, index=0)

            d = st.number_input("Desde", min_value=1, value=1, step=1)
            h = st.number_input("Hasta", min_value=1, value=1, step=1)
            part = st.number_input("Participaciones (informativo)", min_value=0, value=0, step=1)

            vnuevo = st.number_input("Nuevo valor nominal (si aplica)", min_value=0.0, value=0.0, step=0.0001, format="%.4f")
            st.caption("ℹ️ En 'REDENOMINACION' el 'Nuevo valor nominal' es obligatorio y > 0.")

            doc = st.text_input("Documento/Escritura")
            obs = st.text_area("Observaciones")
            confirm = st.checkbox("✅ Confirmo que los datos son correctos")

            # --- Validación preventiva ---
            needs_range = tipo in {"ALTA","AMPL_EMISION","TRANSMISION","BAJA","RED_AMORT","PIGNORACION","EMBARGO","USUFRUCTO"}
            valid_rango = (not needs_range) or (d <= h)
            valid_nom = (tipo != "REDENOMINACION") or (vnuevo > 0)
            valid = (fecha is not None) and valid_rango and valid_nom and confirm

            submit_add = st.form_submit_button("💾 Guardar evento", type="primary", disabled=not valid)

        if submit_add:
            socio_t = label_to_id(socio_t_lbl)
            socio_a = label_to_id(socio_a_lbl)

            # Nominal según tipo
            if tipo in {"AMPL_VALOR", "RED_VALOR"}:
                nv = float(vnuevo)
            elif tipo == "REDENOMINACION":
                nv = float(vnuevo) if (vnuevo is not None and float(vnuevo) > 0) else None
            else:
                nv = None

            # Reglas de rango
            is_reden_global = (tipo == "REDENOMINACION" and socio_t is None and socio_a is None)
            d_sql = int(d) if ((needs_range or tipo == "REDENOMINACION") and not is_reden_global) else None
            h_sql = int(h) if ((needs_range or tipo == "REDENOMINACION") and not is_reden_global) else None

            ev_payload = dict(
                company_id=company_id_ev, tipo=tipo, fecha=str(fecha),
                socio_transmite=socio_t, socio_adquiere=socio_a,
                rango_desde=d_sql, rango_hasta=h_sql,
                nuevo_valor_nominal=nv, participaciones=(part or None),
                documento=doc, observaciones=obs
            )

            # Validación de dominio extra
            with get_connection() as _conn:
                errs = svc.validate_event(_conn, {k: ev_payload[k] for k in ["company_id","tipo","fecha","socio_transmite","socio_adquiere","rango_desde","rango_hasta","nuevo_valor_nominal"]})

            if errs:
                st.error(" • " + "\n • ".join(errs))
            else:
                import sqlite3 as _sqlite3
                try:
                    with get_connection() as conn:
                        conn.execute("""
                            INSERT INTO events
                            (company_id,fecha,tipo,socio_transmite,socio_adquiere,
                             rango_desde,rango_hasta,participaciones,nuevo_valor_nominal,documento,observaciones)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            ev_payload["company_id"], fecha, tipo, socio_t, socio_a,
                            d_sql, h_sql, ev_payload["participaciones"], nv, doc, obs
                        ))
                        # id recién creado (para deshacer)
                        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                        conn.commit()

                    # Push undo: borrar ese id
                    st.session_state["undo_stack"].append((
                        "sql",
                        "DELETE FROM events WHERE id=?",
                        (new_id,)
                    ))
                    st.toast("✅ Evento creado")
                    st.rerun()

                except _sqlite3.IntegrityError as e:
                    AH.log_exception(e, where="events.add.integrity", extra={"company_id": company_id_ev, "tipo": tipo})
                    st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
                except Exception as e:
                    AH.log_exception(e, where="events.add", extra={"company_id": company_id_ev, "tipo": tipo})
                    st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    # ========== ✏️ EDITAR ==========
    with st.expander("✏️ Editar evento", expanded=False):
        if dfe_raw.empty:
            st.info("No hay eventos aún.")
        else:
            ev_opts = [f"{int(r.id)} – {r.tipo} ({r.fecha})" for _, r in dfe_raw.iterrows()]
            ev_sel = st.selectbox("Evento a editar", ev_opts, key="ev_edit_selector")
            ev_id = int(ev_sel.split(" – ")[0])
            ev_row = dfe_raw[dfe_raw["id"] == ev_id].iloc[0]

            # Defaults seguros
            d_default = safe_int(ev_row.get("rango_desde"), 1)
            h_default = safe_int(ev_row.get("rango_hasta"), 1)
            part_default = safe_int(ev_row.get("participaciones"), 0)
            vn_default = safe_float(ev_row.get("nuevo_valor_nominal"), 0.0)

            socio_t_pref_lbl = id_to_label(df_partners, ev_row.get("socio_transmite"))
            socio_a_pref_lbl = id_to_label(df_partners, ev_row.get("socio_adquiere"))
            socio_t_idx = partner_opts.index(socio_t_pref_lbl) if socio_t_pref_lbl in partner_opts else 0
            socio_a_idx = partner_opts.index(socio_a_pref_lbl) if socio_a_pref_lbl in partner_opts else 0

            fecha_val = pd.to_datetime(ev_row["fecha"]).date()
            fecha_val = max(MIN_DATE, min(fecha_val, MAX_DATE))

            with st.form("form_event_edit"):
                fecha_e = st.date_input("Fecha", value=fecha_val, min_value=MIN_DATE, max_value=MAX_DATE)
                tipo_e = st.selectbox("Tipo", TIPOS_EVENTO, index=TIPOS_EVENTO.index(ev_row["tipo"]))
                socio_t_e_lbl = st.selectbox("Socio transmite / titular (si aplica)", partner_opts, index=socio_t_idx)
                socio_a_e_lbl = st.selectbox("Socio adquiere / acreedor (si aplica)", partner_opts, index=socio_a_idx)
                d_e = st.number_input("Desde", min_value=1, value=d_default, step=1)
                h_e = st.number_input("Hasta", min_value=1, value=h_default, step=1)
                part_e = st.number_input("Participaciones (informativo)", min_value=0, value=part_default, step=1)
                vnuevo_e = st.number_input("Nuevo valor nominal (si aplica)", min_value=0.0, value=vn_default, step=0.0001, format="%.4f")
                doc_e = st.text_input("Documento/Escritura", value=ev_row["documento"] or "")
                obs_e = st.text_area("Observaciones", value=ev_row["observaciones"] or "")

                # Validación preventiva
                needs_range_e = tipo_e in {"ALTA","AMPL_EMISION","TRANSMISION","BAJA","RED_AMORT","PIGNORACION","EMBARGO","USUFRUCTO"}
                valid_rango_e = (not needs_range_e) or (d_e <= h_e)
                valid_nom_e = (tipo_e != "REDENOMINACION") or (vnuevo_e > 0)
                valid_e = (fecha_e is not None) and valid_rango_e and valid_nom_e

                colu1, colu2 = st.columns(2)
                with colu1:
                    save_ev = st.form_submit_button("💾 Guardar cambios", type="primary", disabled=not valid_e)
                with colu2:
                    ask_delete = st.form_submit_button("🗑️ Borrar evento", type="secondary")

            if save_ev:
                socio_t_e = label_to_id(socio_t_e_lbl)
                socio_a_e = label_to_id(socio_a_e_lbl)

                if tipo_e in {"AMPL_VALOR", "RED_VALOR"}:
                    nv_e = float(vnuevo_e)
                elif tipo_e == "REDENOMINACION":
                    nv_e = float(vnuevo_e) if (vnuevo_e is not None and float(vnuevo_e) > 0) else None
                else:
                    nv_e = None

                is_reden_global_e = (tipo_e == "REDENOMINACION" and socio_t_e is None and socio_a_e is None)
                d_sql_e = int(d_e) if ((needs_range_e or tipo_e == "REDENOMINACION") and not is_reden_global_e) else None
                h_sql_e = int(h_e) if ((needs_range_e or tipo_e == "REDENOMINACION") and not is_reden_global_e) else None

                ev_e = dict(
                    company_id=company_id_ev, tipo=tipo_e, fecha=str(fecha_e),
                    socio_transmite=socio_t_e, socio_adquiere=socio_a_e,
                    rango_desde=d_sql_e, rango_hasta=h_sql_e,
                    nuevo_valor_nominal=nv_e
                )

                # Snapshot BEFORE para reversa UPDATE
                with get_connection() as conn:
                    row_before = conn.execute("SELECT * FROM events WHERE id=?", (ev_id,)).fetchone()
                    if row_before is None:
                        st.error("No se pudo cargar el evento antes de editar.")
                    else:
                        cols_b = [k for k in row_before.keys() if k != "id"]
                        set_clause_b = ", ".join([f"{c}=?" for c in cols_b])
                        params_rev = tuple([row_before[c] for c in cols_b] + [ev_id])

                        # Valida negocio
                        errs = svc.validate_event(conn, ev_e)

                if 'errs' in locals() and errs:
                    st.error(" • " + "\n • ".join(errs))
                else:
                    import sqlite3 as _sqlite3
                    try:
                        with get_connection() as conn:
                            conn.execute("""
                                UPDATE events
                                SET fecha=?, tipo=?, socio_transmite=?, socio_adquiere=?,
                                    rango_desde=?, rango_hasta=?, participaciones=?,
                                    nuevo_valor_nominal=?, documento=?, observaciones=?
                                WHERE id=? AND company_id=?
                            """, (fecha_e, tipo_e, socio_t_e, socio_a_e,
                                  d_sql_e, h_sql_e, (part_e or None),
                                  nv_e, doc_e, obs_e, ev_id, company_id_ev))
                            conn.commit()

                        # Push undo (reversa del UPDATE con snapshot previo)
                        st.session_state["undo_stack"].append((
                            "sql",
                            f"UPDATE events SET {set_clause_b} WHERE id=?",
                            params_rev
                        ))

                        st.toast("✅ Evento actualizado")
                        st.rerun()

                    except _sqlite3.IntegrityError as e:
                        AH.log_exception(e, where="events.edit.integrity", extra={"event_id": ev_id})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
                    except Exception as e:
                        AH.log_exception(e, where="events.edit", extra={"event_id": ev_id})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

            if ask_delete:
                st.warning("Confirma el borrado en el expander de abajo ('🗑️ Borrar evento').")

    # ========== 🗑️ BORRAR ==========
    with st.expander("🗑️ Borrar evento", expanded=False):
        if dfe_raw.empty:
            st.info("No hay eventos aún.")
        else:
            ev_opts_del = [f"{int(r.id)} – {r.tipo} ({r.fecha})" for _, r in dfe_raw.iterrows()]
            ev_sel_del = st.selectbox("Evento a borrar", ev_opts_del, key="ev_delete_selector")
            ev_id_del = int(ev_sel_del.split(" – ")[0])

            confirm_del = st.checkbox("🛑 Confirmo que deseo borrar el evento seleccionado", key="ev_delete_confirm")
            if st.button("🗑️ Borrar definitivamente", type="secondary", disabled=not confirm_del):
                # Snapshot para poder restaurar (INSERT reverso)
                with get_connection() as conn:
                    row_del = conn.execute("SELECT * FROM events WHERE id=?", (ev_id_del,)).fetchone()
                if row_del is None:
                    st.error("No se pudo cargar el evento a borrar.")
                else:
                    cols = list(row_del.keys())
                    qs = ", ".join(["?"] * len(cols))
                    params_ins = tuple(row_del[c] for c in cols)

                    import sqlite3 as _sqlite3
                    try:
                        with get_connection() as conn:
                            conn.execute("DELETE FROM events WHERE id=? AND company_id=?", (ev_id_del, company_id_ev))
                            conn.commit()

                        # Push undo: INSERT con todos los campos del snapshot
                        st.session_state["undo_stack"].append((
                            "sql",
                            f"INSERT INTO events ({', '.join(cols)}) VALUES ({qs})",
                            params_ins
                        ))

                        st.toast("🗑️ Evento borrado")
                        st.rerun()

                    except _sqlite3.IntegrityError as e:
                        AH.log_exception(e, where="events.delete.integrity", extra={"event_id": ev_id_del})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
                    except Exception as e:
                        AH.log_exception(e, where="events.delete", extra={"event_id": ev_id_del})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    # ========== 🏛️ GOBERNANZA ==========
    with st.expander("🏛️ Gobernanza (ayuda)", expanded=False):
        st.caption("ℹ️ Recuerda: las operaciones de 'REDENOMINACION' pueden ser globales (sin socio) o por socio (con rangos).")
        st.caption("ℹ️ Valida siempre rangos coherentes (Desde ≤ Hasta) y, si aplica, el 'Nuevo valor nominal' > 0.")
        st.caption("ℹ️ Los cambios quedan reflejados en el PDF/Excel legalizable.")

# ================= RECALCULAR / INVENTARIO =================
with tabs[3]:
    st.subheader("Recalcular inventario desde eventos")
    with get_connection() as conn:
        dfc_r = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
    r_options = ["(elige)"] + [fmt_company(row) for _, row in dfc_r.iterrows()]
    r_choice = st.selectbox("Sociedad", r_options, key="recalc_company_selector")
    if r_choice != "(elige)":
        company_id_r = int(r_choice.split(" – ")[0])
        if st.button("🔄 Recalcular ahora"):
            try:
                with get_connection() as conn:
                    svc.recompute_company(conn, company_id_r)
                st.success("Inventario recalculado ✅"); st.rerun()
            except Exception as e:
                AH.log_exception(e, where="inventory.recompute", extra={"company_id": company_id_r})
                st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
        with get_connection() as conn:
            dfh = pd.read_sql_query("""
                SELECT h.*, p.nombre
                FROM holdings h JOIN partners p ON p.id = h.socio_id
                WHERE h.company_id=? ORDER BY p.nombre, rango_desde
            """, conn, params=(company_id_r,))
        st.markdown("**Holdings vigentes**")
        q_hold = st.text_input("Filtro rápido (por texto)", placeholder="Socio, rangos, notas…", key="q_holdings")
        try:
            dfh_view = AH.filter_df_by_query(dfh.copy(), q_hold, cols=None)
        except Exception:
            dfh_view = dfh

        st.dataframe(dfh_view if 'dfh_view' in locals() else dfh, use_container_width=True, height=TABLE_H_BIG)
        with get_connection() as conn:
            snap = svc.snapshot_socios(conn, company_id_r)
        st.markdown("**Socios actuales (plena propiedad)**")
        df_snap2 = pd.DataFrame(snap)
        q_snap = st.text_input("Filtro rápido (por texto)", placeholder="Nombre, participaciones…", key="q_snapshot")
        try:
            df_snap_view = AH.filter_df_by_query(df_snap2.copy(), q_snap, cols=None)
        except Exception:
            df_snap_view = df_snap2

        st.dataframe(df_snap_view if 'df_snap_view' in locals() else pd.DataFrame(snap), use_container_width=True, height=TABLE_H)
    else:
        st.info("Elige una sociedad para ver su inventario.")

# ================= EXPORTAR =================
with tabs[4]:
    st.subheader("Exportar")

    # CSV rápidos (con nombres)
    st.markdown("#### CSV rápidos (con nombres en movimientos)")
    with get_connection() as conn:
        dfc_x = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
    x_options = ["(elige)"] + [fmt_company(row) for _, row in dfc_x.iterrows()]
    x_choice = st.selectbox("Sociedad (CSV)", x_options, key="export_company_selector_csv")

    if x_choice != "(elige)":
        company_id_x = int(x_choice.split(" – ")[0])
        with get_connection() as conn:
            dfh = pd.read_sql_query("""
                SELECT h.*, p.nombre
                FROM holdings h JOIN partners p ON p.id=h.socio_id
                WHERE h.company_id=?""",
                conn, params=(company_id_x,)
            )

            dfe = pd.read_sql_query("""
                SELECT e.fecha AS 'Fecha',
                       e.tipo  AS 'Tipo',
                       pt.nombre AS 'Socio transmite',
                       pa.nombre AS 'Socio adquiere',
                       e.rango_desde AS 'Desde',
                       e.rango_hasta AS 'Hasta',
                       e.participaciones AS 'Participaciones',
                       e.nuevo_valor_nominal AS 'Nuevo valor nominal',
                       e.documento AS 'Documento',
                       e.observaciones AS 'Observaciones'
                FROM events e
                LEFT JOIN partners pt ON pt.id = e.socio_transmite
                LEFT JOIN partners pa ON pa.id = e.socio_adquiere
                WHERE e.company_id = ?
                ORDER BY e.fecha, e.id
            """, conn, params=(company_id_x,))

        st.download_button("📤 Descargar holdings.csv", dfh.to_csv(index=False).encode("utf-8"), "holdings.csv", "text/csv")
        st.download_button("📤 Descargar eventos.csv", dfe.to_csv(index=False).encode("utf-8"), "eventos.csv", "text/csv")

    st.divider()

    # Excel legalizable
    st.markdown("#### Excel legalizable (foto fija por fecha)")
    x2_choice = st.selectbox("Sociedad (Excel)", x_options, key="export_company_selector_xlsx")
    fecha_corte = st.date_input("Fecha de corte", value=date.today(), min_value=MIN_DATE, max_value=MAX_DATE, key="fecha_corte_export")
    if x2_choice != "(elige)":
        company_id_excel = int(x2_choice.split(" – ")[0])
        if st.button("📤 Generar Excel legalizable"):
            try:
                tmp_path = Path(EXPORT_DIR) / "libro_socios.xlsx"
                with get_connection() as conn:
                    svc.export_excel(conn, company_id_excel, str(fecha_corte), str(tmp_path))
                with open(tmp_path, "rb") as f:
                    st.download_button(
                        "Descargar libro_socios.xlsx",
                        f.read(),
                        file_name=f"Libro_Registro_Socios_{fecha_corte}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                st.success("Excel generado ✅")
            except Exception as e:
                AH.log_exception(e, where="export.excel", extra={"company_id": company_id_excel, "fecha": str(fecha_corte)})
                st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

    st.divider()

    # PDF legalizable + hash
    st.markdown("#### PDF legalizable (con hash verificable)")
    x3_choice = st.selectbox("Sociedad (PDF)", x_options, key="export_company_selector_pdf")
    fecha_corte_pdf = st.date_input("Fecha de corte (PDF)", value=date.today(), min_value=MIN_DATE, max_value=MAX_DATE, key="fecha_corte_export_pdf")
    if x3_choice != "(elige)":
        company_id_pdf = int(x3_choice.split(" – ")[0])
        if st.button("📤 Generar PDF legalizable"):
            try:
                tmp_pdf = Path(EXPORT_DIR) / "libro_socios.pdf"
                with get_connection() as conn:
                    svc.export_pdf(conn, company_id_pdf, str(fecha_corte_pdf), str(tmp_pdf))
                with open(tmp_pdf, "rb") as f:
                    st.download_button(
                        "Descargar libro_socios.pdf",
                        f.read(),
                        file_name=f"Libro_Registro_Socios_{fecha_corte_pdf}.pdf",
                        mime="application/pdf"
                    )
                try:
                    os.remove(tmp_pdf)
                except Exception:
                    pass
                st.success("PDF generado ✅ (incluye hash SHA-256 en portada adicional)")
            except Exception as e:
                AH.log_exception(e, where="export.pdf", extra={"company_id": company_id_pdf, "fecha": str(fecha_corte_pdf)})
                st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

# ================= ADMINISTRACIÓN =================
with tabs[5]:
    st.subheader("🛠️ Administración")

        # --- Borrar sociedades (reset selectivo) ---
    with st.expander("⚠️ Borrar sociedades (reset selectivo)", expanded=False):
        st.warning("Esta acción elimina definitivamente las sociedades seleccionadas y todos sus datos relacionados (socios, eventos, holdings).")

        # Cargar sociedades y métricas
        with get_connection() as conn:
            df_companies = pd.read_sql_query("SELECT id, name, cif FROM companies ORDER BY id", conn)
            df_counts = pd.read_sql_query("""
                SELECT c.id,
                       COALESCE(p.n,0) AS socios,
                       COALESCE(e.n,0) AS eventos,
                       COALESCE(h.n,0) AS holdings
                FROM companies c
                LEFT JOIN (SELECT company_id, COUNT(*) n FROM partners GROUP BY company_id) p ON p.company_id=c.id
                LEFT JOIN (SELECT company_id, COUNT(*) n FROM events GROUP BY company_id)   e ON e.company_id=c.id
                LEFT JOIN (SELECT company_id, COUNT(*) n FROM holdings GROUP BY company_id) h ON h.company_id=c.id
                ORDER BY c.id
            """, conn)

        if df_companies.empty:
            st.info("No hay sociedades en la base de datos.")
        else:
            # Buscador + opciones filtradas
            q_admin = st.text_input("Filtro rápido (por texto)", placeholder="Nombre o CIF…", key="q_admin_reset")
            try:
                df_companies_view = AH.filter_df_by_query(df_companies.copy(), q_admin, cols=None) if q_admin else df_companies
            except Exception:
                df_companies_view = df_companies

            options = [fmt_company(row) for _, row in df_companies_view.iterrows()]
            sel = st.multiselect("Selecciona sociedades a borrar", options, key="admin_del_multiselect")

            # Resumen de impacto
            if sel:
                sel_ids = [int(s.split(" – ")[0]) for s in sel]
                resumen = df_counts[df_counts["id"].isin(sel_ids)].rename(columns={
                    "id":"ID sociedad","socios":"Socios","eventos":"Eventos","holdings":"Holdings"
                })
                st.markdown("**Resumen de lo que se va a borrar**")
                st.dataframe(resumen.set_index("ID sociedad"), use_container_width=True, height=TABLE_H)

            colA, colB, colC = st.columns([3,2,2])
            with colA:
                confirm_txt = st.text_input("Escribe BORRAR para confirmar", value="", key="admin_del_confirm")
            with colB:
                do_backup = st.checkbox("He hecho copia de seguridad", value=False, key="admin_del_backup")
            with colC:
                reset_seq = st.checkbox(
                    "Reiniciar contadores de todas las tablas",
                    value=True,
                    help="Reinicia los AUTOINCREMENT de companies, partners, events y holdings.",
                    key="admin_del_reset_seq"
                )

            if st.button("🗑️ Borrar sociedades seleccionadas", type="secondary", disabled=not sel, key="admin_del_button"):
                if not sel:
                    st.error("No has seleccionado ninguna sociedad.")
                elif confirm_txt.strip().upper() != "BORRAR" or not do_backup:
                    st.error("Debes escribir BORRAR y marcar que has hecho copia.")
                else:
                    try:
                        with get_connection() as conn:
                            # Borrado en cascada manual por company_id
                            q_in = ",".join("?" for _ in sel)  # ?,?,?
                            ids = [int(s.split(" – ")[0]) for s in sel]
                            conn.execute(f"DELETE FROM holdings WHERE company_id IN ({q_in})", ids)
                            conn.execute(f"DELETE FROM events   WHERE company_id IN ({q_in})", ids)
                            conn.execute(f"DELETE FROM partners WHERE company_id IN ({q_in})", ids)
                            conn.execute(f"DELETE FROM companies WHERE id IN ({q_in})", ids)

                            if reset_seq:
                                try:
                                    conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('companies','partners','events','holdings')")
                                except Exception:
                                    pass

                            conn.commit()

                        st.toast("🗑️ Borrado completado")
                        st.success("Borrado completado ✅")
                        st.rerun()
                    except Exception as e:
                        AH.log_exception(e, where="admin.bulk_delete_companies", extra={"ids": ids} if "ids" in locals() else {})
                        st.error(f"⚠️ {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

        st.caption("Consejo: si prefieres un borrado total, lo más limpio es eliminar el fichero .db y dejar que init_db() lo regenere.")

    # --- Mantenimiento DB (WAL/VACUUM/ANALYZE) ---
    with st.expander("🔧 Mantenimiento DB (SQLite)", expanded=False):
        st.caption("Consolida WAL y desfragmenta el fichero. Seguro en local.")
        if st.button("🧹 Compactar DB (VACUUM + ANALYZE)", use_container_width=True, key="btn_compact_db"):
            with st.status("Compactando base de datos…", expanded=True) as status:
                ok, msg = svc.compact_database()
                st.write(msg)
                if ok:
                    status.update(label="Listo ✅", state="complete")
                    st.toast("DB compactada", icon="✅")
                else:
                    status.update(label="Falló ❌", state="error")
                    st.toast("Error al compactar", icon="⚠️")
                    
        # --- Copias de seguridad ---
    with st.expander("📦 Copias de seguridad (backup / restore)", expanded=False):
        st.caption("Crea una copia consistente del fichero .db y restaura desde una copia previa cuando lo necesites.")

        # ====== Crear backup ======
        col_b1, col_b2 = st.columns([2, 3])
        with col_b1:
            if st.button("📦 Copia de seguridad ahora", key="btn_backup_now", use_container_width=True):
                with st.status("Creando copia de seguridad…", expanded=True) as status:
                    try:
                        info = svc.backup_database(include_hash=True)
                        if info.get("ok"):
                            status.update(label="Backup creado ✅", state="complete")
                            st.success(f"Backup: {info['path']}")
                            st.write(f"Tamaño: {info['size_bytes']:,} bytes".replace(",", "."))
                            if info.get("sha256"):
                                st.code(info["sha256"], language="text")
                            st.toast("Copia de seguridad creada", icon="✅")
                        else:
                            status.update(label="Error creando backup ❌", state="error")
                            st.error(info.get("error") or "Error desconocido")
                    except Exception as e:
                        AH.log_exception(e, where="backup.create")
                        status.update(label="Error creando backup ❌", state="error")
                        st.error(f"Error creando backup: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")

        with col_b2:
            st.markdown("**Backups disponibles**")
            try:
                backups = svc.list_backups(limit=50)
            except Exception as e:
                AH.log_exception(e, where="backup.list")
                backups = []
                st.error(f"No se pudieron listar los backups: {AH.friendly_error(e)}")

            if backups:
                import pandas as _pd
                df_bk = _pd.DataFrame(backups)
                # columnas bonitas
                if not df_bk.empty:
                    df_bk = df_bk[["name", "mtime", "size_bytes", "path"]]
                    df_bk = df_bk.rename(columns={
                        "name": "Fichero",
                        "mtime": "Fecha",
                        "size_bytes": "Tamaño (bytes)",
                        "path": "Ruta"
                    })
                    st.dataframe(df_bk, use_container_width=True, height=TABLE_H)
            else:
                st.info(f"No hay backups en la carpeta '{BACKUP_DIR}/' aún.")

        st.divider()

        # ====== Restaurar backup ======
        st.markdown("### ⤴️ Restaurar copia")
        st.caption("Puedes restaurar **desde un backup listado** o **subiendo un .db**. Esta acción **sobrescribe** los datos actuales. Se hará un backup previo automático.")

        col_r1, col_r2 = st.columns(2)
        with col_r1:
            # Seleccionar uno de los backups ya existentes
            opciones = ["(elige)"] + [b["name"] for b in backups] if backups else ["(elige)"]
            sel_existing = st.selectbox("Elegir backup existente", opciones, key="restore_select_existing")
            selected_path = None
            if backups and sel_existing != "(elige)":
                # busca ruta completa por nombre
                match = next((b for b in backups if b["name"] == sel_existing), None)
                selected_path = match["path"] if match else None

        with col_r2:
            # Subir un .db para restaurar
            up = st.file_uploader("…o subir un fichero .db", type=["db"], key="restore_uploader")

        # Confirmación triple
        col_c1, col_c2, col_c3 = st.columns([3, 2, 2])
        with col_c1:
            confirm_txt = st.text_input("Escribe RESTAURAR para confirmar", value="", key="restore_confirm_text")
        with col_c2:
            confirm_box = st.checkbox("Entiendo que se sobrescribe el DB actual", key="restore_confirm_box")
        with col_c3:
            really_sure = st.checkbox("Sí, estoy completamente seguro", key="restore_confirm_box2")

        # Botón de restaurar
        can_restore = (confirm_txt.strip().upper() == "RESTAURAR") and confirm_box and really_sure and (selected_path or up)
        if st.button("⤴️ Restaurar copia", type="secondary", disabled=not can_restore, key="btn_restore_now", use_container_width=True):
            with st.status("Restaurando base de datos…", expanded=True) as status:
                try:
                    # Si suben un archivo, lo guardamos temporalmente
                    restore_path = selected_path
                    tmp_file = None
                    if up is not None:
                        # guardar a un fichero temporal en BACKUP_DIR
                        svc.ensure_backup_dir()
                        tmp_file = Path(BACKUP_DIR) / f"_restore_upload_{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
                        with open(tmp_file, "wb") as f:
                            f.write(up.read())
                        restore_path = str(tmp_file)

                    res = svc.restore_database_from_path(restore_path, create_pre_restore_backup=True)
                    if res.get("ok"):
                        status.update(label="Restauración completada ✅", state="complete")
                        st.success("BD restaurada con éxito.")
                        pre = res.get("pre_backup")
                        if isinstance(pre, dict) and pre.get("ok"):
                            st.caption(f"Se creó backup previo: {pre.get('path')}")
                        st.toast("Restauración correcta. Recargando…", icon="✅")
                        st.rerun()
                    else:
                        status.update(label="Falló la restauración ❌", state="error")
                        st.error(res.get("error") or "Error desconocido al restaurar")

                except Exception as e:
                    AH.log_exception(e, where="backup.restore", extra={"selected_path": selected_path})
                    status.update(label="Falló la restauración ❌", state="error")
                    st.error(f"Error restaurando: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
                finally:
                    # Limpieza de temporal si lo hubo
                    try:
                        if 'tmp_file' in locals() and tmp_file and Path(tmp_file).exists():
                            Path(tmp_file).unlink()
                    except Exception:
                        pass
                    
    # --- Autochequeo ---
    with st.expander("🔎 Autochequeo de consistencia", expanded=False):
        st.caption("Ejecuta pruebas básicas de consistencia en la base de datos.")

        if st.button("🔎 Ejecutar autochequeo", key="btn_autochequeo"):
            with st.status("Ejecutando autochequeo…", expanded=True) as status:
                try:
                    res = svc.run_autochequeo()
                    status.update(label="Autochequeo completado ✅", state="complete")

                    st.markdown(f"**Versión de esquema:** `{res.get('schema_version')}`")

                    st.markdown("**Conteos por tabla:**")

                    counts = res.get("counts", {})
                    if counts:
                        df_counts = pd.DataFrame(
                            [{"Tabla": k, "Registros": v if v is not None else "—"} for k, v in counts.items()]
                        )
                        st.dataframe(df_counts, use_container_width=True, height=TABLE_H)
                    else:
                        st.info("No se pudieron obtener los conteos.")

                    fk = res.get("fk_errors")
                    if isinstance(fk, list) and not fk:
                        st.success("✔️ Sin errores de llaves foráneas")
                    elif isinstance(fk, list):
                        st.error(f"⚠️ Se encontraron {len(fk)} errores de FK")
                        st.write(fk)
                    else:
                        st.error(f"Error ejecutando foreign_key_check: {fk}")

                    st.toast("Autochequeo finalizado", icon="✅")

                except Exception as e:
                    AH.log_exception(e, where="admin.autochequeo")
                    status.update(label="Falló ❌", state="error")
                    st.error(f"Error en autochequeo: {AH.friendly_error(e)}\n\nDetalle técnico: `{e}`")
    
    # --- (Espacio para futuras utilidades) ---
    with st.expander("🧩 Otras utilidades (próximamente)", expanded=False):
        st.caption("Aquí añadiremos backups/restauración, chequeos de consistencia, etc.")