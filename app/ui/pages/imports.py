# app/ui/pages/imports.py
from __future__ import annotations
import logging
import io

import pandas as pd
import streamlit as st

from app.core.services.import_service import dry_run, commit, get_csv_template, DryRunRow

log = logging.getLogger(__name__)

MAX_PREVIEW_ROWS = 200  # límite de filas a mostrar en previsualización


def _rows_to_dataframe(rows: list[DryRunRow]) -> pd.DataFrame:
    """
    Construye un DataFrame amigable con columnas:
    row, ok, errores, ...campos normalizados...
    """
    out = []
    for r in rows[:MAX_PREVIEW_ROWS]:
        norm = r.normalized or {}
        out.append({
            "fila_csv": r.rownum,
            "ok": (len(r.errors) == 0),
            "errores": "; ".join(r.errors) if r.errors else "",
            **norm
        })
    return pd.DataFrame(out)


def render(company_id: int):
    st.subheader("📥 Importación CSV (beta)")

    st.markdown(
        "Carga un CSV en **UTF-8** separado por comas. Primero se realiza un **dry-run** "
        "para validar y previsualizar. Si no hay errores, podrás **confirmar la importación**."
    )

    kind = st.selectbox("Ámbito a importar", ["partners", "events"], index=0)

    # Descarga de plantillas
    ctpl1, ctpl2 = st.columns(2)
    with ctpl1:
        if st.button("📄 Descargar plantilla de este ámbito"):
            buf = get_csv_template(kind)
            st.download_button(
                "Descargar CSV de ejemplo",
                data=buf,
                file_name=f"plantilla_{kind}.csv",
                mime="text/csv",
                use_container_width=True
            )
    with ctpl2:
        st.caption("Las plantillas incluyen encabezados y 2 filas de ejemplo.")

    st.divider()

    # Subida de fichero
    up = st.file_uploader("Sube tu CSV", type=["csv"], accept_multiple_files=False, key="csv_upload")
    if not up:
        st.info("Selecciona un archivo para continuar.")
        return

    # Dry run
    st.markdown("### 1) Dry-run (validación)")
    if st.button("🔍 Validar CSV (dry-run)", use_container_width=True):
        try:
            report = dry_run(kind, company_id, up.getvalue())
            if report.errors:
                st.error("Errores globales en el archivo:")
                for e in report.errors:
                    st.write(f"• {e}")

            st.metric("Filas totales", report.total_rows)
            st.metric("Filas OK", report.ok_rows)
            st.metric("Filas con error", report.error_rows)

            df_preview = _rows_to_dataframe(report.rows)
            if not df_preview.empty:
                st.dataframe(df_preview, use_container_width=True, hide_index=True)
            else:
                st.info("No hay filas para mostrar.")

            # Guardamos en sesión las filas normalizadas OK para el commit
            st.session_state["import_last_kind"] = kind
            st.session_state["import_last_company"] = company_id
            st.session_state["import_rows_ok"] = [
                r.normalized for r in report.rows if r.errors == []
            ]
            st.session_state["import_last_errs"] = [
                (r.rownum, r.errors) for r in report.rows if r.errors
            ]

            if report.error_rows == 0 and report.ok_rows > 0:
                st.success("Dry-run sin errores. Puedes confirmar la importación más abajo.")
            elif report.error_rows > 0:
                st.warning("Hay filas con errores. Corrige tu CSV y vuelve a intentarlo.")

        except Exception as e:
            log.error("Error en dry-run import %s: %s", kind, e, exc_info=True)
            st.error(str(e))

    st.markdown("### 2) Confirmar importación")
    ok_rows = st.session_state.get("import_rows_ok", [])
    last_kind = st.session_state.get("import_last_kind")
    last_company = st.session_state.get("import_last_company")

    disabled = not (ok_rows and last_kind == kind and last_company == company_id)

    if st.button("✅ Importar (commit transaccional)", disabled=disabled, use_container_width=True):
        if disabled:
            st.info("Primero realiza un dry-run válido.")
        else:
            try:
                summary = commit(kind, company_id, ok_rows)
                if summary.errors:
                    st.error("Se produjo un error y no se importó nada.")
                    st.code("\n".join(summary.errors))
                else:
                    if kind == "partners":
                        st.success(f"Completado: insertados {summary.inserted}, actualizados {summary.updated}.")
                    else:
                        st.success(f"Completado: insertados {summary.inserted}.")
                    # Limpiar estado para evitar reimportes accidentales
                    st.session_state["import_rows_ok"] = []
            except Exception as e:
                st.error(str(e))