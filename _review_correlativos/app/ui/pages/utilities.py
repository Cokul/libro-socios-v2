# app/ui/pages/utilities.py
from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime
from io import BytesIO
import streamlit as st

from app.core.services.backup_service import (
    create_backup, list_backups, restore_backup, BK_DIR
)
from app.core.services.maintenance_service import (
    db_quick_summary, db_integrity_check, db_fk_check,
    run_analyze, run_reindex, run_vacuum
)
from app.infra.logging import LOG_FILE

log = logging.getLogger(__name__)

LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

def _read_tail(path: Path, max_lines: int) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    return lines[-max_lines:]

def render(company_id: int | None = None):
    st.subheader("🛠️ Utilidades")

    tabs = st.tabs(["💾 Backups", "🩺 Salud BD", "🧹 Mantenimiento", "📜 Logs"])

    # =========================
    # 1) BACKUPS
    # =========================
    with tabs[0]:
        st.markdown("### Copias de seguridad")
        colA, colB = st.columns([1, 2], gap="large")

        # Crear backup
        with colA:
            with st.form("form_create_backup", clear_on_submit=False):
                st.caption("Crea una copia de la base de datos actual en `data/backups/`.")
                submit = st.form_submit_button("🧩 Crear backup ahora", use_container_width=True)
                if submit:
                    try:
                        with st.status("Creando backup…", expanded=True) as status:
                            created = create_backup()
                            status.update(label="Backup creado ✅", state="complete")
                        st.success(f"Backup creado: {', '.join(p.name for p in created)}")
                        st.rerun()
                    except Exception as e:
                        log.error("Error creando backup: %s", e, exc_info=True)
                        st.error(f"Error creando backup: {e}")

        # Restaurar / descargar
        with colB:
            backups = list_backups()
            if not backups:
                st.info(f"No hay backups en `{BK_DIR}`.")
            else:
                names = [bk.name for bk in backups]
                idx_default = max(0, len(names) - 1)
                with st.form("form_restore_backup", clear_on_submit=False):
                    st.caption("Selecciona y restaura un backup existente. Se hará copia previa del actual.")
                    sel_name = st.selectbox("Backup (.db) disponible", names, index=idx_default, key="bk_pick")
                    cols = st.columns([1, 1, 2])
                    with cols[0]:
                        down = st.form_submit_button("⬇️ Descargar seleccionado")
                    with cols[1]:
                        confirm = st.text_input("Confirmación", value="", placeholder="Escribe RESTAURAR")
                    with cols[2]:
                        do_restore = st.form_submit_button(
                            "⚠️ Restaurar seleccionado",
                            disabled=(confirm != "RESTAURAR"),
                            use_container_width=True
                        )

                    # Descargar
                    if down:
                        picked = BK_DIR / sel_name
                        if picked.exists():
                            with picked.open("rb") as f:
                                st.download_button(
                                    "Descargar ahora",
                                    data=f.read(),
                                    file_name=picked.name,
                                    mime="application/octet-stream",
                                    use_container_width=True
                                )
                        else:
                            st.error("El backup seleccionado ya no existe.")

                    # Restaurar
                    if do_restore:
                        try:
                            with st.status("Restaurando backup…", expanded=True) as status:
                                restored = restore_backup(BK_DIR / sel_name)
                                status.update(label="Restauración completada ✅", state="complete")
                            st.success(f"Restaurado: {', '.join(p.name for p in restored)}. Reinicia la app si es necesario.")
                        except Exception as e:
                            log.error("Error restaurando backup: %s", e, exc_info=True)
                            st.error(f"Error restaurando backup: {e}")

        st.caption(f"BD activa: `{(BK_DIR.parent / 'libro_socios.db')}`  •  Carpeta de backups: `{BK_DIR}`")

    # =========================
    # 2) SALUD BD
    # =========================
    with tabs[1]:
        st.markdown("### Comprobaciones de integridad")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔎 Comprobar integridad / FK", use_container_width=True, key="btn_check_both"):
                with st.status("Ejecutando PRAGMA integrity_check y foreign_key_check…", expanded=True) as status:
                    summary = db_quick_summary()
                    status.update(label="Comprobaciones finalizadas ✅", state="complete")
                _render_health_summary(summary)

        with c2:
            st.caption("Comprueba integridad de la BD y violaciones de claves foráneas.")

        st.divider()

        col3, col4, col5 = st.columns(3)
        with col3:
            if st.button("Solo integrity_check", use_container_width=True, key="btn_integrity"):
                msgs = db_integrity_check()
                if not msgs:
                    st.success("OK: integrity_check sin incidencias.")
                else:
                    st.error("PRAGMA integrity_check devolvió incidencias:")
                    st.code("\n".join(msgs) or "(sin detalles)")
        with col4:
            if st.button("Solo foreign_key_check", use_container_width=True, key="btn_fk"):
                fks = db_fk_check()
                if not fks:
                    st.success("OK: foreign_key_check sin incidencias.")
                else:
                    st.error("Violaciones FK:")
                    lines = [f"tabla={t} rowid={r} fk_tabla={p} fk_id={fk}" for (t, r, p, fk) in fks]
                    st.code("\n".join(lines) or "(sin detalles)")
        with col5:
            st.caption("Botones individuales por si necesitas aislar un problema.")

    # =========================
    # 3) MANTENIMIENTO
    # =========================
    with tabs[2]:
        st.markdown("### Operaciones periódicas")
        with st.form("form_maintenance"):
            st.caption("Selecciona las tareas a ejecutar:")
            colx, coly, colz = st.columns(3)
            with colx:
                do_analyze = st.checkbox("ANALYZE", value=True)
            with coly:
                do_reindex = st.checkbox("REINDEX", value=False)
            with colz:
                do_vacuum = st.checkbox("VACUUM", value=False)

            run = st.form_submit_button("▶️ Ejecutar selección", use_container_width=True)

            if run:
                with st.status("Ejecutando mantenimiento…", expanded=True) as status:
                    if do_analyze:
                        status.write("• ANALYZE…"); run_analyze()
                    if do_reindex:
                        status.write("• REINDEX…"); run_reindex()
                    if do_vacuum:
                        status.write("• VACUUM…"); run_vacuum()
                    status.update(label="Mantenimiento completado ✅", state="complete")
                st.success("Operación finalizada.")

        st.caption("Sugerencias: ANALYZE tras cargas grandes; REINDEX si sospechas corrupción de índices; VACUUM para compactar.")

    # =========================
    # 4) LOGS
    # =========================
    with tabs[3]:
        st.markdown("### Visor de logs")
        if LOG_FILE.exists():
            size_kb = LOG_FILE.stat().st_size / 1024
            ts = datetime.fromtimestamp(LOG_FILE.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            st.caption(f"Ruta: `{LOG_FILE}` • Tamaño: {size_kb:.1f} KB • Últ. modif: {ts}")
        else:
            st.info(f"No se encontró el log en: {LOG_FILE}")

        with st.form("form_logs"):
            colA, colB, colC, colD = st.columns([1, 1.2, 1, 2])
            with colA:
                max_lines = st.number_input("Líneas", min_value=50, max_value=10000, value=500, step=50, key="log_lines")
            with colB:
                levels = st.multiselect("Niveles", LEVELS, default=["INFO", "WARNING", "ERROR", "CRITICAL"], key="log_levels")
            with colC:
                regex_mode = st.toggle("Regex", value=False, key="log_regex")
            with colD:
                query = st.text_input("Buscar", value="", placeholder=("expresión regular" if regex_mode else "contiene…"), key="log_query")

            submitted = st.form_submit_button("🔄 Mostrar / refrescar", use_container_width=True)

        if submitted and LOG_FILE.exists():
            lines = _read_tail(LOG_FILE, max_lines)

            def _keep(line: str) -> bool:
                if levels and not any(f" {lvl} " in line for lvl in levels):
                    return False
                if query:
                    if regex_mode:
                        import re
                        try:
                            return re.search(query, line, flags=re.IGNORECASE) is not None
                        except re.error:
                            # patrón inválido: devolvemos nada y mostramos aviso
                            return False
                    else:
                        return query.lower() in line.lower()
                return True

            filtered = [ln for ln in lines if _keep(ln)]
            st.text("".join(filtered) if filtered else "(sin resultados)")

            # Descarga (solo lo que se muestra)
            if filtered:
                buf = BytesIO("".join(filtered).encode("utf-8"))
                st.download_button(
                    "⬇️ Descargar líneas mostradas",
                    data=buf.getvalue(),
                    file_name="app_log_filtrado.txt",
                    mime="text/plain",
                    use_container_width=True
                )
        elif submitted and not LOG_FILE.exists():
            st.warning("No hay archivo de log para mostrar.")

def _render_health_summary(summary: dict):
    ok_i = summary.get("integrity_ok", False)
    ok_f = summary.get("fk_ok", False)
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Integrity check", "OK" if ok_i else "ERROR")
    with c2:
        st.metric("Foreign key check", "OK" if ok_f else "ERROR")

    if not ok_i:
        st.error("PRAGMA integrity_check devolvió incidencias:")
        st.code("\n".join(summary.get("integrity_messages", [])) or "(sin detalles)")

    if not ok_f:
        st.error("Violaciones FK:")
        lines = [
            f"tabla={t} rowid={r} fk_tabla={p} fk_id={fk}"
            for (t, r, p, fk) in summary.get("fk_violations", [])
        ]
        st.code("\n".join(lines) or "(sin detalles)")