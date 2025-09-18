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
    run_analyze, run_reindex, run_vacuum,
    recompute_correlativos, ensure_min_indexes,
)
from app.core.services.normalization_service import run_normalization
from app.infra.db import get_connection
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

    tabs = st.tabs([
        "💾 Backups",
        "🩺 Salud BD",
        "🧹 Mantenimiento",
        "📜 Logs",
        "📥 Importar CSV",
        "🧽 Normalización",
    ])

    # 1) BACKUPS
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

    # 2) SALUD BD
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
        
        # 3) CORRELATIVOS POR SOCIEDAD
        st.divider()
        st.markdown("### Correlativos por sociedad")

        with get_connection() as _conn:
            _conn.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
            _companies = _conn.execute("SELECT id, name FROM companies ORDER BY id").fetchall()

        names = ["(Todas)"] + [f"{r['id']} – {r['name']}" for r in _companies]
        choice = st.selectbox("Sociedad", names, index=0, key="recomp_company_selector")
        selected_company_id = None
        if choice != "(Todas)":
            try:
                selected_company_id = int(choice.split(" – ")[0])
            except Exception:
                selected_company_id = None

        c1, c2, c3 = st.columns(3)
        with c1:
            chk_partners = st.checkbox("Socios (partner_no)", value=True)
        with c2:
            chk_events = st.checkbox("Eventos (correlativo)", value=True)
        with c3:
            chk_govern = st.checkbox("Gobernanza (board_no)", value=True)

        btn = st.button("↻ Recalcular correlativos", use_container_width=True)
        if btn:
            if not any([chk_partners, chk_events, chk_govern]):
                st.warning("Selecciona al menos un ámbito (Socios / Eventos / Gobernanza).")
            else:
                selected = [x for x, ok in (("partners", chk_partners), ("events", chk_events), ("governance", chk_govern)) if ok]
                if len(selected) == 3:
                    scope = "all"
                elif len(selected) == 2 and set(selected) == {"partners", "events"}:
                    scope = "both"
                else:
                    scope = selected[0]
                try:
                    res = recompute_correlativos(company_id=selected_company_id, scope=scope)
                    st.success(
                        f"Hecho. Socios: {res.get('partners',0)} • "
                        f"Eventos: {res.get('events',0)} • "
                        f"Gobernanza: {res.get('governance',0)}"
                    )
                except Exception as e:
                    st.error(f"Error: {e}")  
        
        st.divider()
        st.markdown("### Índices SQL mínimos")
        coli1, coli2 = st.columns([1, 1])
        with coli1:
            st.code(
                "events(company_id, fecha, id)\n"
                "partners(company_id)\n"
                "(opcionales) events(company_id, correlativo), board_members(company_id)"
            )
        with coli2:
            if st.button("⚙️ Crear índices mínimos", use_container_width=True):
                try:
                    out = ensure_min_indexes()
                    st.success("Índices verificados ✅")
                    st.code("\n".join(out) or "(sin cambios)")
                except Exception as e:
                    st.error(f"Error creando/verificando índices: {e}")

    # 4) MANTENIMIENTO
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
                if do_analyze:  run_analyze()
                if do_reindex:  run_reindex()
                if do_vacuum:   run_vacuum()
                st.success("Operación finalizada.")

        st.caption("Sugerencias: ANALYZE tras cargas grandes; REINDEX si sospechas corrupción de índices; VACUUM para compactar.")

    # 5) LOGS
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
                            return False
                    else:
                        return query.lower() in line.lower()
                return True

            filtered = [ln for ln in lines if _keep(ln)]
            st.text("".join(filtered) if filtered else "(sin resultados)")

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

    # 6) IMPORTACIONES
    with tabs[4]:
        from app.ui.pages import imports as imports_page  # import perezoso para evitar ciclos
        imports_page.render(company_id)

    # 7) NORMALIZACIÓN
    with tabs[5]:
        st.markdown("### Normalización avanzada")
        st.caption("Limpia nombres y NIF/NIE/CIF. Puedes ejecutar en modo ‘dry-run’.")

        col_top1, col_top2 = st.columns([1, 1])
        with col_top1:
            scope = st.selectbox("Ámbito", ["both", "partners", "governance"], index=0)
        with col_top2:
            selected_company = company_id if company_id else None
            st.text_input("Sociedad (id)", value=str(selected_company or ""), disabled=True)

        col_opt1, col_opt2, col_opt3 = st.columns(3)
        with col_opt1:
            fix_names = st.checkbox("Corregir nombres", value=True)
        with col_opt2:
            fix_nif = st.checkbox("Corregir NIF/NIE/CIF", value=True)
        with col_opt3:
            remove_accents = st.checkbox("Quitar tildes", value=False)

        dry = st.toggle("Dry-run (simular sin escribir)", value=True)
        if st.button("🚿 Ejecutar normalización", use_container_width=True):
            try:
                res = run_normalization(
                    company_id=selected_company,
                    scope=scope,
                    fix_names=fix_names,
                    fix_nif=fix_nif,
                    remove_accents=remove_accents,
                    dry_run=dry,
                    sample_limit=30,
                )
                st.success("Normalización simulada." if dry else "Normalización aplicada.")
                st.json({
                    "resumen": {
                        "partners_cambiados": res["partners"]["changed"],
                        "gobernanza_cambiados": res["governance"]["changed"],
                        "dry_run": res["dry_run"]
                    }
                })
            except Exception as e:
                st.error(f"Error en normalización: {e}")

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