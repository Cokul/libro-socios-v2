from __future__ import annotations
from pathlib import Path
from datetime import datetime
import shutil
import logging

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[3] / "data"
DB_FILE  = DATA_DIR / "libro_socios.db"
BK_DIR   = DATA_DIR / "backups"
BK_DIR.mkdir(parents=True, exist_ok=True)

def _sidecar_files(base: Path) -> list[Path]:
    # Soporta journal WAL de SQLite
    return [base.with_suffix(base.suffix + sfx) for sfx in ("-wal", "-shm")]

def create_backup() -> list[Path]:
    """
    Crea un backup consistente del fichero principal y sus sidecars si existen.
    Devuelve la lista de rutas creadas.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    created: list[Path] = []
    if not DB_FILE.exists():
        raise FileNotFoundError(f"No existe la BD: {DB_FILE}")

    dst_main = BK_DIR / f"libro_socios_{ts}.db"
    shutil.copy2(DB_FILE, dst_main)
    created.append(dst_main)

    for s in _sidecar_files(DB_FILE):
        if s.exists():
            dst = BK_DIR / f"{s.stem}_{ts}{s.suffix}".replace("-wal_", "_").replace("-shm_", "_")
            shutil.copy2(s, dst)
            created.append(dst)

    log.info("Backup creado: %s", ", ".join(str(p.name) for p in created))
    return created

def list_backups() -> list[Path]:
    # Muestra .db y posibles wal/shm asociados
    files = sorted(BK_DIR.glob("libro_socios_*.db"))
    return files

def restore_backup(backup_db_path: Path) -> list[Path]:
    """
    Restaura desde un .db de backups. Hace copia de seguridad del actual como _pre_restore_*.db
    Retorna lista de archivos restaurados.
    """
    if not backup_db_path.exists():
        raise FileNotFoundError(str(backup_db_path))

    restored: list[Path] = []

    # Copia de seguridad del actual
    safe = BK_DIR / f"_pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(DB_FILE, safe)
    log.warning("Backup previo (pre-restore) guardado como: %s", safe.name)

    # Restaurar principal
    shutil.copy2(backup_db_path, DB_FILE)
    restored.append(DB_FILE)

    # Limpiar/renombrar sidecars actuales si existen
    for s in _sidecar_files(DB_FILE):
        if s.exists():
            s.unlink(missing_ok=True)

    # Si existieran wal/shm con mismo timestamp, restáuralos
    stem = backup_db_path.stem.replace("libro_socios_", "")
    wal_src = BK_DIR / f"libro_socios_{stem}.db-wal"
    shm_src = BK_DIR / f"libro_socios_{stem}.db-shm"
    for src, dst in [(wal_src, DB_FILE.with_suffix(DB_FILE.suffix + "-wal")),
                     (shm_src, DB_FILE.with_suffix(DB_FILE.suffix + "-shm"))]:
        if src.exists():
            shutil.copy2(src, dst)
            restored.append(dst)

    log.warning("Restauración completada desde: %s", backup_db_path.name)
    return restored