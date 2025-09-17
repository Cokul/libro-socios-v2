# app/infra/pdf_fonts.py
from __future__ import annotations
from pathlib import Path
import logging
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

log = logging.getLogger(__name__)

# Rutas donde buscar las fuentes (TU estructura: app/assets/fonts)
# app/infra/pdf_fonts.py -> parents[1] = app/
_APP_DIR = Path(__file__).resolve().parents[1]
_CANDIDATE_DIRS = [
    _APP_DIR / "assets" / "fonts",      # ← app/assets/fonts   ✅
    Path.cwd() / "app" / "assets" / "fonts",  # por si cwd cambia
]

_FILES = {
    "DejaVuSans":              "DejaVuSans.ttf",
    "DejaVuSans-Bold":         "DejaVuSans-Bold.ttf",
    "DejaVuSans-Oblique":      "DejaVuSans-Oblique.ttf",
    "DejaVuSans-BoldOblique":  "DejaVuSans-BoldOblique.ttf",
}

_registered = False

def _find(pathname: str) -> Path | None:
    for base in _CANDIDATE_DIRS:
        p = base / pathname
        if p.exists():
            return p
    return None

def register_fonts():
    """Registra DejaVuSans* si existen en app/assets/fonts."""
    global _registered
    if _registered:
        return

    missing = []
    for face, fname in _FILES.items():
        p = _find(fname)
        if not p:
            missing.append(fname)
            continue
        try:
            pdfmetrics.registerFont(TTFont(face, str(p)))
            log.info("Fuente registrada: %s -> %s", face, p)
        except Exception as e:
            log.warning("No se pudo registrar %s (%s): %s", face, p, e)

    if missing:
        log.warning(
            "Faltan fuentes DejaVu: %s. Si persiste el error, usa Helvetica como fallback.",
            ", ".join(missing),
        )
    _registered = True