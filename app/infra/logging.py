from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

LOG_DIR  = Path(__file__).resolve().parents[2] / "logs"
LOG_FILE = LOG_DIR / "app.log"

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()

    # Evita añadir más de un handler si Streamlit reejecuta
    for h in list(root.handlers):
        if isinstance(h, RotatingFileHandler) and Path(getattr(h, "baseFilename", "")) == LOG_FILE:
            return root

    root.setLevel(level)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    return root

__all__ = ["setup_logging", "LOG_FILE"]