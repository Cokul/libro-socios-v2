#app/core/validators.py

import re
def normalize_nif_cif(value: str | None) -> str | None:
    if not value:
        return value
    v = value.upper().strip().replace("-", "").replace(" ", "")
    return v
def validate_email(value: str | None) -> bool:
    if not value: 
        return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))
def normalize_phone(value: str | None) -> str | None:
    if not value:
        return value
    return value.strip().replace(" ", "")