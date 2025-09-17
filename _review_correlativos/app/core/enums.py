# app/core/enums.py

# Lista “canónica” de cargos que aparecerán en el desplegable
GOVERNANCE_ROLES: list[str] = [
    "Administrador Único",
    "Presidente",
    "Vicepresidente",
    "Secretario",
    "Vicesecretario",
    "Secretario - Consejero",
    "Consejero Delegado",
    "Consejero",
    "Apoderado",
]

# Aliases heredados (v1) -> nombre canónico
GOVERNANCE_ROLE_ALIASES: dict[str, str] = {
    "admin_unico": "Administrador Único",
    "administrador_unico": "Administrador Único",
    "consejero_delegado": "Consejero Delegado",
    "vicepresidente": "Vicepresidente",
    "presidente": "Presidente",
    "secretario": "Secretario",
    "vicesecretario": "Vicesecretario",
    "secretario_consejero": "Secretario - Consejero",
    "sec_consejero": "Secretario - Consejero",
    "consejero": "Consejero",
    "apoderado": "Apoderado",
}

# Tipos canónicos (v1 + v2)
EVENT_TYPES: list[str] = [
    "ALTA",
    "AMPL_EMISION",
    "AMPL_VALOR",
    "TRANSMISION",
    "BAJA",
    "RED_AMORT",
    "RED_VALOR",
    "PIGNORACION",
    "EMBARGO",
    "CANCELA_PIGNORACION",  # ← nuevo
    "CANCELA_EMBARGO",      # ← nuevo
    "LEV_GRAVAMEN",         # opcional genérico
    "ALZAMIENTO",           # opcional genérico
    "USUFRUCTO",
    "REDENOMINACION",
    "SUCESION",
]

# Aliases (normalizamos varios literales a los canónicos)
EVENT_TYPE_ALIASES: dict[str, str] = {
    "TRASMISION": "TRANSMISION",
    "REDENOMINACIÓN": "REDENOMINACION",
    "REDENOM": "REDENOMINACION",
    "REDEN": "REDENOMINACION",
    # Sinónimos de cancelaciones:
    "LEVANTAMIENTO": "LEV_GRAVAMEN",
    "LEVANTAMIENTO_DE_GRAVAMEN": "LEV_GRAVAMEN",
    "ALZAR_EMBARGO": "ALZAMIENTO",
    "ALZAMIENTO_DE_EMBARGO": "ALZAMIENTO",
}

# Etiquetas legibles (por si quieres usarlas en UI/reportes)
EVENT_LABELS: dict[str, str] = {
    "ALTA": "Alta",
    "AMPL_EMISION": "Ampliación (emisión)",
    "AMPL_VALOR": "Aumento de VN",
    "TRANSMISION": "Transmisión",
    "BAJA": "Baja",
    "RED_AMORT": "Reducción (amortización)",
    "RED_VALOR": "Reducción de VN",
    "PIGNORACION": "Pignoración",
    "EMBARGO": "Embargo",
    "CANCELA_PIGNORACION": "Cancelación de pignoración",
    "CANCELA_EMBARGO": "Cancelación de embargo",
    "LEV_GRAVAMEN": "Levantamiento de gravamen",
    "ALZAMIENTO": "Alzamiento",
    "USUFRUCTO": "Usufructo",
    "REDENOMINACION": "Redenominación",
    "SUCESION": "Sucesión",
}

def normalize_event_type(t: str | None) -> str | None:
    if not t:
        return t
    t_up = t.strip().upper()
    if t_up in EVENT_TYPE_ALIASES:
        return EVENT_TYPE_ALIASES[t_up]
    return t_up