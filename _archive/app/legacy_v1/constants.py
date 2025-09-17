# constants.py
TIPOS_EVENTO = [
    "ALTA", "AMPL_EMISION", "AMPL_VALOR", "TRASMISION",
    "RED_VALOR", "PIGNORACION", "EMBARGO", "USUFRUCTO", "REDENOMINACION"
]

ORGANO_GOB = [
    "Administrador único", "Administradores solidarios",
    "Administradores mancomunados", "Consejo de administración"
]

ROLES_CONSEJO = ["Presidente", "Secretario", "Secretario - Consejero", "Vocal", "Vicepresidente", "Consejero"]

LEGEND_TIPOS = {
    "ALTA": "Alta",
    "AMPL_EMISION": "Ampliación por emisión",
    "AMPL_VALOR": "Ampliación por valor",
    "TRASMISION": "Transmisión",
    "RED_VALOR": "Reducción por valor",
    "PIGNORACION": "Pignoración",
    "EMBARGO": "Embargo",
    "USUFRUCTO": "Usufructo",
    "REDENOMINACION": "Redenominación",
}