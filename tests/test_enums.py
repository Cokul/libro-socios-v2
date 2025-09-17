# tests/test_enums.py
from app.core.enums import normalize_event_type, EVENT_TYPE_ALIASES
from app.core.services.governance_service import _normalize_role

def test_normalize_event_type_aliases():
    # sanity: alias deben resolver al canónico
    assert normalize_event_type("trasmision") == "TRANSMISION"
    assert normalize_event_type("REDENOM") == "REDENOMINACION"
    assert normalize_event_type("ALZAMIENTO_DE_EMBARGO") == "ALZAMIENTO"
    # canónico ya correcto
    assert normalize_event_type("BAJA") == "BAJA"
    # None / vacío
    assert normalize_event_type(None) is None

def test_governance_role_aliases_titlecase_and_alias():
    # alias en snake/lower -> canónico
    assert _normalize_role("administrador_unico") == "Administrador Único"
    assert _normalize_role("consejero_delegado") == "Consejero Delegado"
    # ya canónico se mantiene
    assert _normalize_role("Presidente") == "Presidente"
    # si no hay mapping, devolver lo recibido (no perder dato)
    assert _normalize_role("Vocal") == "Vocal"