# tests/test_normalization.py
from app.core.services.normalization_service import build_search_name, build_name_ascii
from app.core.validators import normalize_nif_cif  # asumiendo que ya existe

def test_build_search_name_basic():
    assert build_search_name("  José  Pérez  ") == "jose perez"
    assert build_search_name("ÁLICE S.L.") == "alice s.l."

def test_build_name_ascii_basic():
    assert build_name_ascii("  José  Pérez  ") == "jose perez"
    assert build_name_ascii("ÁLICE, S.L.") == "alice sl"

def test_normalize_nif_basic():
    # Casos ilustrativos; adapta a tu lógica real de validators.py
    assert normalize_nif_cif("  b12345678 ") == "B12345678"
    assert normalize_nif_cif("a-11111111") == "A11111111"