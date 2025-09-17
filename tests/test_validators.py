# tests/test_validators.py
import pytest
from app.core.validators import normalize_nif_cif

@pytest.mark.parametrize("raw,expected", [
    ("  b12345678 ", "B12345678"),
    ("a-11111111",  "A11111111"),
    ("  x1234567l", "X1234567L"),   # NIE ejemplo
    (" y-1234567-z ", "Y1234567Z"),
])
def test_normalize_nif_variants(raw, expected):
    assert normalize_nif_cif(raw) == expected