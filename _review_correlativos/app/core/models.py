#app/core/models.py

from dataclasses import dataclass
from typing import Optional

@dataclass
class Partner:
    id: int
    company_id: int
    nombre: str
    nif: str
    domicilio: Optional[str] = None
    nacionalidad: Optional[str] = None
    fecha_nacimiento_constitucion: Optional[str] = None

@dataclass
class BoardMember:
    id: int
    company_id: int
    nombre: str
    cargo: str
    nif: str
    direccion: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None