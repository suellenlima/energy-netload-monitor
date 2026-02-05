"""Common domain module initialization."""

from .errors import DomainError
from .value_objects import Localizacao, Potencia, Temperatura, ValueObject

__all__ = [
    "DomainError",
    "ValueObject",
    "Localizacao",
    "Potencia",
    "Temperatura",
]
