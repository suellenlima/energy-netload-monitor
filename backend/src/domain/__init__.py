"""Domain layer module initialization."""

from . import analise, satelite, subestacao
from .comum import DomainError, Localizacao, Potencia, Temperatura, ValueObject
from .transformador import (
    AreaCobertura,
    CodigoTransformador,
    ITransformadorRepository,
    NomeTransformador,
    TensaoTipo,
    Transformador,
    TransformadorNotFoundError,
)

__all__ = [
    # Common
    "DomainError",
    "ValueObject",
    "Localizacao",
    "Potencia",
    "Temperatura",
    # Transformador
    "Transformador",
    "TransformadorNotFoundError",
    "ITransformadorRepository",
    "CodigoTransformador",
    "NomeTransformador",
    "TensaoTipo",
    "AreaCobertura",
]
