"""Transformador domain module initialization."""

from .entity import Transformador
from .errors import (
    AreaCoberturaNotFoundError,
    InvalidTransformadorError,
    TransformadorError,
    TransformadorNotFoundError,
)
from .repository_interface import ITransformadorRepository
from .value_objects import (
    AreaCobertura,
    CodigoTransformador,
    NomeTransformador,
    TensaoTipo,
)

__all__ = [
    "Transformador",
    "TransformadorError",
    "TransformadorNotFoundError",
    "InvalidTransformadorError",
    "AreaCoberturaNotFoundError",
    "ITransformadorRepository",
    "CodigoTransformador",
    "NomeTransformador",
    "TensaoTipo",
    "AreaCobertura",
]
