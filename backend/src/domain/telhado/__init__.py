"""Telhado domain module exports."""

from src.domain.telhado.entity import Telhado
from src.domain.telhado.errors import InvalidTelhadoError, TelhadoError, TelhadoNotFoundError
from src.domain.telhado.repository_interface import ITelhadoRepository
from src.domain.telhado.value_objects import (
    AreaTelhado,
    CodigoTelhado,
    InclinacaoTelhado,
    Orientacao,
)

__all__ = [
    "Telhado",
    "TelhadoError",
    "TelhadoNotFoundError",
    "InvalidTelhadoError",
    "ITelhadoRepository",
    "CodigoTelhado",
    "AreaTelhado",
    "InclinacaoTelhado",
    "Orientacao",
]
