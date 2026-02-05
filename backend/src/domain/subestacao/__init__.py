"""Domain Layer - Subestacao Bounded Context"""

from .entity import Subestacao
from .value_objects import (
    CodigoSubestacao,
    NomeSubestacao,
    TensaoNominal,
    AreaCobertura,
)
from .errors import (
    SubestacaoError,
    SubestacaoNotFoundError,
    SubestacaoInvalidError,
)
from .repository_interface import ISubestacaoRepository

__all__ = [
    "Subestacao",
    "CodigoSubestacao",
    "NomeSubestacao",
    "TensaoNominal",
    "AreaCobertura",
    "SubestacaoError",
    "SubestacaoNotFoundError",
    "SubestacaoInvalidError",
    "ISubestacaoRepository",
]
