"""Camada de acesso a dados (Repository Pattern)."""

from .analise_repository import AnaliseRepository
from .base import BaseRepository
from .subestacao_repository import SubestacaoRepository

__all__ = [
    "BaseRepository",
    "SubestacaoRepository",
    "AnaliseRepository",
]
