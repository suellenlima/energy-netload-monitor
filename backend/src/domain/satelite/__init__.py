"""
Satelite domain layer exports.
"""
from .errors import (
    SateliteError,
    TransformadorNotFoundError,
    CoordenadasInvalidasError,
    AreaCoberturaNaoCalculadaError,
    FonteNaoDisponibleError,
    QuotaExcedidaError,
    HistoricoVazioError,
)
from .value_objects import (
    Coordenadas,
    ResolucaoSatelite,
    AreaCobertura,
    RequisicaoHistorico,
    QuotaMensal,
)
from .aggregate import TransformadorSatelite, RequisicaoSatelite
from .repository import SateliteRepository

__all__ = [
    # Errors
    "SateliteError",
    "TransformadorNotFoundError",
    "CoordenadasInvalidasError",
    "AreaCoberturaNaoCalculadaError",
    "FonteNaoDisponibleError",
    "QuotaExcedidaError",
    "HistoricoVazioError",
    # Value Objects
    "Coordenadas",
    "ResolucaoSatelite",
    "AreaCobertura",
    "RequisicaoHistorico",
    "QuotaMensal",
    # Aggregates
    "TransformadorSatelite",
    "RequisicaoSatelite",
    # Repository
    "SateliteRepository",
]
