"""Domain layer exports for LoadCalculation Module."""

from .aggregate import CalculadoraCarga, GeradorMMGD
from .errors import (
    CalibracaoNaoDisponibleError,
    ClasseConsumoInvalidaError,
    DadosConsumoInvalidosError,
    LoadCalculationError,
    PerfilNaoEncontradoError,
    PrevisaoNaoCalculadaError,
)
from .repository import LoadCalculationRepository
from .value_objects import CalibraçaoParametros, CargaCalculada, ConsumoGranular, MMGD, PerfilCargaHorario

__all__ = [
    # Errors
    "LoadCalculationError",
    "PerfilNaoEncontradoError",
    "DadosConsumoInvalidosError",
    "ClasseConsumoInvalidaError",
    "CalibracaoNaoDisponibleError",
    "PrevisaoNaoCalculadaError",
    # Value Objects
    "PerfilCargaHorario",
    "ConsumoGranular",
    "MMGD",
    "CargaCalculada",
    "CalibraçaoParametros",
    # Aggregates
    "CalculadoraCarga",
    "GeradorMMGD",
    # Repository
    "LoadCalculationRepository",
]
