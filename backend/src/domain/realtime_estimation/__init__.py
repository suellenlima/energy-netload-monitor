"""Domain layer exports for RealTimeEstimation Module."""

from .aggregate import MonitorPrevisao, SistemaRealTime
from .errors import (
    CargaONSNaoObtidaError,
    ConfiabilidadeEstimativaError,
    DadosIrradianciaInvalidosError,
    EstadoNaoDisponibleError,
    GeracaoMMGDNaoCalculadaError,
    RealTimeEstimationError,
)
from .repository import RealTimeEstimationRepository
from .value_objects import CargaONS, EstadoSistemaReal, GeracaoMMGD, Irradiancia, Previsao

__all__ = [
    # Errors
    "RealTimeEstimationError",
    "EstadoNaoDisponibleError",
    "DadosIrradianciaInvalidosError",
    "CargaONSNaoObtidaError",
    "GeracaoMMGDNaoCalculadaError",
    "ConfiabilidadeEstimativaError",
    # Value Objects
    "Irradiancia",
    "CargaONS",
    "GeracaoMMGD",
    "EstadoSistemaReal",
    "Previsao",
    # Aggregates
    "SistemaRealTime",
    "MonitorPrevisao",
    # Repository
    "RealTimeEstimationRepository",
]
