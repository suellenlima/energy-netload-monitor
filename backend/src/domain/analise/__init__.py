"""Domain layer exports for Analise Module."""

from .aggregate import AnaliseCarga, MonitorAnomalias, PerfisCarga
from .errors import (
    AnaliseError,
    AnomaliaDeteccaoError,
    AlertaFraudeNaoEncontradoError,
    ConsumoInvalidoError,
    DadosInsuficientesError,
    DistribuidoraNotFoundError,
    EstadoAtualNaoDisponivelError,
    PerfilCargaNotFoundError,
)
from .repository import AnaliseRepository
from .value_objects import (
    Anomalia,
    AlertaFraude,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)

__all__ = [
    # Errors
    "AnaliseError",
    "AnomaliaDeteccaoError",
    "AlertaFraudeNaoEncontradoError",
    "ConsumoInvalidoError",
    "DadosInsuficientesError",
    "DistribuidoraNotFoundError",
    "EstadoAtualNaoDisponivelError",
    "PerfilCargaNotFoundError",
    # Value Objects
    "Anomalia",
    "AlertaFraude",
    "CargaOculta",
    "ClasseConsumo",
    "EstabelecimentoContagem",
    "EstadoAtual",
    "PerfilCarga",
    "ResumoGranular",
    # Aggregates
    "AnaliseCarga",
    "MonitorAnomalias",
    "PerfisCarga",
    # Repository
    "AnaliseRepository",
]
