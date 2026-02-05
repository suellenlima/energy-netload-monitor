"""Application Layer - Subestacao Services"""

from src.application.subestacao.use_cases import (
    ObtenerSubestacaoUseCase,
    ListarSubestacioesUseCase,
    ListarPorDistribuidoraUseCase,
    ListarPorTensaoUseCase,
    ObtenerEstatisticasUseCase,
    AtivarSubestacaoUseCase,
    DesativarSubestacaoUseCase,
    ObtenerTipoTensaoUseCase,
    ObtenerONSSubestacioesUseCase,
    ObtenerGeoJSONSubestacioesUseCase,
    ObtenerResumoSubestacioesUseCase,
    ObtenerDetalhesSubestacaoUseCase,
    AssociarUCsUseCase,
    ObtenerMixConsumidoresUseCase,
    ObtenerCargaSinteticaUseCase,
)
from src.application.subestacao.clustering_use_cases import (
    DetectarSubestacioesClusteringUseCase,
    AtualizarSubestacioesDetectadasUseCase,
    ExecutarClusteringBackgroundUseCase,
)
from src.application.subestacao.area_use_cases import (
    ObtenerAreaSubestacaoUseCase,
    ObtenerTransformadoresUseCase,
    ObtenerEstatisticasAreasUseCase,
)
from src.domain.subestacao import ISubestacaoRepository

__all__ = [
    # Original 15 use cases
    "ObtenerSubestacaoUseCase",
    "ListarSubestacioesUseCase",
    "ListarPorDistribuidoraUseCase",
    "ListarPorTensaoUseCase",
    "ObtenerEstatisticasUseCase",
    "AtivarSubestacaoUseCase",
    "DesativarSubestacaoUseCase",
    "ObtenerTipoTensaoUseCase",
    "ObtenerONSSubestacioesUseCase",
    "ObtenerGeoJSONSubestacioesUseCase",
    "ObtenerResumoSubestacioesUseCase",
    "ObtenerDetalhesSubestacaoUseCase",
    "AssociarUCsUseCase",
    "ObtenerMixConsumidoresUseCase",
    "ObtenerCargaSinteticaUseCase",
    # TIER 2: Clustering use cases (3 new - including background)
    "DetectarSubestacioesClusteringUseCase",
    "AtualizarSubestacioesDetectadasUseCase",
    "ExecutarClusteringBackgroundUseCase",
    # TIER 2: Area use cases (3 new)
    "ObtenerAreaSubestacaoUseCase",
    "ObtenerTransformadoresUseCase",
    "ObtenerEstatisticasAreasUseCase",
]

