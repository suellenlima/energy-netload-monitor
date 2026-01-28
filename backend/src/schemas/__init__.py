"""Schemas Pydantic para validação de dados da API."""

from .analise import (
    AlertaFraude,
    CargaOcultaItem,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    ResumoGranular,
)
from .common import GeoJSONFeature, GeoJSONResponse, QueryParams
from .subestacao import (
    AtualizarDetectadasRequest,
    AtualizarDetectadasResponse,
    SubestacaoDetectadaResponse,
    SubestacaoONSResponse,
    SubestacaoResumo,
)

__all__ = [
    # Common
    "QueryParams",
    "GeoJSONFeature",
    "GeoJSONResponse",
    # Subestacao
    "SubestacaoONSResponse",
    "SubestacaoDetectadaResponse",
    "SubestacaoResumo",
    "AtualizarDetectadasRequest",
    "AtualizarDetectadasResponse",
    # Analise
    "CargaOcultaItem",
    "ClasseConsumoItem",
    "AlertaFraude",
    "EstabelecimentoContagem",
    "ResumoGranular",
]
