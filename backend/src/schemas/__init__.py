"""Schemas Pydantic para validação de dados da API."""

from .analise import (
    AlertaFraude,
    CargaOcultaItem,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    ResumoGranular,
)
from .common import GeoJSONFeature, GeoJSONResponse, QueryParams
from .satelite import (
    BoundingBoxModel,
    CoordenadasGeograficas,
    ConsultaSateliteRequest,
    DadosSatelliteSubestacao,
    ImagemSateliteMetadata,
    ListaImagensSatelite,
    PeriodoTemporal,
    RegistrarImagemRequest,
    RegistrarImagemResponse,
    URLSTACQuery,
    URLWMSQuery,
)
from .subestacao import (
    AtualizarDetectadasRequest,
    AtualizarDetectadasResponse,
    SubestacaoDetectadaResponse,
    SubestacaoONSResponse,
    SubestacaoResumo,
    TaskAsyncResponse,
)
from .telhado import (
    TelhadoSimples,
    ListaTelhadosSimples,
    EstatisticasSimples,
    TelhadosTransformadorResponse,
    EstatisticasSubestacao,
    DetalhesSubestacao,
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
    "TaskAsyncResponse",
    # Satelite
    "CoordenadasGeograficas",
    "BoundingBoxModel",
    "PeriodoTemporal",
    "URLSTACQuery",
    "URLWMSQuery",
    "DadosSatelliteSubestacao",
    "ImagemSateliteMetadata",
    "ListaImagensSatelite",
    "RegistrarImagemRequest",
    "RegistrarImagemResponse",
    "ConsultaSateliteRequest",
    # Analise
    "CargaOcultaItem",
    "ClasseConsumoItem",
    "AlertaFraude",
    "EstabelecimentoContagem",
    "ResumoGranular",
    # Telhado
    "CoordenadaGeografica",
    "BoundingBoxPixeis",
    "CentroidePixeis",
    "TelhadoDetectadoResponse",
    "ResultadoSegmentacaoResponse",
    "ListaTelhadosResponse",
    "EstatisticasSegmentacaoResponse",
]
