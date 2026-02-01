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
    URLSConsultaSatelite,
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
    SegmentarTelhadoRequest,
    ProcessarLoteTelhadosRequest,
    ConsultarTelhadosRequest,
    CoordenadaGeografica,
    BoundingBoxPixeis,
    CentroidePixeis,
    TelhadoDetectadoResponse,
    TelhadoSegmentadoResponse,
    ResultadoSegmentacaoResponse,
    ResultadoProcessamentoYOLOResponse,
    ResultadoLoteResponse,
    ListaTelhadosResponse,
    EstatisticasSegmentacaoResponse,
    ProcessarComYOLORequest,
    RegistrarModeloYOLORequest,
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
    "URLSConsultaSatelite",
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
    "TelhadoSegmentadoResponse",
    "ResultadoSegmentacaoResponse",
    "ResultadoProcessamentoYOLOResponse",
    "ListaTelhadosResponse",
    "EstatisticasSegmentacaoResponse",
    "SegmentarTelhadoRequest",
    "ProcessarLoteTelhadosRequest",
    "ConsultarTelhadosRequest",
    "ProcessarComYOLORequest",
    "RegistrarModeloYOLORequest",
    "ResultadoLoteResponse",
]
