"""Schemas para dados de imagens de satélite."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CoordenadasGeograficas(BaseModel):
    """Coordenadas geográficas (latitude, longitude)."""
    
    latitude: float = Field(..., description="Latitude em graus (-90 a 90)")
    longitude: float = Field(..., description="Longitude em graus (-180 a 180)")


class BoundingBoxModel(BaseModel):
    """Bounding box (retângulo geográfico)."""
    
    min_lat: float = Field(..., description="Latitude mínima")
    max_lat: float = Field(..., description="Latitude máxima")
    min_lon: float = Field(..., description="Longitude mínima")
    max_lon: float = Field(..., description="Longitude máxima")
    
    center: Optional[CoordenadasGeograficas] = Field(
        default=None,
        description="Centro da bbox"
    )
    dimensoes: Optional[Dict[str, float]] = Field(
        default=None,
        description="Dimensões aproximadas em km"
    )


class PeriodoTemporal(BaseModel):
    """Período temporal para consultas."""
    
    data_inicio: datetime = Field(..., description="Data inicial")
    data_fim: datetime = Field(..., description="Data final")


class URLSTACQuery(BaseModel):
    """URL e payload para consulta STAC."""
    
    url: str = Field(..., description="URL base STAC")
    payload: Dict[str, Any] = Field(..., description="Payload JSON para POST")
    method: str = Field(default="POST", description="Método HTTP")


class URLWMSQuery(BaseModel):
    """URL WMS para requisição de imagem."""
    
    url: str = Field(..., description="URL WMS completa")
    camadas_disponiveis: List[str] = Field(
        default=[],
        description="Camadas disponíveis neste endpoint"
    )


class URLSConsultaSatelite(BaseModel):
    """URLs para consultar diferentes fontes de satélite."""
    
    sentinel2: Optional[URLSTACQuery] = Field(
        default=None,
        description="STAC Sentinel-2 Planetary Computer"
    )
    landsat: Optional[URLSTACQuery] = Field(
        default=None,
        description="STAC Landsat USGS"
    )
    terrabrasilis_wms: Optional[URLWMSQuery] = Field(
        default=None,
        description="WMS Terrabrasilis INPE"
    )


class DadosSatelliteSubestacao(BaseModel):
    """Resposta com dados de satélite disponíveis para uma subestação."""
    
    subestacao: Dict[str, Any] = Field(
        ...,
        description="Informações da subestação"
    )
    bbox: BoundingBoxModel = Field(
        ...,
        description="Bounding box calculada"
    )
    periodo: PeriodoTemporal = Field(
        ...,
        description="Período de disponibilidade de dados"
    )
    urls_consulta: URLSConsultaSatelite = Field(
        ...,
        description="URLs para consultar dados de cada sensor"
    )


class BandaSatelite(BaseModel):
    """Metadados de uma banda de imagem de satélite."""
    
    numero_banda: int = Field(..., ge=0, le=4, description="Número da banda (0-4)")
    nome_banda: str = Field(
        ...,
        description="Nome da banda (blue, green, red, nir, swir)"
    )
    url: str = Field(..., description="URL para download da banda")
    resolucao_m: Optional[int] = Field(
        default=None,
        description="Resolução específica da banda em metros"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "numero_banda": 2,
                "nome_banda": "red",
                "url": "https://data.inpe.br/.../BAND2.tif",
                "resolucao_m": 2
            }
        }


class ImagemSateliteMetadata(BaseModel):
    """Metadados de uma imagem de satélite."""
    
    id: int = Field(..., description="ID único da imagem")
    sensor: str = Field(..., description="Sensor/satélite (Sentinel-2, Landsat, etc.)")
    data_aquisicao: datetime = Field(..., description="Data de aquisição")
    resolucao_m: int = Field(..., description="Resolução em metros")
    cobertura_nuvem_pct: float = Field(
        ...,
        ge=0,
        le=100,
        description="Percentual de cobertura de nuvem"
    )
    url: str = Field(..., description="URL para download/acesso à imagem (compatibilidade)")
    bandas: List[BandaSatelite] = Field(
        default_factory=list,
        description="URLs de todas as bandas disponíveis"
    )
    propriedades: Dict[str, Any] = Field(
        default_factory=dict,
        description="Propriedades adicionais"
    )


class ListaImagensSatelite(BaseModel):
    """Lista de imagens de satélite para uma subestação."""
    
    subestacao_id: int = Field(..., description="ID da subestação")
    subestacao_nome: Optional[str] = Field(
        default=None,
        description="Nome da subestação"
    )
    total_imagens: int = Field(..., description="Total de imagens registradas")
    imagens: List[ImagemSateliteMetadata] = Field(
        ...,
        description="Lista de imagens"
    )


class RegistrarImagemRequest(BaseModel):
    """Request para registrar uma imagem de satélite."""
    
    sensor: str = Field(..., description="Sensor/satélite")
    data_aquisicao: datetime = Field(..., description="Data de aquisição")
    resolucao_m: int = Field(..., ge=1, description="Resolução em metros")
    cobertura_nuvem_pct: float = Field(
        ...,
        ge=0,
        le=100,
        description="Percentual de cobertura de nuvem"
    )
    url: str = Field(..., description="URL da imagem")
    propriedades: Dict[str, Any] = Field(
        default_factory=dict,
        description="Propriedades adicionais"
    )


class RegistrarImagemResponse(BaseModel):
    """Response após registrar uma imagem."""
    
    status: str = Field(..., description="Status da operação")
    mensagem: str = Field(..., description="Mensagem descritiva")
    imagem_id: Optional[str] = Field(
        default=None,
        description="ID da imagem registrada"
    )


class ConsultaSateliteRequest(BaseModel):
    """Request para consultar dados de satélite."""
    
    subestacao_id: int = Field(..., description="ID da subestação")
    data_inicio: Optional[datetime] = Field(
        default=None,
        description="Data inicial (default: últimos 30 dias)"
    )
    data_fim: Optional[datetime] = Field(
        default=None,
        description="Data final (default: hoje)"
    )
    raio_km: float = Field(
        default=5.0,
        ge=0.5,
        le=50.0,
        description="Área poligonal de cobertura em km (baseada em bounding box, não circular)"
    )
    sensores: List[str] = Field(
        default=["Sentinel-2", "Landsat"],
        description="Lista de sensores para consultar"
    )
