"""
Schemas Pydantic para o pipeline de segmentação de telhados

Author: Energy Netload Monitor
Date: 2025
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime

# ============================================================================
# MODELOS DE SAÍDA (Responses)
# ============================================================================

class CoordenadaGeografica(BaseModel):
    """Coordenada geográfica (lat/lon)"""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)


class BoundingBoxPixeis(BaseModel):
    """Bounding box em pixels"""
    x: int = Field(..., description="Coordenada X em pixels")
    y: int = Field(..., description="Coordenada Y em pixels")
    largura: int = Field(..., description="Largura em pixels", gt=0)
    altura: int = Field(..., description="Altura em pixels", gt=0)


class CentroidePixeis(BaseModel):
    """Centróide em pixels"""
    x: float
    y: float


class TelhadoDetectadoResponse(BaseModel):
    """Informações de um telhado detectado"""
    
    id_telhado: str
    id_subestacao: str
    id_imagem_satelite: str
    
    # Posição e geometria
    bbox: BoundingBoxPixeis
    centroide: CentroidePixeis
    coordenada_geografica: Optional[CoordenadaGeografica] = None
    
    # Propriedades
    area_pixeis: int = Field(..., description="Número de pixels", gt=0)
    area_m2: float = Field(..., description="Estimativa de área em m²", ge=0)
    confianca: float = Field(..., ge=0, le=1, description="Confiança da detecção")
    tipo_edificio: str = Field(..., description="residencial, comercial, industrial, etc")
    
    # Qualidade
    percentual_cobertura: Optional[float] = Field(None, ge=0, le=100)
    indice_qualidade: Optional[float] = Field(None, ge=0, le=1)
    
    # Metadados
    timestamp_deteccao: datetime
    modelo_deteccao: str = Field("yolov8n-seg")
    propriedades_adicionais: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": "telhado_0_0",
                "id_subestacao": "sub_001",
                "id_imagem_satelite": "sentinel2_20250129",
                "bbox": {"x": 100, "y": 150, "largura": 50, "altura": 40},
                "centroide": {"x": 125, "y": 170},
                "area_pixeis": 2000,
                "area_m2": 18.0,
                "confianca": 0.92,
                "tipo_edificio": "residencial",
                "percentual_cobertura": 95.5,
                "indice_qualidade": 0.87
            }
        }


class TelhadoSegmentadoResponse(BaseModel):
    """Telhado após segmentação e extração"""
    
    id_telhado: str
    bbox_original: BoundingBoxPixeis
    tamanho_roi: tuple = Field(..., description="(altura, largura) em pixels")
    resolucao_m_por_pixel: float
    
    # Qualidade
    percentual_cobertura: float = Field(..., ge=0, le=100)
    indice_qualidade: float = Field(..., ge=0, le=1)
    
    # Armazenamento
    caminho_arquivo_local: Optional[str] = Field(None, description="Caminho da ROI em disco")
    url_storage: Optional[str] = Field(None, description="URL no storage cloud")
    
    timestamp_segmentacao: datetime
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": "telhado_0_0",
                "bbox_original": {"x": 100, "y": 150, "largura": 50, "altura": 40},
                "tamanho_roi": [55, 65],
                "resolucao_m_por_pixel": 3.0,
                "percentual_cobertura": 95.5,
                "indice_qualidade": 0.87
            }
        }


class ResultadoSegmentacaoResponse(BaseModel):
    """Resultado completo do processamento de telhados"""
    
    id_subestacao: str
    id_imagem_satelite: str
    timestamp_processamento: datetime
    
    # Estatísticas
    telhados_detectados: int
    telhados_segmentados: int
    telhados_processados: int = Field(0, description="Processados com YOLO (se habilitado)")
    
    # Tempo
    tempo_processamento_segundos: float = Field(..., ge=0)
    
    # Dados
    telhados: List[TelhadoDetectadoResponse] = Field(default_factory=list)
    telhados_segmentados: List[TelhadoSegmentadoResponse] = Field(default_factory=list)
    
    # Alertas
    erros: List[str] = Field(default_factory=list)
    avisos: List[str] = Field(default_factory=list)
    
    # Resumo
    sucesso: bool = Field(True, description="Se o processamento foi bem-sucedido")
    mensagem: str = Field("Processamento concluído com sucesso")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_subestacao": "sub_001",
                "id_imagem_satelite": "sentinel2_20250129",
                "timestamp_processamento": "2025-01-29T10:30:00",
                "telhados_detectados": 42,
                "telhados_segmentados": 40,
                "tempo_processamento_segundos": 15.3,
                "sucesso": True,
                "mensagem": "Processamento concluído com sucesso"
            }
        }


class ListaTelhadosResponse(BaseModel):
    """Lista paginada de telhados"""
    
    total_resultados: int
    pagina: int
    resultados_por_pagina: int
    total_paginas: int
    
    telhados: List[TelhadoDetectadoResponse]
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_resultados": 1250,
                "pagina": 1,
                "resultados_por_pagina": 100,
                "total_paginas": 13,
                "telhados": []
            }
        }


class EstatisticasSegmentacaoResponse(BaseModel):
    """Estatísticas agregadas de segmentação"""
    
    periodo: str = Field(..., description="Período das estatísticas (ex: '2025-01')")
    
    # Contagem
    total_subestacoes_processadas: int
    total_telhados_detectados: int
    total_telhados_segmentados: int
    total_imagens_processadas: int
    
    # Médias
    media_telhados_por_subestacao: float
    media_confianca_deteccao: float
    media_indice_qualidade: float
    media_area_telhado_m2: float
    
    # Distribuição por tipo
    distribuicao_tipo_edificio: Dict[str, int] = Field(default_factory=dict)
    
    # Performance
    tempo_medio_processamento_segundos: float
    tempo_total_processamento_segundos: float
    
    # Taxa de sucesso
    taxa_sucesso_percentual: float = Field(..., ge=0, le=100)
    
    class Config:
        json_schema_extra = {
            "example": {
                "periodo": "2025-01",
                "total_subestacoes_processadas": 50,
                "total_telhados_detectados": 2150,
                "total_telhados_segmentados": 2100,
                "media_telhados_por_subestacao": 42.0,
                "media_confianca_deteccao": 0.88,
                "taxa_sucesso_percentual": 97.5
            }
        }
