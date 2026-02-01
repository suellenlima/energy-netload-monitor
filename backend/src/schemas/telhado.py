"""
Schemas Pydantic para o pipeline de segmentação de telhados

Author: Energy Netload Monitor
Date: 2025
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime


# ============================================================================
# MODELOS DE ENTRADA (Requests)
# ============================================================================

class SegmentarTelhadoRequest(BaseModel):
    """Requisição para segmentar telhados em uma subestação"""
    
    id_subestacao: str = Field(..., description="ID da subestação")
    url_imagem_satelite: str = Field(..., description="URL da imagem Sentinel-2 ou Landsat")
    resolucao_m_por_pixel: float = Field(3.0, description="Escala da imagem em metros/pixel", ge=0.1, le=10.0)
    confianca_minima: float = Field(0.5, description="Confiança mínima para detecção", ge=0.1, le=1.0)
    salvar_rois: bool = Field(True, description="Se deve salvar ROIs em disco")
    diretorio_saida: Optional[str] = Field(None, description="Diretório para salvar ROIs")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_subestacao": "sub_001",
                "url_imagem_satelite": "https://planetary-computer.azure.com/api/...",
                "resolucao_m_por_pixel": 3.0,
                "confianca_minima": 0.5,
                "salvar_rois": True
            }
        }


class SegmentarTelhadoComImagemIdRequest(BaseModel):
    """Requisição para segmentar telhados de transformador usando imagem_id do banco (v2) ou grid automático"""
    
    transformador_id: int = Field(..., description="ID do transformador no banco")
    imagem_id: Optional[int] = Field(
        default=None, 
        description="ID da imagem em satelite_imagens (opcional - se não fornecido, usa grid automático do Google Maps)",
        example=None
    )
    confianca_minima: float = Field(0.5, description="Confiança mínima para detecção YOLO", ge=0.1, le=1.0)
    salvar_rois: bool = Field(True, description="Se deve salvar ROIs em disco")
    diretorio_saida: Optional[str] = Field(None, description="Diretório para salvar ROIs (padrão: data/processed)")
    aplicar_filtro_ndvi: bool = Field(
        False, 
        description="Se deve aplicar filtro NDVI para remover não-urbano (apenas para CBERS-4A com imagem_id)"
    )
    limiar_ndvi: float = Field(0.3, description="Limiar NDVI para classificação urbana", ge=0.0, le=1.0)
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "transformador_id": 47,
                    "confianca_minima": 0.5,
                    "salvar_rois": True,
                    "diretorio_saida": "data/rois/grid",
                    "aplicar_filtro_ndvi": False
                },
                {
                    "transformador_id": 47,
                    "imagem_id": 13,
                    "confianca_minima": 0.5,
                    "salvar_rois": True,
                    "diretorio_saida": "data/rois/cbers",
                    "aplicar_filtro_ndvi": True,
                    "limiar_ndvi": 0.3
                }
            ]
        }
    }


class ProcessarLoteTelhadosRequest(BaseModel):
    """Requisição para processar múltiplas subestações/imagens"""
    
    subestacoes: List[str] = Field(..., description="Lista de IDs de subestações")
    imagens_por_subestacao: Dict[str, str] = Field(
        ..., 
        description="Mapa de subestacao_id → url_imagem"
    )
    resolucao_m_por_pixel: float = Field(3.0, ge=0.1, le=10.0)
    confianca_minima: float = Field(0.5, ge=0.1, le=1.0)
    processar_com_yolo: bool = Field(False, description="Se deve processar ROIs com modelo YOLO")
    modelo_yolo_path: Optional[str] = Field(None, description="Caminho para modelo YOLO customizado")
    
    class Config:
        json_schema_extra = {
            "example": {
                "subestacoes": ["sub_001", "sub_002"],
                "imagens_por_subestacao": {
                    "sub_001": "https://...",
                    "sub_002": "https://..."
                },
                "processar_com_yolo": False
            }
        }


class ConsultarTelhadosRequest(BaseModel):
    """Requisição para consultar telhados processados"""
    
    id_subestacao: Optional[str] = Field(None, description="Filtrar por subestação")
    data_inicio: Optional[datetime] = Field(None, description="Filtrar por data inicial")
    data_fim: Optional[datetime] = Field(None, description="Filtrar por data final")
    confianca_minima: Optional[float] = Field(None, ge=0.0, le=1.0)
    tipo_edificio: Optional[str] = Field(None, description="Filtrar por tipo (residencial, comercial, industrial)")
    limite: int = Field(100, description="Número máximo de resultados", ge=1, le=10000)
    pagina: int = Field(1, description="Número da página", ge=1)


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


class ResultadoSegmentacaoTransformadorResponse(BaseModel):
    """Resultado completo do processamento de telhados de um transformador (v2)"""
    
    transformador_id: int = Field(..., description="ID do transformador processado")
    imagem_id: int = Field(..., description="ID da imagem CBERS-4A utilizada")
    id_imagem_satelite: str = Field(..., description="Identificador da imagem no pipeline")
    timestamp_processamento: datetime
    
    # Estatísticas
    telhados_detectados: int = Field(..., ge=0, description="Número de telhados detectados com YOLO")
    total_telhados_segmentados: int = Field(..., ge=0, description="Número de telhados segmentados")
    telhados_processados: int = Field(0, ge=0, description="Processados com YOLO se habilitado")
    
    # Tempo
    tempo_processamento_segundos: float = Field(..., ge=0)
    
    # Dados
    telhados: List[TelhadoDetectadoResponse] = Field(default_factory=list)
    telhados_segmentados: List[TelhadoSegmentadoResponse] = Field(default_factory=list)
    
    # Informações sobre o processamento multibanda
    bandas_processadas: List[str] = Field(default_factory=list, description="Bandas CBERS-4A processadas (blue, green, red, nir, swir)")
    filtro_ndvi_aplicado: bool = Field(False, description="Se filtro NDVI foi aplicado")
    limiar_ndvi_utilizado: Optional[float] = Field(None, description="Limiar NDVI usado para máscara urbana")
    
    # Alertas
    erros: List[str] = Field(default_factory=list)
    avisos: List[str] = Field(default_factory=list)
    
    # Resumo
    sucesso: bool = Field(True, description="Se o processamento foi bem-sucedido")
    mensagem: str = Field("Processamento concluído com sucesso")
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 400,
                "imagem_id": 13,
                "id_imagem_satelite": "imagem_13_multibanda",
                "timestamp_processamento": "2026-01-31T20:59:22.291314",
                "telhados_detectados": 12,
                "telhados_segmentados": 10,
                "tempo_processamento_segundos": 8.5,
                "bandas_processadas": ["blue", "green", "red", "nir"],
                "filtro_ndvi_aplicado": True,
                "limiar_ndvi_utilizado": 0.3,
                "sucesso": True,
                "mensagem": "Processamento concluído com sucesso"
            }
        }


class ResultadoProcessamentoYOLOResponse(BaseModel):
    """Resultado do processamento com modelo YOLO (painéis solares, cobertura, etc)"""
    
    id_telhado: str
    timestamp_processamento: datetime
    
    # Modelo utilizado
    modelo_yolo: str = Field(..., description="Nome/caminho do modelo")
    tempo_inferencia_ms: float = Field(..., ge=0)
    
    # Detecções
    numero_paineis_detectados: int = Field(0, ge=0)
    numero_objetos_detectados: int = Field(0, ge=0)
    
    # Confiança e qualidade
    confianca_media: float = Field(..., ge=0, le=1)
    area_coberta_percentual: float = Field(..., ge=0, le=100)
    
    # Detalhes
    deteccoes: List[Dict[str, Any]] = Field(default_factory=list, description="Detalhes das detecções")
    propriedades_calculadas: Dict[str, Any] = Field(default_factory=dict)
    
    # Status
    sucesso: bool
    erros: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": "telhado_0_0",
                "modelo_yolo": "yolov8n-solar-panels-v1.pt",
                "numero_paineis_detectados": 24,
                "confianca_media": 0.87,
                "area_coberta_percentual": 45.2,
                "sucesso": True
            }
        }


class ResultadoLoteResponse(BaseModel):
    """Resultado do processamento de lote"""
    
    timestamp_inicio: datetime
    timestamp_fim: datetime
    tempo_total_segundos: float
    
    # Resumo
    subestacoes_processadas: int
    subestacoes_com_sucesso: int
    subestacoes_com_erro: int
    
    telhados_detectados_total: int
    telhados_segmentados_total: int
    
    # Resultados por subestação
    resultados: Dict[str, ResultadoSegmentacaoResponse] = Field(default_factory=dict)
    
    # Status geral
    taxa_sucesso_percentual: float = Field(..., ge=0, le=100)
    erros_globais: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "timestamp_inicio": "2025-01-29T10:00:00",
                "timestamp_fim": "2025-01-29T11:00:00",
                "tempo_total_segundos": 3600,
                "subestacoes_processadas": 10,
                "subestacoes_com_sucesso": 9,
                "subestacoes_com_erro": 1,
                "telhados_detectados_total": 420,
                "telhados_segmentados_total": 410,
                "taxa_sucesso_percentual": 90.0
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


# ============================================================================
# MODELOS PARA INTEGRAÇÃO COM YOLO
# ============================================================================

class ProcessarComYOLORequest(BaseModel):
    """Requisição para processar ROIs com modelo YOLO"""
    
    id_telhado: str
    caminho_roi_local: Optional[str] = None
    url_roi_storage: Optional[str] = None
    modelo_yolo_id: str = Field("solar-panels-v1", description="ID do modelo registrado")
    parametros_modelo: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": "telhado_0_0",
                "caminho_roi_local": "/data/rois/sub_001_telhado_0_0.png",
                "modelo_yolo_id": "solar-panels-v1",
                "parametros_modelo": {
                    "confianca": 0.5,
                    "iou": 0.45
                }
            }
        }


class RegistrarModeloYOLORequest(BaseModel):
    """Registrar novo modelo YOLO na plataforma"""
    
    modelo_id: str = Field(..., description="Identificador único do modelo")
    nome_modelo: str = Field(..., description="Nome amigável")
    descricao: str = Field(..., description="O que o modelo detecta")
    caminho_arquivo: str = Field(..., description="Caminho para arquivo .pt")
    tipo_deteccao: str = Field(..., description="solar-panels, cobertura, structural, etc")
    versao: str = Field("1.0", description="Versão do modelo")
    metricas: Dict[str, float] = Field(default_factory=dict, description="mAP50, mAP75, F1, etc")
    
    class Config:
        json_schema_extra = {
            "example": {
                "modelo_id": "solar-panels-v1",
                "nome_modelo": "YOLOv8 Solar Panels Detector",
                "descricao": "Detecta painéis solares em telhados residenciais e comerciais",
                "caminho_arquivo": "/models/yolov8n-solar-panels.pt",
                "tipo_deteccao": "solar-panels",
                "versao": "1.0",
                "metricas": {"mAP50": 0.89, "mAP75": 0.81, "F1": 0.87}
            }
        }


# ============================================================================
# SCHEMAS PARA TRANSFORMADORES (novo)
# ============================================================================

class SegmentarTelhadoTransformadorRequest(BaseModel):
    """Requisição para detectar telhados em um transformador"""
    
    transformador_id: int = Field(..., description="ID do transformador")
    subestacao_id: int = Field(..., description="ID da subestação")
    url_imagem: str = Field(..., description="URL da imagem (Google Maps ou CBERS-4A)")
    fonte_imagem: str = Field("google_maps", description="Fonte da imagem", pattern="^(google_maps|cbers4a)$")
    confianca_minima: float = Field(0.5, description="Confiança mínima", ge=0.1, le=1.0)
    resolucao_cm: float = Field(30.0, description="Resolução em cm/pixel")
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 47,
                "subestacao_id": 1,
                "url_imagem": "https://maps.googleapis.com/maps/api/staticmap?...",
                "fonte_imagem": "google_maps",
                "confianca_minima": 0.5,
                "resolucao_cm": 30.0
            }
        }


class TelhadoTransformadorResponse(BaseModel):
    """Informações de um telhado detectado em transformador"""
    
    id_telhado: str
    id_transformador: int
    id_subestacao: int
    bbox: Dict[str, float]
    centroide: Dict[str, float]
    latitude: float
    longitude: float
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_edificio: str
    timestamp_deteccao: datetime
    fonte_imagem: str
    resolucao_cm: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": "trafo_47_telhado_0",
                "id_transformador": 47,
                "id_subestacao": 1,
                "bbox": {"x": 100, "y": 150, "w": 50, "h": 40},
                "centroide": {"x": 125, "y": 170},
                "latitude": -2.7173114,
                "longitude": -60.0408171,
                "area_pixeis": 2000,
                "area_m2": 180.0,
                "confianca": 0.87,
                "tipo_edificio": "residencial",
                "timestamp_deteccao": "2026-01-31T20:30:00",
                "fonte_imagem": "google_maps",
                "resolucao_cm": 30.0
            }
        }


class ResultadoDeteccaoTransformadorResponse(BaseModel):
    """Resultado da detecção de telhados para um transformador"""
    
    transformador_id: int
    subestacao_id: int
    sucesso: bool
    total_telhados: int
    telhados: List[TelhadoTransformadorResponse]
    area_total_m2: float
    confianca_media: float
    motivo: str
    tempo_processamento_ms: float
    fonte_imagem: str
    timestamp: datetime
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 47,
                "subestacao_id": 1,
                "sucesso": True,
                "total_telhados": 2,
                "telhados": [...],
                "area_total_m2": 350.0,
                "confianca_media": 0.85,
                "motivo": "Sucesso",
                "tempo_processamento_ms": 1250.5,
                "fonte_imagem": "google_maps",
                "timestamp": "2026-01-31T20:30:00"
            }
        }


class ProcessarLoteTelhadosTransformadorRequest(BaseModel):
    """Requisição para processar telhados de múltiplos transformadores"""
    
    subestacao_id: int = Field(..., description="ID da subestação")
    transformadores: List[int] = Field(..., description="IDs dos transformadores a processar")
    imagens_por_transformador: Dict[str, str] = Field(..., description="Mapa transformador_id → URL imagem")
    fonte_imagem: str = Field("google_maps", pattern="^(google_maps|cbers4a)$")
    confianca_minima: float = Field(0.5, ge=0.1, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "subestacao_id": 1,
                "transformadores": [47, 50, 247],
                "imagens_por_transformador": {
                    "47": "https://maps.googleapis.com/...",
                    "50": "https://maps.googleapis.com/..."
                },
                "fonte_imagem": "google_maps",
                "confianca_minima": 0.5
            }
        }


class ListaTelhadosTransformadorResponse(BaseModel):
    """Lista de telhados para um transformador"""
    
    transformador_id: int
    transformador_nome: str
    subestacao_id: int
    total_telhados: int
    area_total_m2: float
    area_media_m2: float
    confianca_media: float
    telhados: List[TelhadoTransformadorResponse]
    ultima_atualizacao: datetime


class EstatisticasTransformadorResponse(BaseModel):
    """Estatísticas de detecção para transformadores"""
    
    subestacao_id: int
    total_transformadores: int
    transformadores_processados: int
    total_telhados: int
    area_total_m2: float
    area_media_por_transformador: float
    confianca_media: float
    distribuicao_por_tipo: Dict[str, int]
    timestamp: datetime
