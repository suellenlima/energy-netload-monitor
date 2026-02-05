"""
Schemas Pydantic simples para o endpoint telhado.py refatorado

Author: Energy Netload Monitor
Date: 2026-02-04
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


# ============================================================================
# MODELOS DE SAÍDA - TELHADOS SIMPLES
# ============================================================================

class TelhadoSimples(BaseModel):
    """Telhado detectado - resposta simplificada"""
    
    id_telhado: int = Field(..., description="ID do telhado")
    transformador_id: int
    subestacao_id: int
    latitude: float
    longitude: float
    area_m2: float
    confianca: float = Field(..., ge=0, le=1)
    timestamp_deteccao: datetime
    transformador_codigo: Optional[str] = None
    subestacao_codigo: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "id_telhado": 1,
                "transformador_id": 100,
                "subestacao_id": 5,
                "latitude": -25.5,
                "longitude": -49.3,
                "area_m2": 125.5,
                "confianca": 0.85,
                "timestamp_deteccao": "2026-02-04T10:25:00",
                "transformador_codigo": "TRAFO_001",
                "subestacao_codigo": "SUB_001"
            }
        }


class ListaTelhadosSimples(BaseModel):
    """Lista paginada de telhados"""
    
    total_resultados: int
    pagina: int
    limite: int
    total_paginas: int
    telhados: List[TelhadoSimples]
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_resultados": 150,
                "pagina": 1,
                "limite": 10,
                "total_paginas": 15,
                "telhados": []
            }
        }


class EstatisticasSimples(BaseModel):
    """Estatísticas gerais de telhados"""
    
    total_subestacoes_processadas: int
    total_telhados_detectados: int
    media_confianca_deteccao: float = Field(..., ge=0, le=1)
    media_area_telhado_m2: float
    confianca_minima: float
    confianca_maxima: float
    area_minima_m2: float
    area_maxima_m2: float
    primeira_deteccao: Optional[datetime] = None
    ultima_deteccao: Optional[datetime] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_subestacoes_processadas": 5,
                "total_telhados_detectados": 150,
                "media_confianca_deteccao": 0.82,
                "media_area_telhado_m2": 125.5,
                "confianca_minima": 0.70,
                "confianca_maxima": 0.99
            }
        }


class TelhadosTransformadorResponse(BaseModel):
    """Telhados de um transformador específico"""
    
    transformador_id: int
    total: int
    area_total_m2: float
    confianca_media: float = Field(..., ge=0, le=1)
    telhados: List[TelhadoSimples]
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 100,
                "total": 5,
                "area_total_m2": 625.5,
                "confianca_media": 0.83,
                "telhados": []
            }
        }


class EstatisticasSubestacao(BaseModel):
    """Estatísticas de telhados por subestação"""
    
    subestacao_id: int
    transformadores: int
    total_telhados: int
    area_total_m2: float
    confianca_media: float = Field(..., ge=0, le=1)
    
    class Config:
        json_schema_extra = {
            "example": {
                "subestacao_id": 5,
                "transformadores": 25,
                "total_telhados": 150,
                "area_total_m2": 18750.5,
                "confianca_media": 0.82
            }
        }


class DetalhesSubestacao(BaseModel):
    """Detalhes completos de uma subestação"""
    
    subestacao_id: int
    timestamp_processamento: datetime
    telhados_detectados: int
    area_total_m2: float
    confianca_media: float
    transformadores_processados: int
    telhados: List[TelhadoSimples]
    
    class Config:
        json_schema_extra = {
            "example": {
                "subestacao_id": 5,
                "timestamp_processamento": "2026-02-04T10:25:00",
                "telhados_detectados": 150,
                "area_total_m2": 18750.5,
                "confianca_media": 0.82,
                "transformadores_processados": 25,
                "telhados": []
            }
        }
