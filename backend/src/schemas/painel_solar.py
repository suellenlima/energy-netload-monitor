"""
Schemas para detecção de painéis solares
"""

from pydantic import BaseModel, Field
from typing import List, Dict
from datetime import datetime

# ============================================================================
# RESPONSES
# ============================================================================

class PainelSolarResponse(BaseModel):
    """Informações de um painel solar detectado"""
    
    id_painel: str
    bbox: Dict
    centroide: Dict
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_painel: str
    timestamp_deteccao: datetime


class EstimativaPotenciaResponse(BaseModel):
    """Estimativa de potência e produção"""
    
    total_area_m2: float = Field(..., description="Área total dos painéis")
    num_paineis: int = Field(..., description="Número de painéis detectados")
    potencia_instalada_kw: float = Field(..., description="Potência instalada em kW")
    producao_diaria_kwh: float = Field(..., description="Produção diária média em kWh")
    producao_anual_kwh: float = Field(..., description="Produção anual estimada em kWh")
    fator_capacidade: float = Field(..., description="Fator de capacidade (0-1)")
    economia_anual_brl: float = Field(..., description="Economia anual estimada em R$")


class TelhadorComPaineis(BaseModel):
    """Informações de um telhado com seus painéis detectados"""
    
    telhado_id: int
    num_paineis: int
    area_total_m2: float
    potencia_instalada_kw: float
    producao_anual_kwh: float
    economia_anual_brl: float
    paineis: List[PainelSolarResponse] = []


# ============================================================================
# REQUESTS
# ============================================================================

class DeteccaoPainelRequest(BaseModel):
    """Request para detecção de painéis solares em telhados de um transformador"""
    
    transformador_id: int = Field(..., description="ID do transformador")
    confianca_minima: float = Field(default=0.5, ge=0, le=1, description="Confiança mínima para detecção (0-1)")
    potencia_por_m2: float = Field(default=200, ge=100, le=300, description="Potência por m² (W/m²)")
    processar_todos_telhados: bool = Field(default=True, description="Se False, processa apenas telhados sem painéis")
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 95422,
                "confianca_minima": 0.5,
                "potencia_por_m2": 200,
                "processar_todos_telhados": True
            }
        }


class DeteccaoPainelResponse(BaseModel):
    """Response da detecção de painéis solares"""
    
    sucesso: bool
    transformador_id: int
    telhados_processados: int
    paineis_detectados: int
    tempo_processamento_s: float
    paineis_salvos: List[int]
    area_total_paineis_m2: float = 0.0
    potencia_total_kw: float = 0.0
    erros: List[str] = Field(default_factory=list)
    avisos: List[str] = Field(default_factory=list)
    detalhes: Dict = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "sucesso": True,
                "transformador_id": 95422,
                "telhados_processados": 17,
                "paineis_detectados": 45,
                "tempo_processamento_s": 23.5,
                "paineis_salvos": [1, 2, 3],
                "area_total_paineis_m2": 125.5,
                "potencia_total_kw": 25.1,
                "erros": [],
                "avisos": [],
                "detalhes": {}
            }
        }
