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
