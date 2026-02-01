"""
Schemas para detecção de painéis solares
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime


# ============================================================================
# REQUESTS
# ============================================================================

class DetectarPainelSolarRequest(BaseModel):
    """Requisição para detectar painéis solares (um telhado OU transformador com múltiplos telhados)"""
    
    telhado_id: Optional[int] = Field(None, description="ID do telhado (use telhado_id OU transformador_id)")
    transformador_id: Optional[int] = Field(None, description="ID do transformador para processar TODOS os telhados")
    url_imagem: Optional[str] = Field(None, description="URL da imagem Google Maps (necessária se telhado_id informado)")
    bbox_json: Optional[str] = Field(None, description="JSON com bbox do telhado (necessária se telhado_id informado)")
    confianca_minima: float = Field(0.5, description="Confiança mínima para detecção YOLO", ge=0.1, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 123,
                "confianca_minima": 0.5
            }
        }


class DetectarPainelSolarEmRoiRequest(BaseModel):
    """Requisição alternativa: passa a ROI cortada diretamente"""
    
    roi_base64: str = Field(..., description="Imagem ROI em base64")
    confianca_minima: float = Field(0.5, description="Confiança mínima para detecção", ge=0.1, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "roi_base64": "iVBORw0KGgoAAAANSUhEUgAAAAUA...",
                "confianca_minima": 0.5
            }
        }


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


class DeteccaoPainelSolarResponse(BaseModel):
    """Resultado completo da detecção de painéis solares"""
    
    sucesso: bool
    telhado_id: Optional[int] = None
    transformador_id: Optional[int] = None
    num_telhados_processados: int = 0
    paineis: List[PainelSolarResponse] = []
    potencia: Optional[EstimativaPotenciaResponse] = None
    telhados_com_paineis: Optional[List[TelhadorComPaineis]] = None  # Todos os telhados com painéis detalhados
    potencia_por_telhado: Optional[List[Dict]] = None  # Resumo rápido por telhado (deprecated, usar telhados_com_paineis)
    erros: List[str] = []
    tempo_processamento_s: float
    timestamp: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_schema_extra = {
            "example": {
                "sucesso": True,
                "telhado_id": 1,
                "paineis": [
                    {
                        "id_painel": "painel_1",
                        "bbox": {"x": 10, "y": 20, "w": 30, "h": 40},
                        "centroide": {"x": 25, "y": 40},
                        "area_pixeis": 1200,
                        "area_m2": 10.8,
                        "confianca": 0.92,
                        "tipo_painel": "desconhecido",
                        "timestamp_deteccao": "2026-02-01T13:45:30"
                    }
                ],
                "potencia": {
                    "total_area_m2": 10.8,
                    "num_paineis": 1,
                    "potencia_instalada_kw": 1.62,
                    "producao_diaria_kwh": 7.29,
                    "producao_anual_kwh": 2660.85,
                    "fator_capacidade": 0.15,
                    "economia_anual_brl": 2128.68
                },
                "erros": [],
                "tempo_processamento_s": 2.34
            }
        }
