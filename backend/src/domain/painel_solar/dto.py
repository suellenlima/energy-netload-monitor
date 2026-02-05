"""Data Transfer Objects for solar panel domain"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class PainelSolarDTO:
    """DTO for detected solar panel data transfer"""
    
    id_painel: str
    bbox: Dict[str, float]  # {x, y, w, h}
    centroide: Dict[str, float]  # {x, y}
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_painel: str
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "id_painel": self.id_painel,
            "bbox": self.bbox,
            "centroide": self.centroide,
            "area_pixeis": self.area_pixeis,
            "area_m2": self.area_m2,
            "confianca": self.confianca,
            "tipo_painel": self.tipo_painel,
            "timestamp_deteccao": self.timestamp_deteccao.isoformat(),
        }


@dataclass
class EstimativaPotenciaDTO:
    """DTO for power estimation data transfer"""
    
    total_area_m2: float
    num_paineis: int
    potencia_instalada_kw: float
    producao_diaria_kwh: float = 0.0
    producao_anual_kwh: float = 0.0
    fator_capacidade: float = 0.15
    economia_anual_brl: float = 0.0
    tarifa_media_brl_kwh: float = 0.80
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "total_area_m2": self.total_area_m2,
            "num_paineis": self.num_paineis,
            "potencia_instalada_kw": self.potencia_instalada_kw,
            "producao_diaria_kwh": self.producao_diaria_kwh,
            "producao_anual_kwh": self.producao_anual_kwh,
            "fator_capacidade": self.fator_capacidade,
            "economia_anual_brl": self.economia_anual_brl,
        }


@dataclass
class PropertyClassificationDTO:
    """DTO for property classification result"""
    
    tipo: str  # residencial, comercial, industrial, unknown
    confianca: float
    descricao: str
    faixa_potencia_kw: tuple
    num_paineis: int
    potencia_estimada_kw: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "tipo": self.tipo,
            "confianca": self.confianca,
            "descricao": self.descricao,
            "faixa_potencia_kw": self.faixa_potencia_kw,
            "num_paineis": self.num_paineis,
            "potencia_estimada_kw": self.potencia_estimada_kw,
        }


@dataclass
class DetectionResultDTO:
    """DTO for complete detection result"""
    
    sucesso: bool
    paineis: List[PainelSolarDTO] = field(default_factory=list)
    estimativa_potencia: Optional[EstimativaPotenciaDTO] = None
    classificacao: Optional[PropertyClassificationDTO] = None
    erros: List[str] = field(default_factory=list)
    tempo_processamento_s: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "sucesso": self.sucesso,
            "paineis": [p.to_dict() for p in self.paineis],
            "estimativa_potencia": (
                self.estimativa_potencia.to_dict()
                if self.estimativa_potencia
                else None
            ),
            "classificacao": (
                self.classificacao.to_dict() if self.classificacao else None
            ),
            "erros": self.erros,
            "tempo_processamento_s": self.tempo_processamento_s,
        }


@dataclass
class PowerEstimationRequestDTO:
    """DTO for power estimation request"""
    
    detections: List[Dict]
    resolution_m_per_pixel: float = 0.3
    power_density: float = 200  # W/m┬▓
    efficiency: float = 0.20  # 20%
    location: str = "Brazil"
    capacity_factor: float = 0.18


@dataclass
class PowerEstimationResponseDTO:
    """DTO for power estimation response"""
    
    total_power_kw: float
    power_from_area_kw: float
    power_from_count_kw: float
    total_area_m2: float
    num_panels_detected: int
    avg_power_per_panel_kw: float
    annual_production_kwh: float
    daily_avg_kwh: float
    monthly_avg_kwh: float
    annual_savings_brl: float
    monthly_savings_brl: float
    estimated_payback_years: float
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "total_power_kw": self.total_power_kw,
            "power_from_area_kw": self.power_from_area_kw,
            "power_from_count_kw": self.power_from_count_kw,
            "total_area_m2": self.total_area_m2,
            "num_panels_detected": self.num_panels_detected,
            "avg_power_per_panel_kw": self.avg_power_per_panel_kw,
            "annual_production_kwh": self.annual_production_kwh,
            "daily_avg_kwh": self.daily_avg_kwh,
            "monthly_avg_kwh": self.monthly_avg_kwh,
            "annual_savings_brl": self.annual_savings_brl,
            "monthly_savings_brl": self.monthly_savings_brl,
            "estimated_payback_years": self.estimated_payback_years,
        }
