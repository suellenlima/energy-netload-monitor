"""Domain entities for solar panel detection and analysis"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional
from abc import ABC, abstractmethod
import numpy as np


class PropertyType(str, Enum):
    """Property classification by solar installation type"""
    
    RESIDENCIAL = "residencial"
    COMERCIAL = "comercial"
    INDUSTRIAL = "industrial"
    UNKNOWN = "unknown"
    
    @property
    def description(self) -> str:
        """Get descriptive text for property type"""
        descriptions = {
            "residencial": "Resid├¬ncia unifamiliar",
            "comercial": "Estabelecimento comercial",
            "industrial": "Ind├║stria ou grande instala├º├úo",
            "unknown": "Tipo desconhecido",
        }
        return descriptions.get(self.value, "Tipo desconhecido")
    
    def power_range(self) -> tuple[float, float]:
        """Get typical power range (min, max) in kW"""
        ranges = {
            "residencial": (3, 10),
            "comercial": (10, 50),
            "industrial": (50, 500),
            "unknown": (0, 0),
        }
        return ranges.get(self.value, (0, 0))


@dataclass(frozen=True)
class Centroide:
    """Solar panel centroid value object"""
    
    x: float
    y: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary"""
        return {"x": self.x, "y": self.y}


@dataclass(frozen=True)
class BoundingBox:
    """Solar panel bounding box value object"""
    
    x: int
    y: int
    w: int
    h: int
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary"""
        return {"x": self.x, "y": self.y, "w": self.w, "h": self.h}


@dataclass
class PainelSolar:
    """Domain entity representing a detected solar panel"""
    
    id_painel: str
    bbox: BoundingBox
    centroide: Centroide
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_painel: str  # monocristalino, policristalino, filme fino, desconhecido
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate domain entity constraints"""
        if self.confianca < 0 or self.confianca > 1:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confianca}")
        
        if self.area_m2 < 0 or self.area_pixeis < 0:
            raise ValueError("Area values cannot be negative")
        
        if not self.id_painel:
            raise ValueError("Panel ID cannot be empty")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            "id_painel": self.id_painel,
            "bbox": self.bbox.to_dict(),
            "centroide": self.centroide.to_dict(),
            "area_pixeis": self.area_pixeis,
            "area_m2": self.area_m2,
            "confianca": self.confianca,
            "tipo_painel": self.tipo_painel,
            "timestamp_deteccao": self.timestamp_deteccao.isoformat(),
        }


@dataclass
class EstimativaPotencia:
    """Domain entity representing solar power estimation"""
    
    total_area_m2: float
    num_paineis: int
    potencia_instalada_kw: float
    potencia_por_m2: float = 150.0  # W/m┬▓ (standard for modern panels)
    producao_diaria_kwh: float = 0.0
    producao_anual_kwh: float = 0.0
    fator_capacidade: float = 0.15  # 15% is standard (varies 12-18%)
    insolacao_media_kwh_m2_dia: float = 4.5  # Brazil: 4-5.5 kWh/m┬▓/day
    economia_anual_brl: float = 0.0
    tarifa_media_brl_kwh: float = 0.80  # Average tariff Brazil (2026)
    
    def __post_init__(self):
        """Validate domain constraints"""
        if self.total_area_m2 < 0:
            raise ValueError("Total area cannot be negative")
        
        if self.num_paineis < 0:
            raise ValueError("Number of panels cannot be negative")
        
        if self.potencia_instalada_kw < 0:
            raise ValueError("Installed power cannot be negative")
        
        if self.potencia_por_m2 < 0:
            raise ValueError("Power density cannot be negative")
    
    def calcular(self) -> "EstimativaPotencia":
        """Calculate annual production and savings"""
        # Daily production = Power × Insolation / 1000
        self.producao_diaria_kwh = (
            self.potencia_instalada_kw * self.insolacao_media_kwh_m2_dia
        ) / 1000
        
        # Annual production
        self.producao_anual_kwh = self.producao_diaria_kwh * 365
        
        # Annual savings
        self.economia_anual_brl = self.producao_anual_kwh * self.tarifa_media_brl_kwh
        
        return self
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
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
class PropertyClassification:
    """Domain value object for property classification result"""
    
    property_type: PropertyType
    confidence: float
    num_panels: int
    avg_area_pixels: float
    total_area_pixels: float
    avg_confidence: float
    estimated_power_kw: Optional[float] = None
    
    def __post_init__(self):
        """Validate classification"""
        if self.confidence < 0 or self.confidence > 1:
            raise ValueError(f"Confidence must be between 0 and 1")
    
    @property
    def description(self) -> str:
        """Get property type description"""
        return self.property_type.description
    
    @property
    def power_range(self) -> tuple[float, float]:
        """Get typical power range for this property type"""
        return self.property_type.power_range()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "tipo": self.property_type.value,
            "confianca": self.confidence,
            "descricao": self.description,
            "faixa_potencia_kw": self.power_range,
            "num_paineis": self.num_panels,
            "potencia_estimada_kw": self.estimated_power_kw,
        }
