"""
Value Objects for Satellite domain.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Coordenadas:
    """Immutable coordinate pair (latitude, longitude)"""
    latitude: float
    longitude: float
    
    def validar(self) -> None:
        """Validate coordinate ranges"""
        if not (-90 <= self.latitude <= 90):
            raise ValueError(f"Latitude inválida: {self.latitude}")
        if not (-180 <= self.longitude <= 180):
            raise ValueError(f"Longitude inválida: {self.longitude}")


@dataclass(frozen=True)
class ResolucaoSatelite:
    """Resolution of satellite source"""
    resolucao_metros: float
    fonte: str  # 'google_maps' ou 'cbers4a'
    
    def eh_alta_resolucao(self) -> bool:
        """Check if resolution is high (< 1.5m)"""
        return self.resolucao_metros < 1.5


@dataclass(frozen=True)
class AreaCobertura:
    """Coverage area for transformador"""
    area_m2: float
    area_km2: float
    metodo_calculo: str
    num_consumidores: int
    num_vertices: int
    
    def validar(self) -> None:
        """Validate area values"""
        if self.area_m2 <= 0:
            raise ValueError(f"Área m² inválida: {self.area_m2}")
        if self.num_consumidores < 0:
            raise ValueError(f"Consumidores negativos: {self.num_consumidores}")


@dataclass(frozen=True)
class RequisicaoHistorico:
    """Single satellite request record"""
    id: int
    transformador_id: int
    subestacao_id: int
    fonte_satelite: str
    status: str  # 'sucesso', 'sem_cobertura', 'erro'
    imagem_id: Optional[str] = None
    url_download: Optional[str] = None
    cobertura_nuvem_percentual: Optional[float] = None
    resolucao_metros: Optional[float] = None
    tempo_requisicao_ms: Optional[int] = None
    custo_usd_estimado: Optional[float] = None
    
    def foi_sucesso(self) -> bool:
        """Check if request was successful"""
        return self.status == 'sucesso'


@dataclass(frozen=True)
class QuotaMensal:
    """Monthly quota for Google Maps"""
    requisicoes_mes: int
    limite_mensal: int
    
    @property
    def disponivel(self) -> int:
        """Calculate available quota"""
        return max(0, self.limite_mensal - self.requisicoes_mes)
    
    @property
    def percentual_uso(self) -> float:
        """Calculate usage percentage"""
        if self.limite_mensal == 0:
            return 0.0
        return (self.requisicoes_mes / self.limite_mensal) * 100
    
    def tem_quota_disponivel(self) -> bool:
        """Check if quota is available"""
        return self.disponivel > 0
