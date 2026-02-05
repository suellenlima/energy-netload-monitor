"""
Aggregate for Satellite module.
"""
from dataclasses import dataclass
from typing import Optional, List
from .value_objects import Coordenadas, AreaCobertura, RequisicaoHistorico


@dataclass
class TransformadorSatelite:
    """Aggregate root for transformador satellite data"""
    transformador_id: int
    transformador_codigo: str
    transformador_nome: Optional[str] = None
    distribuidora: Optional[str] = None
    coordenadas: Optional[Coordenadas] = None
    tipo_tensao: Optional[str] = None
    area_cobertura: Optional[AreaCobertura] = None
    
    def validar_coordenadas(self) -> bool:
        """Validate if coordinates are available"""
        if not self.coordenadas:
            return False
        try:
            self.coordenadas.validar()
            return True
        except ValueError:
            return False
    
    def tem_area_cobertura(self) -> bool:
        """Check if coverage area has been calculated"""
        return self.area_cobertura is not None


@dataclass
class RequisicaoSatelite:
    """Satellite request aggregate"""
    transformador_id: int
    subestacao_id: int
    fonte_satelite: str  # 'google_maps', 'cbers4a'
    status: str  # 'sucesso', 'sem_cobertura', 'erro'
    imagem_id: Optional[str] = None
    url_download: Optional[str] = None
    cobertura_nuvem_percentual: Optional[float] = None
    resolucao_metros: Optional[float] = None
    tempo_requisicao_ms: Optional[int] = None
    custo_usd_estimado: Optional[float] = None
    data_requisicao: Optional[str] = None
    data_imagem: Optional[str] = None
    
    def foi_sucesso(self) -> bool:
        """Check if request was successful"""
        return self.status == 'sucesso'
    
    def pode_usar_para_deteccao(self) -> bool:
        """Check if image can be used for detection"""
        # Deve ter sucesso, ter imagem válida e nuvens < 80%
        if not self.foi_sucesso():
            return False
        if not self.imagem_id:
            return False
        if self.cobertura_nuvem_percentual and self.cobertura_nuvem_percentual > 80:
            return False
        return True
