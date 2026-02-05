"""
Repository interface for Satellite domain.
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from .aggregate import TransformadorSatelite, RequisicaoSatelite
from .value_objects import QuotaMensal, AreaCobertura, Coordenadas


class SateliteRepository(ABC):
    """Repository interface for satellite data"""
    
    @abstractmethod
    def obter_transformador(self, transformador_id: int) -> Optional[TransformadorSatelite]:
        """Get transformador by ID"""
        pass
    
    @abstractmethod
    def obter_coordenadas_transformador(
        self, transformador_id: int
    ) -> Optional[Coordenadas]:
        """Get coordinates for transformador"""
        pass
    
    @abstractmethod
    def obter_area_cobertura(
        self, transformador_id: int
    ) -> Optional[AreaCobertura]:
        """Get coverage area for transformador"""
        pass
    
    @abstractmethod
    def listar_historico_requisicoes(
        self,
        transformador_id: int,
        limite: int = 50,
        offset: int = 0,
        apenas_sucesso: bool = True
    ) -> tuple[List[RequisicaoSatelite], int]:
        """List satellite request history for transformador"""
        pass
    
    @abstractmethod
    def obter_quota_mensal_atual(self) -> QuotaMensal:
        """Get current month Google Maps quota"""
        pass
    
    @abstractmethod
    def obter_estatisticas_google_maps(self) -> Dict[str, Any]:
        """Get statistics for Google Maps usage"""
        pass
    
    @abstractmethod
    def registrar_requisicao(self, requisicao: RequisicaoSatelite) -> int:
        """Register satellite request in database"""
        pass
    
    @abstractmethod
    def decidir_fonte_melhor(
        self,
        transformador_id: int,
        tentar_google: bool = True,
        tentar_cbers: bool = True,
        forcar_cbers: bool = False
    ) -> Dict[str, Any]:
        """Decide best satellite source for transformador"""
        pass
