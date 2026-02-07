"""Domain Repository Interface - Subestacao"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

from .entity import Subestacao


class ISubestacaoRepository(ABC):
    """Interface do repositório para Subestacao (abstração de persistência)"""
    
    @abstractmethod
    def obter_por_codigo(self, codigo: str) -> Optional[Subestacao]:
        """Obtém subestação por código.
        
        Args:
            codigo: Código único da subestação
            
        Returns:
            Subestacao ou None se não encontrada
        """
        pass
    
    @abstractmethod
    def listar_paginados(self, offset: int = 0, limite: int = 100) -> List[Subestacao]:
        """Lista subestações com paginação.
        
        Args:
            offset: Número de registros a pular
            limite: Máximo de registros a retornar
            
        Returns:
            Lista de Subestacao
        """
        pass
    
    @abstractmethod
    def listar_por_tensao(
        self,
        tensao_nominal_kv: float,
        offset: int = 0,
        limite: int = 100
    ) -> List[Subestacao]:
        """Lista subestações por tensão nominal.
        
        Args:
            tensao_nominal_kv: Tensão em kV
            offset: Paginação
            limite: Limite de registros
            
        Returns:
            Lista de Subestacao
        """
        pass
    
    @abstractmethod
    def listar_por_distribuidora(
        self,
        distribuidora_codigo: str,
        offset: int = 0,
        limite: int = 100
    ) -> List[Subestacao]:
        """Lista subestações por distribuidora.
        
        Args:
            distribuidora_codigo: Código da distribuidora
            offset: Paginação
            limite: Limite de registros
            
        Returns:
            Lista de Subestacao
        """
        pass
    
    @abstractmethod
    def contar_total(self) -> int:
        """Retorna total de subestações.
        
        Returns:
            Número total de subestações
        """
        pass
    
    @abstractmethod
    def contar_por_distribuidora(self, distribuidora_codigo: str) -> int:
        """Conta subestações por distribuidora.
        
        Args:
            distribuidora_codigo: Código da distribuidora
            
        Returns:
            Número de subestações
        """
        pass
    
    @abstractmethod
    def obter_estatisticas_gerais(self) -> Dict[str, Any]:
        """Obtém estatísticas gerais de subestações.
        
        Returns:
            Dicionário com estatísticas (total, tensão média, etc)
        """
        pass
    
    @abstractmethod
    def obter_mix_consumidores(self, subestacao_id: int) -> Dict[str, Any]:
        """Obtém mix de consumidores por subestação.
        
        Args:
            subestacao_id: ID da subestação
            
        Returns:
            Dicionário com mix de consumidores por classe
        """
        pass
