"""Repository interface for Subsistema domain."""

from abc import ABC, abstractmethod

from .value_objects import Subsistema


class SubsistemaRepository(ABC):
    """Abstract repository for Subsistema entities."""
    
    @abstractmethod
    def listar_todos(self) -> list[Subsistema]:
        """
        Lista todos os subsistemas disponíveis.
        
        Returns:
            Lista de subsistemas ordenados
        """
        pass
    
    @abstractmethod
    def obter_por_codigo(self, codigo: str) -> Subsistema | None:
        """
        Obtém um subsistema pelo código.
        
        Args:
            codigo: Código do subsistema (ex: "NO", "NE")
            
        Returns:
            Subsistema encontrado ou None
        """
        pass
    
    @abstractmethod
    def listar_nomes(self) -> list[str]:
        """
        Lista apenas os nomes/identificadores dos subsistemas.
        
        Returns:
            Lista de nomes de subsistemas
        """
        pass
