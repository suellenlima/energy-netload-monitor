"""Telhado repository interface."""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from src.domain.telhado.entity import Telhado


class ITelhadoRepository(ABC):
    """Interface for roof repository."""

    @abstractmethod
    def obter_por_id(self, telhado_id: int) -> Optional[Telhado]:
        """Get a roof by ID."""
        pass

    @abstractmethod
    def listar_todos(
        self, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List all roofs with pagination."""
        pass

    @abstractmethod
    def listar_por_transformador(
        self, transformador_id: int, limite: int = 100
    ) -> List[Telhado]:
        """List roofs for a specific transformer."""
        pass

    @abstractmethod
    def listar_por_confianca(
        self, min_confianca: float = 0.8, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs with minimum confidence level."""
        pass

    @abstractmethod
    def listar_por_area(
        self, min_area: float, max_area: float, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs within area range."""
        pass

    @abstractmethod
    def listar_por_orientacao(
        self, orientacao: str, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs with specific orientation."""
        pass

    @abstractmethod
    def obter_estatisticas(self) -> dict:
        """Get statistics about roofs."""
        pass
