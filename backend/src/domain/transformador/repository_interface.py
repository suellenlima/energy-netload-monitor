"""Transformador repository interface.

Defines the contract for persisting and retrieving Transformador domain entities.
This interface is independent of implementation details (SQL, NoSQL, etc).
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from .entity import Transformador


class ITransformadorRepository(ABC):
    """
    Repository interface for Transformador domain entity.

    Defines the operations available for persisting and retrieving transformadores.
    """

    @abstractmethod
    def obter_por_id(self, transformador_id: int) -> Optional[Transformador]:
        """
        Retrieve a transformador by its ID.

        Args:
            transformador_id: Transformador ID

        Returns:
            Transformador domain entity or None if not found
        """
        pass

    @abstractmethod
    def obter_por_codigo(self, codigo: str) -> Optional[Transformador]:
        """
        Retrieve a transformador by its code.

        Args:
            codigo: ANEEL transformador code

        Returns:
            Transformador domain entity or None if not found
        """
        pass

    @abstractmethod
    def listar_todos(
        self, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """
        List all transformadores with pagination.

        Args:
            limite: Maximum number of results
            pagina: Page number (0-indexed)

        Returns:
            List of Transformador domain entities
        """
        pass

    @abstractmethod
    def listar_por_subestacao(
        self, subestacao_codigo: str, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """
        List all transformadores for a specific substation.

        Args:
            subestacao_codigo: Substation code
            limite: Maximum number of results
            pagina: Page number (0-indexed)

        Returns:
            List of Transformador domain entities
        """
        pass

    @abstractmethod
    def listar_por_distribuidora(
        self, distribuidora: str, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """
        List all transformadores for a specific distribution company.

        Args:
            distribuidora: Distribution company name
            limite: Maximum number of results
            pagina: Page number (0-indexed)

        Returns:
            List of Transformador domain entities
        """
        pass

    @abstractmethod
    def contar_total(self) -> int:
        """
        Count total number of transformadores.

        Returns:
            Total count
        """
        pass

    @abstractmethod
    def contar_por_subestacao(self, subestacao_codigo: str) -> int:
        """
        Count transformadores for a specific substation.

        Args:
            subestacao_codigo: Substation code

        Returns:
            Count of transformadores
        """
        pass

    @abstractmethod
    def obter_bbox_para_satelite(
        self, transformador_id: int, margem_km: float = 2.0
    ) -> Optional[dict]:
        """
        Get bounding box for satellite imagery with margin.

        Args:
            transformador_id: Transformador ID
            margem_km: Margin in kilometers

        Returns:
            Dict with {min_lat, min_lon, max_lat, max_lon}
        """
        pass

    @abstractmethod
    def listar_por_tipo_tensao(
        self, tipo_tensao: str, limite: int = 100, pagina: int = 0
    ) -> list:
        """
        List transformadores filtered by voltage type.

        Args:
            tipo_tensao: Voltage type (BT, MT, AT)
            limite: Max results
            pagina: Page (0-indexed)

        Returns:
            List of transformadores
        """
        pass

    @abstractmethod
    def contar_por_tipo_tensao(self, tipo_tensao: str) -> int:
        """Count transformadores by voltage type."""
        pass

    @abstractmethod
    def obter_estatisticas_gerais(self) -> dict:
        """Get general statistics about transformadores."""
        pass

    @abstractmethod
    def obter_estatisticas_areas(self) -> dict:
        """Get area-based statistics."""
        pass

    @abstractmethod
    def buscar_por_regiao(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        limite: int = 100,
        pagina: int = 0,
    ) -> list:
        """
        Search transformadores in geographic region.

        Args:
            min_lat, min_lon, max_lat, max_lon: Bounding box
            limite: Max results
            pagina: Page (0-indexed)

        Returns:
            List of transformadores
        """
        pass

    @abstractmethod
    def contar_por_regiao(
        self, min_lat: float, min_lon: float, max_lat: float, max_lon: float
    ) -> int:
        """Count transformadores in geographic region."""
        pass

    @abstractmethod
    def obter_resumo_consumidores(self, transformador_codigo: str) -> Optional[dict]:
        """Get consumer summary (BT/MT/AT counts)."""
        pass

    @abstractmethod
    def listar_consumidores_bt(self, transformador_codigo: str, limite: int) -> list:
        """List BT (low voltage) consumers."""
        pass

    @abstractmethod
    def listar_consumidores_mt(self, transformador_codigo: str, limite: int) -> list:
        """List MT (medium voltage) consumers."""
        pass

    @abstractmethod
    def listar_consumidores_at(self, transformador_codigo: str, limite: int) -> list:
        """List AT (high voltage) consumers."""
        pass

    @abstractmethod
    def exportar(self, formato: str = "json") -> Optional[str]:
        """Export all transformadores in specified format."""
        pass
