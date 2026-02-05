"""Transformador application use cases.

Use cases implement the business logic workflows and orchestrate domain entities
and repositories. Each use case represents a single user action or business operation.
"""

from typing import List, Optional

from ...domain.transformador import (
    ITransformadorRepository,
    Transformador,
    TransformadorNotFoundError,
)


class ObtenerTransformadorUseCase:
    """Use case for obtaining a single transformador by ID."""

    def __init__(self, repository: ITransformadorRepository):
        """
        Initialize use case.

        Args:
            repository: Transformador repository
        """
        self.repository = repository

    def execute(self, transformador_id: int) -> Transformador:
        """
        Execute the use case.

        Args:
            transformador_id: ID of transformador to retrieve

        Returns:
            Transformador domain entity

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        transformador = self.repository.obter_por_id(transformador_id)

        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        return transformador


class ListarTransformadoresUseCase:
    """Use case for listing all transformadores with pagination."""

    def __init__(self, repository: ITransformadorRepository):
        """
        Initialize use case.

        Args:
            repository: Transformador repository
        """
        self.repository = repository

    def execute(
        self, limite: int = 100, pagina: int = 0
    ) -> tuple[List[Transformador], int]:
        """
        Execute the use case.

        Args:
            limite: Maximum number of results per page
            pagina: Page number (0-indexed)

        Returns:
            Tuple of (transformadores list, total count)
        """
        if limite <= 0 or limite > 1000:
            limite = 100

        if pagina < 0:
            pagina = 0

        transformadores = self.repository.listar_todos(limite=limite, pagina=pagina)
        total = self.repository.contar_total()

        return transformadores, total


class ListarTransformadoresPorSubestacaoUseCase:
    """Use case for listing transformadores by substation."""

    def __init__(self, repository: ITransformadorRepository):
        """
        Initialize use case.

        Args:
            repository: Transformador repository
        """
        self.repository = repository

    def execute(
        self, subestacao_codigo: str, limite: int = 100, pagina: int = 0
    ) -> tuple[List[Transformador], int]:
        """
        Execute the use case.

        Args:
            subestacao_codigo: Substation code
            limite: Maximum number of results per page
            pagina: Page number (0-indexed)

        Returns:
            Tuple of (transformadores list, total count)
        """
        if limite <= 0 or limite > 1000:
            limite = 100

        if pagina < 0:
            pagina = 0

        transformadores = self.repository.listar_por_subestacao(
            subestacao_codigo=subestacao_codigo, limite=limite, pagina=pagina
        )
        total = self.repository.contar_por_subestacao(subestacao_codigo)

        return transformadores, total


class ListarTransformadoresPorDistribuidoraUseCase:
    """Use case for listing transformadores by distribution company."""

    def __init__(self, repository: ITransformadorRepository):
        """
        Initialize use case.

        Args:
            repository: Transformador repository
        """
        self.repository = repository

    def execute(
        self, distribuidora: str, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """
        Execute the use case.

        Args:
            distribuidora: Distribution company name
            limite: Maximum number of results per page
            pagina: Page number (0-indexed)

        Returns:
            List of transformadores
        """
        if limite <= 0 or limite > 1000:
            limite = 100

        if pagina < 0:
            pagina = 0

        return self.repository.listar_por_distribuidora(
            distribuidora=distribuidora, limite=limite, pagina=pagina
        )


class ObtenerAreaCoberturaUseCase:
    """Use case for obtaining coverage area of a transformador."""

    def __init__(self, repository: ITransformadorRepository):
        """
        Initialize use case.

        Args:
            repository: Transformador repository
        """
        self.repository = repository

    def execute(self, transformador_id: int) -> Optional[str]:
        """
        Execute the use case.

        Args:
            transformador_id: ID of transformador

        Returns:
            GeoJSON string of coverage area or None if not found

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        # First verify transformador exists
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        # Get coverage area
        area_cobertura = self.repository.obter_area_cobertura(transformador_id)

        if area_cobertura:
            return area_cobertura.geojson

        return None


class GetBboxUseCase:
    """Use case for obtaining bounding box of a transformador with margin."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, transformador_id: int, margem_km: float = 2.0) -> Optional[dict]:
        """
        Get bounding box for satellite imagery.

        Args:
            transformador_id: ID of transformador
            margem_km: Margin in kilometers around the transformer

        Returns:
            Dict with {min_lat, min_lon, max_lat, max_lon} or None

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        return self.repository.obter_bbox_para_satelite(transformador_id, margem_km)


class ListarPorTipoTensaoUseCase:
    """Use case for listing transformadores by voltage type."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(
        self, tipo_tensao: str, limite: int = 100, pagina: int = 0
    ) -> tuple[List[Transformador], int]:
        """
        List transformadores filtered by voltage type.

        Args:
            tipo_tensao: Voltage type (BT, MT, AT)
            limite: Results per page
            pagina: Page number (0-indexed)

        Returns:
            Tuple of (transformadores list, total count)
        """
        if limite <= 0 or limite > 1000:
            limite = 100
        if pagina < 0:
            pagina = 0

        transformadores = self.repository.listar_por_tipo_tensao(
            tipo_tensao=tipo_tensao, limite=limite, pagina=pagina
        )
        total = self.repository.contar_por_tipo_tensao(tipo_tensao)

        return transformadores, total


class GetEstatisticasGeraisUseCase:
    """Use case for obtaining general statistics."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self) -> dict:
        """
        Get general statistics about transformadores.

        Returns:
            Dict with statistics (total, potencia_total, media_potencia, etc.)
        """
        return self.repository.obter_estatisticas_gerais()


class GetEstatisticasAreasUseCase:
    """Use case for obtaining area statistics."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self) -> dict:
        """
        Get statistics grouped by area/region.

        Returns:
            Dict with area statistics
        """
        return self.repository.obter_estatisticas_areas()


class BuscarRegiaoUseCase:
    """Use case for spatial region search."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        limite: int = 100,
        pagina: int = 0,
    ) -> tuple[List[Transformador], int]:
        """
        Search transformadores within a geographic region.

        Args:
            min_lat, min_lon, max_lat, max_lon: Bounding box coordinates
            limite: Results per page
            pagina: Page number (0-indexed)

        Returns:
            Tuple of (transformadores list, total count)
        """
        if limite <= 0 or limite > 1000:
            limite = 100
        if pagina < 0:
            pagina = 0

        transformadores = self.repository.buscar_por_regiao(
            min_lat=min_lat,
            min_lon=min_lon,
            max_lat=max_lat,
            max_lon=max_lon,
            limite=limite,
            pagina=pagina,
        )
        total = self.repository.contar_por_regiao(min_lat, min_lon, max_lat, max_lon)

        return transformadores, total


class GetResumoConsumidoresUseCase:
    """Use case for obtaining consumer summary."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, transformador_id: int) -> Optional[dict]:
        """
        Get summary of consumers (BT/MT/AT count) for a transformador.

        Args:
            transformador_id: ID of transformador

        Returns:
            Dict with consumer counts by type

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        return self.repository.obter_resumo_consumidores(
            transformador.codigo.valor
        )


class ListarConsumidoresBTUseCase:
    """Use case for listing low voltage consumers."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, transformador_id: int, limite: int = 100) -> Optional[list]:
        """
        List BT (low voltage) consumers for a transformador.

        Args:
            transformador_id: ID of transformador
            limite: Max results

        Returns:
            List of BT consumers

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        return self.repository.listar_consumidores_bt(
            transformador.codigo.valor, limite
        )


class ListarConsumidoresMTUseCase:
    """Use case for listing medium voltage consumers."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, transformador_id: int, limite: int = 100) -> Optional[list]:
        """
        List MT (medium voltage) consumers for a transformador.

        Args:
            transformador_id: ID of transformador
            limite: Max results

        Returns:
            List of MT consumers

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)

        return self.repository.listar_consumidores_mt(
            transformador.codigo.valor, limite
        )


class ExportarTransformadoresUseCase:
    """Use case for exporting transformadores in various formats."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, formato: str = "json") -> Optional[str]:
        """
        Export all transformadores in the specified format.

        Args:
            formato: Export format (json, csv, geojson)

        Returns:
            Exported data as string

        Raises:
            ValueError: If format not supported
        """
        if formato not in ["json", "csv", "geojson"]:
            raise ValueError(f"Format '{formato}' not supported")

        return self.repository.exportar(formato)


class ListarConsumidoresATUseCase:
    """Use case for listing high voltage consumers."""

    def __init__(self, repository: ITransformadorRepository):
        """Initialize use case."""
        self.repository = repository

    def execute(self, transformador_id: int, limite: int = 100) -> Optional[list]:
        """
        List AT (high voltage) consumers for a transformador.

        Args:
            transformador_id: ID of transformador
            limite: Max results

        Returns:
            List of AT consumers (empty list if error or not found)

        Raises:
            TransformadorNotFoundError: If transformador not found
        """
        try:
            transformador = self.repository.obter_por_id(transformador_id)
            if not transformador:
                raise TransformadorNotFoundError(transformador_id)

            return self.repository.listar_consumidores_at(
                transformador.codigo.valor, limite
            )
        except TransformadorNotFoundError:
            raise
        except Exception:
            # Return empty list on any other error (e.g., table not found)
            return []
