"""Telhado (Roof) application use cases."""

from typing import List, Tuple

from src.domain.telhado import ITelhadoRepository, Telhado, TelhadoNotFoundError


class ObtenerTelhadoUseCase:
    """Get a single roof by ID."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(self, telhado_id: int) -> Telhado:
        """Execute use case."""
        telhado = self.repository.obter_por_id(telhado_id)
        if not telhado:
            raise TelhadoNotFoundError(telhado_id)
        return telhado


class ListarTelhadosUseCase:
    """List all roofs with pagination."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(self, limite: int = 100, pagina: int = 0) -> Tuple[List[Telhado], int]:
        """Execute use case."""
        return self.repository.listar_todos(limite=limite, pagina=pagina)


class ListarTelhadosPorTransformadorUseCase:
    """List roofs for a specific transformer."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(self, transformador_id: int, limite: int = 100) -> List[Telhado]:
        """Execute use case."""
        return self.repository.listar_por_transformador(
            transformador_id=transformador_id, limite=limite
        )


class ListarTelhadosPorConfiancaUseCase:
    """List roofs with minimum confidence level."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(
        self, min_confianca: float = 0.8, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """Execute use case."""
        return self.repository.listar_por_confianca(
            min_confianca=min_confianca, limite=limite, pagina=pagina
        )


class ListarTelhadosPorAreaUseCase:
    """List roofs within area range."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(
        self,
        min_area: float,
        max_area: float,
        limite: int = 100,
        pagina: int = 0,
    ) -> Tuple[List[Telhado], int]:
        """Execute use case."""
        return self.repository.listar_por_area(
            min_area=min_area, max_area=max_area, limite=limite, pagina=pagina
        )


class ListarTelhadosPorOrientacaoUseCase:
    """List roofs with specific orientation."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(
        self, orientacao: str, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """Execute use case."""
        return self.repository.listar_por_orientacao(
            orientacao=orientacao, limite=limite, pagina=pagina
        )


class GetTelhadoEstatisticasUseCase:
    """Get statistics about roofs."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(self) -> dict:
        """Execute use case."""
        return self.repository.obter_estatisticas()


class CalcularPotencialSolarUseCase:
    """Calculate solar potential for a roof."""

    def __init__(self, repository: ITelhadoRepository):
        self.repository = repository

    def execute(self, telhado_id: int) -> dict:
        """Execute use case."""
        telhado = self.repository.obter_por_id(telhado_id)
        if not telhado:
            raise TelhadoNotFoundError(telhado_id)

        potencia_kw = telhado.calcular_potencia_estimada_kw()

        return {
            "telhado_id": telhado.id,
            "area_m2": telhado.area.valor,
            "orientacao": str(telhado.orientacao),
            "inclinacao_graus": telhado.inclinacao.valor,
            "potencia_estimada_kw": potencia_kw,
            "confianca_deteccao": telhado.confianca_deteccao,
        }
