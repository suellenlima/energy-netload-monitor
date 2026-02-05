"""Caso de uso: Obter previsões de carga."""

from dataclasses import dataclass
from typing import List

from ...domain.realtime_estimation import Previsao, RealTimeEstimationRepository


@dataclass(frozen=True)
class ObterPrevisoesCargaUseCase:
    """Obtém previsões de carga para as próximas horas/dias."""

    repository: RealTimeEstimationRepository

    def executar(self, subsistema: str, horas: int = 24) -> List[Previsao]:
        """
        Executa o caso de uso.

        Args:
            subsistema: Identificador do subsistema (SE, S, NE, N)
            horas: Número de horas a prever (1-168 = até 7 dias)

        Returns:
            Lista de Previsao com previsões de carga

        Raises:
            ValueError: Se os parâmetros forem inválidos
        """
        if not subsistema:
            raise ValueError("Subsistema não pode ser vazio")

        subsistema = subsistema.upper()
        if subsistema not in ["SE", "S", "NE", "N"]:
            raise ValueError(f"Subsistema inválido: {subsistema}")

        if not (1 <= horas <= 168):
            raise ValueError("Horas deve estar entre 1 e 168 (7 dias)")

        previsoes = self.repository.obter_previsoes(subsistema, horas)
        return previsoes if previsoes else []

    def validar_subsistema(self, subsistema: str) -> bool:
        """Valida se o subsistema é válido."""
        return subsistema.upper() in ["SE", "S", "NE", "N"]

    def validar_horizonte_previsao(self, horas: int) -> bool:
        """Valida se o horizonte de previsão é válido."""
        return 1 <= horas <= 168
