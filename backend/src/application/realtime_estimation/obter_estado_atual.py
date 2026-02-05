"""Caso de uso: Obter estado atual do sistema em tempo real."""

from dataclasses import dataclass
from typing import Optional

from ...domain.realtime_estimation import (
    EstadoSistemaReal,
    RealTimeEstimationRepository,
    RealTimeEstimationError,
)


@dataclass(frozen=True)
class ObterEstadoAtualUseCase:
    """Obtém o estado atual do sistema de tempo real para um subsistema."""

    repository: RealTimeEstimationRepository

    def executar(self, subsistema: str) -> EstadoSistemaReal:
        """
        Executa o caso de uso.

        Args:
            subsistema: Identificador do subsistema (SE, S, NE, N)

        Returns:
            EstadoSistemaReal com os dados atuais do sistema

        Raises:
            EstadoNaoDisponibleError: Se o estado não puder ser obtido
            RealTimeEstimationError: Para outros erros de estimação
        """
        if not subsistema:
            raise RealTimeEstimationError("Subsistema não pode ser vazio")

        subsistema = subsistema.upper()
        if subsistema not in ["SE", "S", "NE", "N"]:
            raise RealTimeEstimationError(f"Subsistema inválido: {subsistema}")

        estado = self.repository.obter_estado_atual(subsistema)
        return estado

    def validar_subsistema(self, subsistema: str) -> bool:
        """Valida se o subsistema é válido."""
        return subsistema.upper() in ["SE", "S", "NE", "N"]
