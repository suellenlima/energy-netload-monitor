"""Caso de uso: Obter geração estimada de MMGD."""

from dataclasses import dataclass

from ...domain.realtime_estimation import (
    GeracaoMMGD,
    GeracaoMMGDNaoCalculadaError,
    RealTimeEstimationRepository,
)


@dataclass(frozen=True)
class ObterGeracaoMMGDUseCase:
    """Obtém a geração estimada de microgeração/minigeração distribuída."""

    repository: RealTimeEstimationRepository

    def executar(self, subsistema: str) -> GeracaoMMGD:
        """
        Executa o caso de uso.

        Args:
            subsistema: Identificador do subsistema (SE, S, NE, N)

        Returns:
            GeracaoMMGD com estimativa de geração MMGD

        Raises:
            GeracaoMMGDNaoCalculadaError: Se a geração não puder ser estimada
        """
        if not subsistema:
            raise GeracaoMMGDNaoCalculadaError("Subsistema não pode ser vazio")

        subsistema = subsistema.upper()
        if subsistema not in ["SE", "S", "NE", "N"]:
            raise GeracaoMMGDNaoCalculadaError(f"Subsistema inválido: {subsistema}")

        geracao_mmgd = self.repository.obter_geracao_mmgd_estimada(subsistema)

        if geracao_mmgd is None:
            raise GeracaoMMGDNaoCalculadaError(
                f"Não foi possível estimar geração MMGD para {subsistema}"
            )

        return geracao_mmgd

    def validar_subsistema(self, subsistema: str) -> bool:
        """Valida se o subsistema é válido."""
        return subsistema.upper() in ["SE", "S", "NE", "N"]
