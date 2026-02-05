"""Caso de uso: Obter carga do ONS para subsistema."""

from dataclasses import dataclass

from ...domain.realtime_estimation import CargaONS, CargaONSNaoObtidaError, RealTimeEstimationRepository


@dataclass(frozen=True)
class ObterCargaONSUseCase:
    """Obtém a carga atual do ONS para um subsistema específico."""

    repository: RealTimeEstimationRepository

    def executar(self, subsistema: str) -> CargaONS:
        """
        Executa o caso de uso.

        Args:
            subsistema: Identificador do subsistema (SE, S, NE, N)

        Returns:
            CargaONS com dados de carga obtidos do ONS

        Raises:
            CargaONSNaoObtidaError: Se os dados não puderem ser obtidos
        """
        if not subsistema:
            raise CargaONSNaoObtidaError("Subsistema não pode ser vazio")

        subsistema = subsistema.upper()
        if subsistema not in ["SE", "S", "NE", "N"]:
            raise CargaONSNaoObtidaError(f"Subsistema inválido: {subsistema}")

        carga_ons = self.repository.obter_carga_ons(subsistema)

        if carga_ons is None:
            raise CargaONSNaoObtidaError(
                f"Não foi possível obter carga do ONS para {subsistema}"
            )

        return carga_ons

    def validar_subsistema(self, subsistema: str) -> bool:
        """Valida se o subsistema é válido."""
        return subsistema.upper() in ["SE", "S", "NE", "N"]
