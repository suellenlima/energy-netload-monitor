"""Caso de uso: Obter MMGD do subsistema."""

from dataclasses import dataclass

from ...domain.load_calculation import (
    LoadCalculationError,
    MMGD,
    LoadCalculationRepository,
)


@dataclass(frozen=True)
class ObterMMGDSubsistemaUseCase:
    """Obtém os dados de microgeração/minigeração distribuída para um subsistema."""

    repository: LoadCalculationRepository

    def executar(self, subsistema: str) -> MMGD:
        """
        Executa o caso de uso.

        Args:
            subsistema: Identificador do subsistema (SE, S, NE, N)

        Returns:
            MMGD com dados de geração distribuída do subsistema

        Raises:
            LoadCalculationError: Se os dados não forem encontrados
        """
        if not subsistema:
            raise LoadCalculationError("Subsistema não pode ser vazio")

        subsistema = subsistema.upper()
        subsistemas_validos = ["SE", "S", "NE", "N"]
        if subsistema not in subsistemas_validos:
            raise LoadCalculationError(f"Subsistema inválido: {subsistema}")

        mmgd = self.repository.obter_mmgd_subsistema(subsistema)

        if not mmgd:
            raise LoadCalculationError(f"MMGD não encontrado para subsistema: {subsistema}")

        if mmgd.quantidade_instalacoes <= 0:
            raise LoadCalculationError(f"Quantidade de instalações deve ser positiva")

        if mmgd.potencia_instalada_mw <= 0:
            raise LoadCalculationError(f"Potência instalada deve ser positiva")

        return mmgd

    def validar_subsistema(self, subsistema: str) -> bool:
        """Valida se o subsistema é válido."""
        return subsistema.upper() in ["SE", "S", "NE", "N"]

    def calcular_fator_instalacao(self, mmgd: MMGD) -> float:
        """Calcula o fator de instalação (MW por unidade)."""
        if mmgd.quantidade_instalacoes <= 0:
            return 0.0
        return mmgd.potencia_instalada_mw / mmgd.quantidade_instalacoes
