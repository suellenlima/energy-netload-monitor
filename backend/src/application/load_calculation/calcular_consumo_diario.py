"""Caso de uso: Calcular consumo diário granular."""

from dataclasses import dataclass

from ...domain.load_calculation import (
    CalculadoraCarga,
    ConsumoGranular,
    LoadCalculationError,
    LoadCalculationRepository,
)


@dataclass(frozen=True)
class CalcularConsumiDiarioUseCase:
    """Calcula o consumo diário total e granular por hora para uma classe."""

    repository: LoadCalculationRepository

    def executar(self, classe: str) -> ConsumoGranular:
        """
        Executa o caso de uso.

        Args:
            classe: Classe de consumo

        Returns:
            ConsumoGranular com dados de consumo diário

        Raises:
            LoadCalculationError: Se houver erro no cálculo
            DadosConsumoInvalidosError: Se os dados forem inválidos
        """
        if not classe:
            raise LoadCalculationError("Classe não pode ser vazia")

        # Obter dados de consumo
        consumo_granular = self.repository.obter_consumo_granular(classe)

        if not consumo_granular:
            raise LoadCalculationError(f"Consumo granular não encontrado para classe: {classe}")

        # Validar consumo
        if consumo_granular.consumo_mwh <= 0:
            raise LoadCalculationError(f"Consumo deve ser positivo: {consumo_granular.consumo_mwh}")

        if consumo_granular.quantidade_ucs <= 0:
            raise LoadCalculationError(f"Quantidade de UCs deve ser positiva: {consumo_granular.quantidade_ucs}")

        return consumo_granular

    def calcular_consumo_medio_por_uc(self, consumo_granular: ConsumoGranular) -> float:
        """Calcula o consumo médio por UC."""
        if consumo_granular.quantidade_ucs <= 0:
            return 0.0

        return consumo_granular.consumo_mwh / consumo_granular.quantidade_ucs

    def calcular_consumo_diario(self, consumo_granular: ConsumoGranular) -> float:
        """Calcula o consumo diário total em MWh."""
        return consumo_granular.consumo_mwh
