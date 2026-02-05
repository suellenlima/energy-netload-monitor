"""Caso de uso: Calcular carga horária para uma classe."""

from dataclasses import dataclass
from typing import Union, List

from ...domain.load_calculation import (
    CargaCalculada,
    CalculadoraCarga,
    LoadCalculationError,
    LoadCalculationRepository,
)


@dataclass(frozen=True)
class CalcularCargaHorarioUseCase:
    """Calcula a carga estimada para cada hora do dia de uma classe."""

    repository: LoadCalculationRepository

    def executar(self, classe: str, hora: int = None) -> Union[CargaCalculada, List[CargaCalculada]]:
        """
        Executa o caso de uso.

        Args:
            classe: Classe de consumo
            hora: Hora específica (0-23) ou None para todas as 24 horas

        Returns:
            CargaCalculada para hora específica ou lista de CargaCalculada para todas as horas

        Raises:
            LoadCalculationError: Se houver erro no cálculo
            ClasseConsumoInvalidaError: Se a classe for inválida
        """
        if not classe:
            raise LoadCalculationError("Classe não pode ser vazia")

        # Obter perfil, consumo e calibração
        perfil = self.repository.obter_perfil_classe(classe)
        consumo = self.repository.obter_consumo_granular(classe)
        calibracao = self.repository.obter_calibracao(classe)

        if not perfil:
            raise LoadCalculationError(f"Perfil não encontrado para classe: {classe}")

        if not consumo:
            raise LoadCalculationError(f"Consumo não encontrado para classe: {classe}")

        # Criar calculadora
        calculadora = CalculadoraCarga(
            classe=classe,
            perfil=perfil,
            consumo=consumo,
            calibracao=calibracao,
        )

        # Calcular para hora específica ou todas as horas
        if hora is not None:
            if not (0 <= hora <= 23):
                raise LoadCalculationError(f"Hora inválida: {hora}")

            carga_calculada = calculadora.calcular_carga_hora(hora)
            return carga_calculada
        else:
            # Calcular para todas as 24 horas
            cargas = []
            for h in range(24):
                carga = calculadora.calcular_carga_hora(h)
                cargas.append(carga)
            return cargas

    def validar_hora(self, hora: int) -> bool:
        """Valida se a hora é válida."""
        return 0 <= hora <= 23
