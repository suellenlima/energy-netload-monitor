"""Caso de uso: Obter irradiância solar atual."""

from dataclasses import dataclass
from typing import Optional

from ...domain.realtime_estimation import (
    DadosIrradianciaInvalidosError,
    Irradiancia,
    RealTimeEstimationRepository,
)


@dataclass(frozen=True)
class ObterIrradianciaAtualUseCase:
    """Obtém a irradiância solar atual para uma localização geográfica."""

    repository: RealTimeEstimationRepository

    def executar(self, latitude: float, longitude: float) -> Irradiancia:
        """
        Executa o caso de uso.

        Args:
            latitude: Coordenada de latitude (-33 a 5)
            longitude: Coordenada de longitude (-75 a -35)

        Returns:
            Irradiancia com dados solares atuais

        Raises:
            DadosIrradianciaInvalidosError: Se as coordenadas forem inválidas
        """
        # Validar coordenadas do Brasil
        if not (-33 <= latitude <= 5):
            raise DadosIrradianciaInvalidosError(
                f"Latitude fora da faixa do Brasil: {latitude}"
            )

        if not (-75 <= longitude <= -35):
            raise DadosIrradianciaInvalidosError(
                f"Longitude fora da faixa do Brasil: {longitude}"
            )

        irradiancia = self.repository.obter_irradiancia_atual(latitude, longitude)
        return irradiancia

    def validar_coordenadas(self, latitude: float, longitude: float) -> bool:
        """Valida se as coordenadas estão no Brasil."""
        return -33 <= latitude <= 5 and -75 <= longitude <= -35
