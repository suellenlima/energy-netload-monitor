"""Domain repository interface for RealTimeEstimation Module."""

from abc import ABC, abstractmethod
from typing import Optional

from .value_objects import CargaONS, EstadoSistemaReal, GeracaoMMGD, Irradiancia, Previsao


class RealTimeEstimationRepository(ABC):
    """Abstract repository for RealTime domain."""

    @abstractmethod
    def obter_carga_ons(self, subsistema: str) -> Optional[CargaONS]:
        """Get current ONS load."""
        pass

    @abstractmethod
    def obter_irradiancia_atual(self, latitude: float, longitude: float) -> Optional[Irradiancia]:
        """Get current irradiance."""
        pass

    @abstractmethod
    def obter_geracao_mmgd_estimada(self, subsistema: str) -> Optional[GeracaoMMGD]:
        """Get estimated MMGD generation."""
        pass

    @abstractmethod
    def obter_estado_atual(self, subsistema: str) -> Optional[EstadoSistemaReal]:
        """Get current system state."""
        pass

    @abstractmethod
    def obter_previsoes(self, subsistema: str, horas: int = 24) -> list[Previsao]:
        """Get forecasts for next hours."""
        pass

    @abstractmethod
    def salvar_estado(self, estado: EstadoSistemaReal) -> None:
        """Save current state for history."""
        pass
