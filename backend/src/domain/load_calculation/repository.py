"""Domain repository interface for LoadCalculation Module."""

from abc import ABC, abstractmethod
from typing import Optional

from .value_objects import CalibraçaoParametros, CargaCalculada, ConsumoGranular, MMGD, PerfilCargaHorario


class LoadCalculationRepository(ABC):
    """Abstract repository for Load domain."""

    @abstractmethod
    def obter_perfil_classe(self, classe: str) -> Optional[PerfilCargaHorario]:
        """Get load profile for consumption class."""
        pass

    @abstractmethod
    def obter_consumo_granular(self, classe: str) -> Optional[ConsumoGranular]:
        """Get granular consumption data."""
        pass

    @abstractmethod
    def obter_mmgd_subsistema(self, subsistema: str) -> list[MMGD]:
        """Get MMGD installations for subsystem."""
        pass

    @abstractmethod
    def obter_calibracao(self, classe: str) -> Optional[CalibraçaoParametros]:
        """Get calibration parameters."""
        pass

    @abstractmethod
    def salvar_carga_calculada(self, carga: CargaCalculada) -> None:
        """Save calculated load."""
        pass

    @abstractmethod
    def obter_historico_cargas(self, classe: str, dias: int = 30) -> list[CargaCalculada]:
        """Get historical load calculations."""
        pass
