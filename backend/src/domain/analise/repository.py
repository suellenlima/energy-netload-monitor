"""Domain repository interface for Analise Module."""

from abc import ABC, abstractmethod
from typing import Optional

from .value_objects import (
    AlertaFraude,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    ResumoGranular,
    Anomalia,
    EstadoAtual,
    PerfilCarga,
)


class AnaliseRepository(ABC):
    """Abstract repository for Analise domain."""

    @abstractmethod
    def obter_carga_oculta(
        self, subsistema: str, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get hidden load analysis."""
        pass

    @abstractmethod
    def obter_classes_consumo(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get consumption classes."""
        pass

    @abstractmethod
    def obter_alerta_fraude(
        self, distribuidora: Optional[str] = None
    ) -> Optional[AlertaFraude]:
        """Get latest fraud alert."""
        pass

    @abstractmethod
    def obter_contagem_estabelecimentos(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get establishment count."""
        pass

    @abstractmethod
    def obter_resumo_granular(
        self, distribuidora: Optional[str] = None
    ) -> Optional[ResumoGranular]:
        """Get granular data summary."""
        pass

    @abstractmethod
    def obter_perfis_carga(
        self, classes: Optional[list[str]] = None
    ) -> list[PerfilCarga]:
        """Get load profiles."""
        pass

    @abstractmethod
    def obter_estado_atual(
        self,
        subsistema: str,
        distribuidora: Optional[str] = None,
        subestacao_id: Optional[int] = None,
    ) -> Optional[EstadoAtual]:
        """Get current system state."""
        pass

    @abstractmethod
    def obter_alertas_historico(
        self,
        distribuidora: Optional[str] = None,
        dias: int = 30,
        limite: int = 50,
    ) -> list[AlertaFraude]:
        """Get alert history."""
        pass

    @abstractmethod
    def detectar_anomalias(
        self, distribuidora: Optional[str] = None, limite: int = 10
    ) -> list[Anomalia]:
        """Detect anomalies."""
        pass

    @abstractmethod
    def obter_distribuidoras(
        self, subsistema: Optional[str] = None
    ) -> list[str]:
        """Get list of available electricity distributors."""
        pass
