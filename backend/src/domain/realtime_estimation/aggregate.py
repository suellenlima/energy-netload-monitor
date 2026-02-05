"""Domain aggregates for RealTimeEstimation Module."""

from dataclasses import dataclass
from typing import Optional

from .value_objects import CargaONS, EstadoSistemaReal, GeracaoMMGD, Irradiancia, Previsao


@dataclass
class SistemaRealTime:
    """Root aggregate for real-time system monitoring."""

    subsistema: str
    carga_ons: Optional[CargaONS] = None
    irradiancia: Optional[Irradiancia] = None
    geracao_mmgd: Optional[GeracaoMMGD] = None
    estado_atual: Optional[EstadoSistemaReal] = None

    def estimar_consumo(self) -> float:
        """Estimate total consumption."""
        if not self.carga_ons or not self.geracao_mmgd:
            return 0.0
        return self.carga_ons.carga_mw + self.geracao_mmgd.geracao_estimada_mw

    def calcular_confiabilidade_geral(self) -> float:
        """Calculate overall confidence."""
        if not self.irradiancia or not self.geracao_mmgd:
            return 0.5
        return (
            self.irradiancia.confiabilidade
            + self.geracao_mmgd.confiabilidade_estimativa
        ) / 2

    def validar_estado(self) -> bool:
        """Validate system state consistency."""
        if self.estado_atual and self.geracao_mmgd:
            return (
                self.estado_atual.geracao_mmgd_mw
                == self.geracao_mmgd.geracao_estimada_mw
            )
        return True


@dataclass
class MonitorPrevisao:
    """Root aggregate for forecasting."""

    subsistema: str
    previsoes: list[Previsao]

    def obter_proxima_hora(self) -> Optional[float]:
        """Get next hour forecast."""
        if self.previsoes:
            return self.previsoes[0].proximaHora_mw
        return None

    def confiabilidade_media(self) -> float:
        """Calculate average confidence."""
        if not self.previsoes:
            return 0.0
        return sum(p.confiabilidade for p in self.previsoes) / len(self.previsoes)
