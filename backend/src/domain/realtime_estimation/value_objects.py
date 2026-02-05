"""Domain value objects for RealTimeEstimation Module."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Irradiancia:
    """Solar irradiance value object."""

    wm2: float
    nuvem_percentual: float
    confiabilidade: float

    def __post_init__(self) -> None:
        """Validate irradiance."""
        if self.wm2 < 0:
            raise ValueError("Irradiância não pode ser negativa")
        if not 0 <= self.nuvem_percentual <= 100:
            raise ValueError("Nuvem deve estar entre 0 e 100%")
        if not 0 <= self.confiabilidade <= 1:
            raise ValueError("Confiabilidade deve estar entre 0 e 1")

    def esta_clara(self) -> bool:
        """Check if sky is clear."""
        return self.nuvem_percentual < 20


@dataclass(frozen=True)
class CargaONS:
    """ONS load data."""

    carga_mw: float
    hora_medicao: datetime
    subsistema: str
    precisao: float

    def __post_init__(self) -> None:
        """Validate ONS data."""
        if self.carga_mw < 0:
            raise ValueError("Carga não pode ser negativa")


@dataclass(frozen=True)
class GeracaoMMGD:
    """MMGD generation estimation."""

    geracao_estimada_mw: float
    confiabilidade_estimativa: float
    hora_calculo: datetime
    fatores_usados: dict

    def __post_init__(self) -> None:
        """Validate generation."""
        if self.geracao_estimada_mw < 0:
            raise ValueError("Geração não pode ser negativa")
        if not 0 <= self.confiabilidade_estimativa <= 1:
            raise ValueError("Confiabilidade deve estar entre 0 e 1")


@dataclass(frozen=True)
class EstadoSistemaReal:
    """Real-time system state."""

    timestamp: datetime
    hora_atual: int
    carga_ons_mw: float
    geracao_mmgd_mw: float
    consumo_estimado_mw: float
    irradiancia_wm2: float
    subsistema: str
    confiabilidade_geral: float

    def __post_init__(self) -> None:
        """Validate state."""
        if not 0 <= self.hora_atual < 24:
            raise ValueError("Hora deve estar entre 0 e 23")
        if any(
            v < 0
            for v in [
                self.carga_ons_mw,
                self.geracao_mmgd_mw,
                self.consumo_estimado_mw,
                self.irradiancia_wm2,
            ]
        ):
            raise ValueError("Valores não podem ser negativos")


@dataclass(frozen=True)
class Previsao:
    """Short-term forecast."""

    proximaHora_mw: float
    proximas3horas_mw: float
    proximias24horas_mw: float
    confiabilidade: float
    data_geracao: datetime
