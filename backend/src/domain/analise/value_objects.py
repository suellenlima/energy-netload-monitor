"""Domain value objects for Analise Module."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CargaOculta:
    """Hidden load (unmetered solar generation)."""

    subsistema: str
    carga_oculta_estimada_mw: float
    total_mmgd_mw: float
    percentual_total: float
    periodo_analise: str

    def __post_init__(self) -> None:
        """Validate load values."""
        if self.carga_oculta_estimada_mw < 0:
            raise ValueError("Carga oculta não pode ser negativa")
        if self.total_mmgd_mw < 0:
            raise ValueError("MMGD não pode ser negativo")


@dataclass(frozen=True)
class ClasseConsumo:
    """Consumption class data."""

    classe: str
    consumo_mwh: float
    consumo_percentual: float
    quantidade_ucs: int

    def __post_init__(self) -> None:
        """Validate class data."""
        if self.consumo_mwh < 0:
            raise ValueError("Consumo não pode ser negativo")
        if not 0 <= self.consumo_percentual <= 100:
            raise ValueError("Percentual deve estar entre 0 e 100")


@dataclass(frozen=True)
class AlertaFraude:
    """Fraud alert."""

    id: int
    data_deteccao: datetime
    distribuidora: str
    tipo: str
    severidade: str
    descricao: str
    status: str
    impacto_kw: float

    def eh_critico(self) -> bool:
        """Check if alert is critical."""
        return self.severidade.lower() in ["critico", "alto"]


@dataclass(frozen=True)
class EstabelecimentoContagem:
    """Establishment counting."""

    tipo_estabelecimento: str
    quantidade: int
    consumo_medio_mwh: float

    def __post_init__(self) -> None:
        """Validate count."""
        if self.quantidade < 0:
            raise ValueError("Quantidade não pode ser negativa")


@dataclass(frozen=True)
class ResumoGranular:
    """Summary of granular data."""

    total_ucs: int
    consumo_total_mwh: float
    consumo_medio_por_uc_mwh: float
    geracao_mmgd_mw: float
    distribuidora: str
    periodo: str

    def __post_init__(self) -> None:
        """Validate summary."""
        if self.total_ucs < 0 or self.consumo_total_mwh < 0:
            raise ValueError("Valores não podem ser negativos")


@dataclass(frozen=True)
class PerfilCarga:
    """Load profile for a consumption class."""

    classe: str
    fatores_horarios: list[float]  # 24 hourly factors
    pico_hora: int
    minima_hora: int
    fator_pico: float  # peak / average

    def __post_init__(self) -> None:
        """Validate profile."""
        if len(self.fatores_horarios) != 24:
            raise ValueError("Perfil deve ter 24 fatores horários")
        if any(f < 0 for f in self.fatores_horarios):
            raise ValueError("Fatores não podem ser negativos")
        if self.fator_pico < 1.0:
            raise ValueError("Fator de pico deve ser >= 1.0")


@dataclass(frozen=True)
class EstadoAtual:
    """Current system state."""

    timestamp: datetime
    hora_atual: int
    carga_ons_mw: float
    geracao_mmgd_mw: float
    consumo_estimado_mw: float
    irradiancia_atual_wm2: float
    subsistema: str
    confiabilidade_estimativa: float  # 0-1

    def __post_init__(self) -> None:
        """Validate state."""
        if not 0 <= self.hora_atual < 24:
            raise ValueError("Hora deve estar entre 0 e 23")
        if any(v < 0 for v in [self.carga_ons_mw, self.geracao_mmgd_mw, self.consumo_estimado_mw, self.irradiancia_atual_wm2]):
            raise ValueError("Valores não podem ser negativos")


@dataclass(frozen=True)
class Anomalia:
    """Detected anomaly."""

    distribuidora: str
    tipo: str
    severidade: str
    desvio_percentual: float
    total_ucs_afetadas: int
    impacto_kw: float
    timestamp: datetime

    def __post_init__(self) -> None:
        """Validate anomaly."""
        if self.total_ucs_afetadas < 0:
            raise ValueError("UCs afetadas não pode ser negativo")
        if self.impacto_kw < 0:
            raise ValueError("Impacto não pode ser negativo")

    def eh_critica(self) -> bool:
        """Check if anomaly is critical."""
        return self.severidade.lower() in ["critico", "alto"]
