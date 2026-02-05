"""Domain value objects for LoadCalculation Module."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PerfilCargaHorario:
    """Hourly load profile."""

    classe: str
    fatores_24h: list[float]  # Normalized factors for 24 hours
    pico_hora: int
    minima_hora: int
    fator_pico: float

    def __post_init__(self) -> None:
        """Validate profile."""
        if len(self.fatores_24h) != 24:
            raise ValueError("Perfil deve ter 24 fatores horários")
        if any(f < 0 for f in self.fatores_24h):
            raise ValueError("Fatores não podem ser negativos")
        if self.fator_pico < 1.0:
            raise ValueError("Fator de pico deve ser >= 1.0")


@dataclass(frozen=True)
class ConsumoGranular:
    """Granular consumption data."""

    classe: str
    consumo_mwh: float
    quantidade_ucs: int
    consumo_medio_por_uc_kwh: float

    def __post_init__(self) -> None:
        """Validate consumption."""
        if self.consumo_mwh < 0:
            raise ValueError("Consumo não pode ser negativo")
        if self.quantidade_ucs < 0:
            raise ValueError("Quantidade de UCs não pode ser negativa")


@dataclass(frozen=True)
class MMGD:
    """Distributed generation (MMGD) data."""

    quantidade_instalacoes: int
    potencia_instalada_mw: float
    geracao_estimada_mw: float
    tipo_tecnologia: str

    def __post_init__(self) -> None:
        """Validate MMGD."""
        if self.potencia_instalada_mw < 0 or self.geracao_estimada_mw < 0:
            raise ValueError("Potência/Geração não pode ser negativa")


@dataclass(frozen=True)
class CargaCalculada:
    """Calculated load."""

    classe: str
    hora: int
    carga_base_mw: float
    carga_com_sazonalidade_mw: float
    carga_estimada_final_mw: float
    confiabilidade: float

    def __post_init__(self) -> None:
        """Validate calculated load."""
        if not 0 <= self.hora < 24:
            raise ValueError("Hora deve estar entre 0 e 23")
        if any(
            v < 0
            for v in [
                self.carga_base_mw,
                self.carga_com_sazonalidade_mw,
                self.carga_estimada_final_mw,
            ]
        ):
            raise ValueError("Cargas não podem ser negativas")


@dataclass(frozen=True)
class CalibraçaoParametros:
    """Calibration parameters for load models."""

    fator_sazonalidade: float
    fator_dia_semana: float
    fator_feriado: float
    ajuste_temperatura: float
    data_calibracao: str

    def __post_init__(self) -> None:
        """Validate parameters."""
        if not 0.5 <= self.fator_sazonalidade <= 1.5:
            raise ValueError("Fator de sazonalidade deve estar entre 0.5 e 1.5")
