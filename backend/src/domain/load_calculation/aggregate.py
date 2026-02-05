"""Domain aggregates for LoadCalculation Module."""

from dataclasses import dataclass
from typing import Optional

from .value_objects import CalibraçaoParametros, CargaCalculada, ConsumoGranular, MMGD, PerfilCargaHorario


@dataclass
class CalculadoraCarga:
    """Root aggregate for load calculation."""

    classe: str
    perfil: PerfilCargaHorario
    consumo: ConsumoGranular
    mmgd: Optional[MMGD] = None
    calibracao: Optional[CalibraçaoParametros] = None
    cargas_calculadas: list[CargaCalculada] = None

    def __post_init__(self):
        """Initialize cargas_calculadas if None."""
        if self.cargas_calculadas is None:
            self.cargas_calculadas = []

    def calcular_carga_hora(self, hora: int) -> Optional[CargaCalculada]:
        """Calculate load for specific hour."""
        if not 0 <= hora < 24:
            return None

        fator_horario = self.perfil.fatores_24h[hora]
        carga_base = self.consumo.consumo_mwh * fator_horario

        # Apply calibration if available
        if self.calibracao:
            carga_com_sazonalidade = carga_base * self.calibracao.fator_sazonalidade
        else:
            carga_com_sazonalidade = carga_base

        # Calculate final load (considering MMGD injection)
        if self.mmgd:
            carga_final = carga_com_sazonalidade - (self.mmgd.geracao_estimada_mw / 24)
        else:
            carga_final = carga_com_sazonalidade

        return CargaCalculada(
            classe=self.classe,
            hora=hora,
            carga_base_mw=carga_base,
            carga_com_sazonalidade_mw=carga_com_sazonalidade,
            carga_estimada_final_mw=max(0, carga_final),  # Avoid negative loads
            confiabilidade=0.85 if self.calibracao else 0.70,
        )

    def calcular_consumo_diario(self) -> float:
        """Calculate total daily consumption."""
        return self.consumo.consumo_mwh

    def tem_geracao_local(self) -> bool:
        """Check if has local MMGD generation."""
        return self.mmgd is not None and self.mmgd.geracao_estimada_mw > 0


@dataclass
class GeradorMMGD:
    """Root aggregate for distributed generation."""

    subsistema: str
    instalacoes_ativas: list[MMGD]

    def potencia_total_instalada(self) -> float:
        """Get total installed capacity."""
        return sum(m.potencia_instalada_mw for m in self.instalacoes_ativas)

    def geracao_total_estimada(self) -> float:
        """Get total estimated generation."""
        return sum(m.geracao_estimada_mw for m in self.instalacoes_ativas)

    def fator_utilizacao(self) -> float:
        """Calculate utilization factor."""
        potencia_total = self.potencia_total_instalada()
        if potencia_total == 0:
            return 0.0
        return self.geracao_total_estimada() / potencia_total
