"""Domain aggregates for Analise Module."""

from dataclasses import dataclass
from typing import Optional

from .value_objects import (
    Anomalia,
    AlertaFraude,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)


@dataclass
class AnaliseCarga:
    """Root aggregate for load analysis."""

    subsistema: str
    carga_oculta: Optional[CargaOculta] = None
    classes_consumo: Optional[list[ClasseConsumo]] = None
    estabelecimentos: Optional[list[EstabelecimentoContagem]] = None
    resumo: Optional[ResumoGranular] = None

    def validar_dados(self) -> bool:
        """Validate analysis data consistency."""
        if self.carga_oculta and self.resumo:
            # Carga oculta should be less than total generation
            if self.carga_oculta.carga_oculta_estimada_mw > self.resumo.geracao_mmgd_mw * 1.5:
                return False
        return True

    def tem_consumo_valido(self) -> bool:
        """Check if consumption data is valid."""
        if not self.resumo:
            return False
        return self.resumo.consumo_total_mwh > 0


@dataclass
class MonitorAnomalias:
    """Root aggregate for anomaly monitoring."""

    distribuidora: str
    anomalias_detectadas: list[Anomalia]
    alertas_fraude: Optional[list[AlertaFraude]] = None
    estado_atual: Optional[EstadoAtual] = None

    def tem_criticas(self) -> bool:
        """Check if there are critical anomalies."""
        return any(a.eh_critica() for a in self.anomalias_detectadas)

    def impacto_total_kw(self) -> float:
        """Calculate total impact of anomalies."""
        return sum(a.impacto_kw for a in self.anomalias_detectadas)

    def contar_por_severidade(self) -> dict[str, int]:
        """Count anomalies by severity level."""
        contagem = {"critico": 0, "alto": 0, "medio": 0, "baixo": 0}
        for anomalia in self.anomalias_detectadas:
            severidade = anomalia.severidade.lower()
            if severidade in contagem:
                contagem[severidade] += 1
        return contagem


@dataclass
class PerfisCarga:
    """Root aggregate for load profiles."""

    classe_referencia: str
    perfis: list[PerfilCarga]
    data_atualizacao: str

    def obter_perfil(self, classe: str) -> Optional[PerfilCarga]:
        """Get profile for specific consumption class."""
        for perfil in self.perfis:
            if perfil.classe.lower() == classe.lower():
                return perfil
        return None

    def listar_classes_disponiveis(self) -> list[str]:
        """List available consumption classes."""
        return [p.classe for p in self.perfis]
