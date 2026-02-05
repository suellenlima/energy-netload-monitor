"""Application use cases for Analise Module."""

from dataclasses import dataclass
from typing import Optional

from ...domain.analise import (
    AlertaFraude,
    Anomalia,
    AnaliseRepository,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)


@dataclass
class ObtenerCargaOcultaUseCase:
    """Use case: Get hidden load (unmetered solar generation)."""

    repository: AnaliseRepository

    def executar(
        self, subsistema: str, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Execute use case."""
        return self.repository.obter_carga_oculta(subsistema, distribuidora)


@dataclass
class ObtenerClassesConsumoUseCase:
    """Use case: Get consumption classes."""

    repository: AnaliseRepository

    def executar(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Execute use case."""
        return self.repository.obter_classes_consumo(distribuidora)


@dataclass
class ObtenerAlertaFraudeUseCase:
    """Use case: Get latest fraud alert."""

    repository: AnaliseRepository

    def executar(
        self, distribuidora: Optional[str] = None
    ) -> Optional[dict]:
        """Execute use case."""
        return self.repository.obter_alerta_fraude(distribuidora)


@dataclass
class ObtenerContagemEstabelecimentosUseCase:
    """Use case: Get establishment count."""

    repository: AnaliseRepository

    def executar(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Execute use case."""
        return self.repository.obter_contagem_estabelecimentos(distribuidora)


@dataclass
class ObtenerResumoGranularUseCase:
    """Use case: Get granular data summary."""

    repository: AnaliseRepository

    def executar(
        self, distribuidora: Optional[str] = None
    ) -> Optional[ResumoGranular]:
        """Execute use case."""
        return self.repository.obter_resumo_granular(distribuidora)


@dataclass
class ObtenerPerfisCargaUseCase:
    """Use case: Get load profiles."""

    repository: AnaliseRepository

    def executar(
        self, classes: Optional[list[str]] = None
    ) -> list[PerfilCarga]:
        """Execute use case."""
        return self.repository.obter_perfis_carga(classes)


@dataclass
class ObtenerEstadoAtualUseCase:
    """Use case: Get current system state."""

    repository: AnaliseRepository

    def executar(
        self,
        subsistema: str,
        distribuidora: Optional[str] = None,
        subestacao_id: Optional[int] = None,
    ) -> Optional[EstadoAtual]:
        """Execute use case."""
        return self.repository.obter_estado_atual(
            subsistema, distribuidora, subestacao_id
        )


@dataclass
class ObtenerAlertasHistoricoUseCase:
    """Use case: Get alert history."""

    repository: AnaliseRepository

    def executar(
        self,
        distribuidora: Optional[str] = None,
        dias: int = 30,
        limite: int = 50,
    ) -> list[AlertaFraude]:
        """Execute use case."""
        return self.repository.obter_alertas_historico(distribuidora, dias, limite)


@dataclass
class DetectarAnomalasUseCase:
    """Use case: Detect anomalies."""

    repository: AnaliseRepository

    def executar(
        self, distribuidora: Optional[str] = None, limite: int = 10
    ) -> list[Anomalia]:
        """Execute use case."""
        return self.repository.detectar_anomalias(distribuidora, limite)
