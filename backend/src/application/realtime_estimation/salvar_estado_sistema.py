"""Caso de uso: Salvar estado do sistema em tempo real."""

from dataclasses import dataclass

from ...domain.realtime_estimation import EstadoSistemaReal, RealTimeEstimationRepository


@dataclass(frozen=True)
class SalvarEstadoSistemaUseCase:
    """Salva o estado atual do sistema em tempo real para histórico/análise."""

    repository: RealTimeEstimationRepository

    def executar(self, estado: EstadoSistemaReal) -> EstadoSistemaReal:
        """
        Executa o caso de uso.

        Args:
            estado: EstadoSistemaReal a ser salvo

        Returns:
            EstadoSistemaReal salvo com confirmação

        Raises:
            ValueError: Se o estado for inválido
        """
        if estado is None:
            raise ValueError("Estado não pode ser nulo")

        if not estado.subsistema:
            raise ValueError("Subsistema do estado não pode ser vazio")

        # Salvar no repositório
        estado_salvo = self.repository.salvar_estado(estado)

        return estado_salvo

    def validar_estado(self, estado: EstadoSistemaReal) -> bool:
        """Valida se o estado é válido para salvar."""
        if not estado:
            return False

        if not estado.subsistema or estado.subsistema.upper() not in ["SE", "S", "NE", "N"]:
            return False

        if estado.carga_ons_mw < 0 or estado.geracao_mmgd_mw < 0:
            return False

        return True
