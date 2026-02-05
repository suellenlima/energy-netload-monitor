"""Caso de uso: Salvar carga calculada."""

from dataclasses import dataclass

from ...domain.load_calculation import (
    CargaCalculada,
    LoadCalculationError,
    LoadCalculationRepository,
)


@dataclass(frozen=True)
class SalvarCargaCalculadaUseCase:
    """Salva a carga calculada para histórico e análise."""

    repository: LoadCalculationRepository

    def executar(self, carga: CargaCalculada) -> CargaCalculada:
        """
        Executa o caso de uso.

        Args:
            carga: CargaCalculada a ser salva

        Returns:
            CargaCalculada salva com confirmação

        Raises:
            LoadCalculationError: Se houver erro ao salvar
        """
        if not carga:
            raise LoadCalculationError("Carga não pode ser nula")

        # Validar carga
        if not carga.classe:
            raise LoadCalculationError("Classe da carga não pode ser vazia")

        if not (0 <= carga.hora <= 23):
            raise LoadCalculationError(f"Hora inválida: {carga.hora}")

        if carga.carga_estimada_final_mw < 0:
            raise LoadCalculationError(f"Carga não pode ser negativa: {carga.carga_estimada_final_mw}")

        if not (0 <= carga.confiabilidade <= 1):
            raise LoadCalculationError(f"Confiabilidade deve estar entre 0 e 1: {carga.confiabilidade}")

        # Salvar no repositório
        carga_salva = self.repository.salvar_carga_calculada(carga)

        return carga_salva

    def validar_carga(self, carga: CargaCalculada) -> bool:
        """Valida se a carga é válida para salvar."""
        if not carga:
            return False

        if not carga.classe or carga.hora < 0 or carga.hora > 23:
            return False

        if carga.carga_estimada_final_mw < 0:
            return False

        if not (0 <= carga.confiabilidade <= 1):
            return False

        return True
