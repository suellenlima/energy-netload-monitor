"""Caso de uso: Obter perfil de carga por classe."""

from dataclasses import dataclass

from ...domain.load_calculation import (
    LoadCalculationError,
    PerfilCargaHorario,
    PerfilNaoEncontradoError,
    LoadCalculationRepository,
)


@dataclass(frozen=True)
class ObterPerfilClasseUseCase:
    """Obtém o perfil de carga horário para uma classe de consumo."""

    repository: LoadCalculationRepository

    def executar(self, classe: str) -> PerfilCargaHorario:
        """
        Executa o caso de uso.

        Args:
            classe: Classe de consumo (Residencial, Comercial, Industrial, Rural, Iluminação Pública, Serviço Público)

        Returns:
            PerfilCargaHorario com fatores normalizados para cada hora do dia

        Raises:
            PerfilNaoEncontradoError: Se o perfil não existir para a classe
            LoadCalculationError: Para outros erros
        """
        if not classe:
            raise LoadCalculationError("Classe não pode ser vazia")

        classe = classe.upper()
        classes_validas = ["RESIDENCIAL", "COMERCIAL", "INDUSTRIAL", "RURAL", "ILUMINACAO_PUBLICA", "SERVICO_PUBLICO"]
        if classe not in classes_validas:
            raise LoadCalculationError(f"Classe inválida: {classe}")

        perfil = self.repository.obter_perfil_classe(classe)

        if perfil is None:
            raise PerfilNaoEncontradoError(f"Perfil não encontrado para classe: {classe}")

        return perfil

    def validar_classe(self, classe: str) -> bool:
        """Valida se a classe é conhecida."""
        classes_validas = ["RESIDENCIAL", "COMERCIAL", "INDUSTRIAL", "RURAL", "ILUMINACAO_PUBLICA", "SERVICO_PUBLICO"]
        return classe.upper() in classes_validas

    def listar_classes_validas(self) -> list:
        """Retorna lista de classes válidas."""
        return ["RESIDENCIAL", "COMERCIAL", "INDUSTRIAL", "RURAL", "ILUMINACAO_PUBLICA", "SERVICO_PUBLICO"]
