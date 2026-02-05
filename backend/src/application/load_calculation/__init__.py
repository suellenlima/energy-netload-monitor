"""Application layer use cases for LoadCalculation Module."""

from .calcular_carga_horario import CalcularCargaHorarioUseCase
from .calcular_consumo_diario import CalcularConsumiDiarioUseCase
from .obter_mmgd_subsistema import ObterMMGDSubsistemaUseCase
from .obter_perfil_classe import ObterPerfilClasseUseCase
from .salvar_carga_calculada import SalvarCargaCalculadaUseCase

__all__ = [
    "ObterPerfilClasseUseCase",
    "CalcularCargaHorarioUseCase",
    "CalcularConsumiDiarioUseCase",
    "ObterMMGDSubsistemaUseCase",
    "SalvarCargaCalculadaUseCase",
]
