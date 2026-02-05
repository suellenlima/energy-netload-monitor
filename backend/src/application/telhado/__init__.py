"""Telhado application module exports."""

from src.application.telhado.use_cases import (
    CalcularPotencialSolarUseCase,
    GetTelhadoEstatisticasUseCase,
    ListarTelhadosPorAreaUseCase,
    ListarTelhadosPorConfiancaUseCase,
    ListarTelhadosPorOrientacaoUseCase,
    ListarTelhadosPorTransformadorUseCase,
    ListarTelhadosUseCase,
    ObtenerTelhadoUseCase,
)

__all__ = [
    "ObtenerTelhadoUseCase",
    "ListarTelhadosUseCase",
    "ListarTelhadosPorTransformadorUseCase",
    "ListarTelhadosPorConfiancaUseCase",
    "ListarTelhadosPorAreaUseCase",
    "ListarTelhadosPorOrientacaoUseCase",
    "GetTelhadoEstatisticasUseCase",
    "CalcularPotencialSolarUseCase",
]
