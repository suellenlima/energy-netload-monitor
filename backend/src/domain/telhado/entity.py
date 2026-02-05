"""Telhado (Roof) entity."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.domain.telhado.value_objects import (
    AreaTelhado,
    CodigoTelhado,
    InclinacaoTelhado,
    Orientacao,
)
from src.domain.comum.value_objects import Localizacao


@dataclass
class Telhado:
    """Roof aggregate root."""

    id: int
    codigo: CodigoTelhado
    localizacao: Localizacao
    area: AreaTelhado
    inclinacao: InclinacaoTelhado
    orientacao: Orientacao
    confianca_deteccao: float  # 0-1 confidence score
    transformador_id: Optional[int] = None
    consumidor_id: Optional[int] = None
    criado_em: Optional[datetime] = None
    atualizado_em: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Validate roof data."""
        if not 0 <= self.confianca_deteccao <= 1:
            raise ValueError("Detection confidence must be between 0 and 1.")

    def eh_alta_confianca(self) -> bool:
        """Check if detection has high confidence (>80%)."""
        return self.confianca_deteccao > 0.8

    def calcular_potencia_estimada_kw(self, eficiencia_painel: float = 0.2) -> float:
        """Calculate estimated solar power based on roof area."""
        # 1000 W/m² solar irradiance × area × efficiency
        return (1000 * self.area.valor * eficiencia_painel) / 1000

    def __str__(self) -> str:
        return f"Telhado({self.codigo}, {self.area})"
