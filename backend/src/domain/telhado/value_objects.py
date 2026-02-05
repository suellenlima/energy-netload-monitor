"""Telhado (Roof) value objects."""

from dataclasses import dataclass
from typing import Optional

from src.domain.comum.value_objects import Localizacao, ValueObject


@dataclass(frozen=True)
class CodigoTelhado(ValueObject):
    """Roof detection code."""

    valor: str

    def __post_init__(self) -> None:
        """Validate code."""
        if not self.valor or not self.valor.strip():
            raise ValueError("Roof code cannot be empty.")

    def __str__(self) -> str:
        return self.valor


@dataclass(frozen=True)
class AreaTelhado(ValueObject):
    """Roof area in square meters."""

    valor: float

    def __post_init__(self) -> None:
        """Validate area."""
        if self.valor < 0:
            raise ValueError("Roof area cannot be negative.")

    def __str__(self) -> str:
        return f"{self.valor} m²"


@dataclass(frozen=True)
class InclinacaoTelhado(ValueObject):
    """Roof inclination angle in degrees."""

    valor: float

    def __post_init__(self) -> None:
        """Validate inclination."""
        if not 0 <= self.valor <= 90:
            raise ValueError("Roof inclination must be between 0 and 90 degrees.")

    def __str__(self) -> str:
        return f"{self.valor}°"


@dataclass(frozen=True)
class Orientacao(ValueObject):
    """Roof orientation (N, NE, E, SE, S, SW, W, NW)."""

    valor: str

    VALID_DIRECTIONS = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"}

    def __post_init__(self) -> None:
        """Validate orientation."""
        if self.valor and self.valor not in self.VALID_DIRECTIONS:
            raise ValueError(
                f"Invalid orientation: {self.valor}. "
                f"Must be one of {self.VALID_DIRECTIONS}"
            )

    def __str__(self) -> str:
        return self.valor or "UNKNOWN"
