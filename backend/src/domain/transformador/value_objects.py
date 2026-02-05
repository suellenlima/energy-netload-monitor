"""Transformador domain value objects."""

from dataclasses import dataclass
from typing import Optional

from ..comum.value_objects import Localizacao, Potencia, ValueObject


@dataclass(frozen=True)
class CodigoTransformador(ValueObject):
    """Transformador code - unique identifier in ANEEL database."""

    valor: str

    def __post_init__(self) -> None:
        """Validate code format."""
        if not self.valor or not self.valor.strip():
            raise ValueError("Transformador code cannot be empty.")
        if len(self.valor) > 50:
            raise ValueError("Transformador code cannot exceed 50 characters.")

    def __str__(self) -> str:
        """Return code as string."""
        return self.valor


@dataclass(frozen=True)
class NomeTransformador(ValueObject):
    """Transformador name."""

    valor: str

    def __post_init__(self) -> None:
        """Validate name."""
        # Allow empty names (database may have incomplete data)
        if self.valor and len(self.valor) > 255:
            raise ValueError("Transformador name cannot exceed 255 characters.")

    def __str__(self) -> str:
        """Return name as string."""
        return self.valor or "N/A"


@dataclass(frozen=True)
class TensaoTipo(ValueObject):
    """Voltage type (e.g., 'Alta', 'Média', 'Baixa')."""

    valor: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate voltage type."""
        # Allow any type from database
        pass

    def __str__(self) -> str:
        """Return type as string."""
        return self.valor or "UNKNOWN"


@dataclass(frozen=True)
class AreaCobertura(ValueObject):
    """Coverage area in GeoJSON format."""

    geojson: str  # GeoJSON as string
    wkt: Optional[str] = None  # Well-Known Text format

    def __post_init__(self) -> None:
        """Validate area data."""
        if not self.geojson or not self.geojson.strip():
            raise ValueError("Area coverage GeoJSON cannot be empty.")
