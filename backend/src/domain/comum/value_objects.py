"""Base domain value objects.

Value Objects are immutable objects that represent concepts in the domain.
They are identified by their attributes, not by an ID.
Examples: Localizacao (Latitude/Longitude), Potencia, Temperatura
"""

from dataclasses import dataclass
from typing import Any


class ValueObject:
    """Base class for all value objects.

    Value objects must be immutable and comparable by value.
    """

    def __eq__(self, other: Any) -> bool:
        """Compare by value, not by identity."""
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        """Make value objects hashable."""
        return hash(tuple(sorted(self.__dict__.items())))

    def __repr__(self) -> str:
        """Return string representation."""
        attrs = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}({attrs})"


@dataclass(frozen=True)
class Localizacao(ValueObject):
    """Geographic location represented by latitude and longitude.

    Immutable value object that represents a point on Earth.
    """

    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        """Validate coordinates after initialization."""
        if not (-90 <= self.latitude <= 90):
            raise ValueError(
                f"Invalid latitude: {self.latitude}. Must be between -90 and 90."
            )
        if not (-180 <= self.longitude <= 180):
            raise ValueError(
                f"Invalid longitude: {self.longitude}. Must be between -180 and 180."
            )

    def __str__(self) -> str:
        """Return formatted location string."""
        return f"({self.latitude:.4f}, {self.longitude:.4f})"


@dataclass(frozen=True)
class Potencia(ValueObject):
    """Electrical power measurement in kVA.

    Immutable value object representing electrical power.
    """

    kva: float

    def __post_init__(self) -> None:
        """Validate power value after initialization."""
        if self.kva <= 0:
            raise ValueError(f"Power must be positive. Got: {self.kva} kVA")

    @property
    def mva(self) -> float:
        """Convert to MVA."""
        return self.kva / 1000

    @property
    def w(self) -> float:
        """Convert to Watts."""
        return self.kva * 1000

    def __str__(self) -> str:
        """Return formatted power string."""
        if self.kva >= 1000:
            return f"{self.mva:.2f} MVA"
        return f"{self.kva:.2f} kVA"


@dataclass(frozen=True)
class Temperatura(ValueObject):
    """Temperature measurement in Celsius.

    Immutable value object representing temperature.
    """

    celsius: float

    def __post_init__(self) -> None:
        """Validate temperature value after initialization."""
        if self.celsius < -273.15:
            raise ValueError(
                f"Invalid temperature: {self.celsius}°C. Below absolute zero."
            )

    @property
    def fahrenheit(self) -> float:
        """Convert to Fahrenheit."""
        return (self.celsius * 9 / 5) + 32

    @property
    def kelvin(self) -> float:
        """Convert to Kelvin."""
        return self.celsius + 273.15

    def __str__(self) -> str:
        """Return formatted temperature string."""
        return f"{self.celsius:.2f}°C"
