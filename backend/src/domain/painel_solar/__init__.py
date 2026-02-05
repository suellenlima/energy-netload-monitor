"""Domain layer for solar panel detection and analysis"""

from .entity import (
    PropertyType,
    PainelSolar,
    EstimativaPotencia,
    PropertyClassification,
    BoundingBox,
    Centroide,
)
from .dto import (
    PainelSolarDTO,
    EstimativaPotenciaDTO,
    PropertyClassificationDTO,
    DetectionResultDTO,
)

__all__ = [
    "PropertyType",
    "PainelSolar",
    "EstimativaPotencia",
    "PropertyClassification",
    "BoundingBox",
    "Centroide",
    "PainelSolarDTO",
    "EstimativaPotenciaDTO",
    "PropertyClassificationDTO",
    "DetectionResultDTO",
]
