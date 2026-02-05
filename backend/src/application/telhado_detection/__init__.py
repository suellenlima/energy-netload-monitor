"""Telhado Detection Service exports."""

from src.application.telhado_detection.service import (
    TelhadoDetectionService,
    DetectionResult,
)
from src.application.telhado_detection.multifonte_service import (
    TelhadoMultiFonteApplicationService,
)

__all__ = [
    "TelhadoDetectionService",
    "DetectionResult",
    "TelhadoMultiFonteApplicationService",
]
