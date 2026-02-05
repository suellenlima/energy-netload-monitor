"""Telhado Detection Service exports."""

from src.application.telhado_detection.service import (
    TelhadoDetectionService,
    DetectionResult,
)

__all__ = [
    "TelhadoDetectionService",
    "DetectionResult",
]
