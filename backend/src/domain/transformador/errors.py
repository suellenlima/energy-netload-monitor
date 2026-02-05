"""Transformador domain-specific errors."""

from ..comum.errors import DomainError


class TransformadorError(DomainError):
    """Base exception for transformador domain errors."""

    pass


class TransformadorNotFoundError(TransformadorError):
    """Raised when a transformador is not found."""

    def __init__(self, transformador_id: int):
        """Initialize error with transformador ID."""
        super().__init__(
            f"Transformador with ID {transformador_id} not found.",
            code="TRANSFORMADOR_NOT_FOUND",
        )
        self.transformador_id = transformador_id


class InvalidTransformadorError(TransformadorError):
    """Raised when transformador data is invalid."""

    def __init__(self, message: str):
        """Initialize error with message."""
        super().__init__(message, code="INVALID_TRANSFORMADOR")


class AreaCoberturaNotFoundError(TransformadorError):
    """Raised when coverage area is not found for a transformador."""

    def __init__(self, transformador_id: int):
        """Initialize error with transformador ID."""
        super().__init__(
            f"Coverage area for transformador {transformador_id} not found.",
            code="AREA_COBERTURA_NOT_FOUND",
        )
        self.transformador_id = transformador_id
