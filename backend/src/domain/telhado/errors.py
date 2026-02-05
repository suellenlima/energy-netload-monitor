"""Telhado (Roof) domain errors."""

from ..comum.errors import DomainError


class TelhadoError(DomainError):
    """Base error for telhado domain."""

    pass


class TelhadoNotFoundError(TelhadoError):
    """Raised when a roof is not found."""

    def __init__(self, telhado_id: int):
        self.telhado_id = telhado_id
        super().__init__(f"Telhado {telhado_id} not found")


class InvalidTelhadoError(TelhadoError):
    """Raised when roof data is invalid."""

    pass
