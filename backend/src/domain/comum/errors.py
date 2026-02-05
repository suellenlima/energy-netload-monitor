"""Base domain exceptions.

All domain-specific exceptions inherit from DomainError.
This allows for a clear separation between business logic errors
and infrastructure/framework errors.
"""


class DomainError(Exception):
    """Base exception for all domain errors."""

    def __init__(self, message: str, code: str | None = None):
        """
        Initialize domain error.

        Args:
            message: Error message
            code: Optional error code for categorization
        """
        self.message = message
        self.code = code or self.__class__.__name__
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return formatted error string."""
        return f"[{self.code}] {self.message}"
