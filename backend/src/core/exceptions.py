"""Exceções customizadas e handlers para a aplicação."""

from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class AppException(Exception):
    """Exceção base da aplicação."""

    def __init__(
        self,
        message: str,
        error_code: str = "INTERNAL_ERROR",
        status_code: int = 500,
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        super().__init__(message)


class DatabaseError(AppException):
    """Erro de operação no banco de dados."""

    def __init__(self, message: str = "Erro ao acessar banco de dados"):
        super().__init__(
            message=message,
            error_code="DATABASE_ERROR",
            status_code=500,
        )


class NotFoundError(AppException):
    """Recurso não encontrado."""

    def __init__(self, message: str = "Recurso não encontrado"):
        super().__init__(
            message=message,
            error_code="NOT_FOUND",
            status_code=404,
        )


class ValidationError(AppException):
    """Erro de validação de dados."""

    def __init__(self, message: str = "Dados inválidos"):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=400,
        )


async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
    """Handler para exceções da aplicação."""
    logger.error(
        f"AppException: {exc.error_code} - {exc.message}",
        extra={"path": request.url.path, "method": request.method},
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.error_code,
            "message": exc.message,
        },
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handler para exceções não tratadas."""
    logger.error(
        f"Unhandled exception: {type(exc).__name__} - {exc}",
        exc_info=True,
        extra={"path": request.url.path, "method": request.method},
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_ERROR",
            "message": "Erro interno do servidor",
        },
    )
