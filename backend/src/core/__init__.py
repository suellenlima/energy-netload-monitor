"""Core module - database utilities, logging, config and exception handling."""

from .config import Settings, get_settings
from .database import (
    delete_all_rows,
    get_db_connection,
    get_engine,
    invalidate_table_cache,
    table_exists,
)
from .exceptions import (
    AppException,
    DatabaseError,
    NotFoundError,
    ValidationError,
    app_exception_handler,
    generic_exception_handler,
)
from .logging_config import get_logger, setup_logging

__all__ = [
    # Config
    "Settings",
    "get_settings",
    # Database
    "get_engine",
    "get_db_connection",
    "table_exists",
    "invalidate_table_cache",
    "delete_all_rows",
    # Logging
    "setup_logging",
    "get_logger",
    # Exceptions
    "AppException",
    "DatabaseError",
    "NotFoundError",
    "ValidationError",
    "app_exception_handler",
    "generic_exception_handler",
]
