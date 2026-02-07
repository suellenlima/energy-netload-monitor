"""Configuração centralizada da aplicação usando Pydantic Settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configurações da aplicação com validação automática."""

    # Database
    database_url: str = ""
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_recycle: int = 3600

    # API
    api_title: str = "EFFICIENERGY API"
    api_version: str = "1.0.0"
    cors_origins: list[str] = ["*"]

    # Cache
    cache_ttl_seconds: int = 300

    # Logging
    log_level: str = "INFO"
    
    # Google Maps
    google_maps_api_key: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    """
    Retorna singleton de configurações.
    Usa cache para evitar leituras repetidas do .env.
    """
    return Settings()
