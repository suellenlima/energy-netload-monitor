"""Sistema de cache in-memory com TTL."""

from __future__ import annotations

import hashlib
import logging
from functools import wraps
from time import time
from typing import Any, Callable, TypeVar

from .config import get_settings

logger = logging.getLogger(__name__)

# Cache storage: {key: (value, timestamp)}
_cache: dict[str, tuple[Any, float]] = {}

T = TypeVar("T")


def _make_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """
    Cria chave de cache baseada no nome da função e argumentos.

    Args:
        func_name: Nome da função
        args: Argumentos posicionais
        kwargs: Argumentos nomeados

    Returns:
        Hash MD5 como chave de cache
    """
    key_data = f"{func_name}:{args}:{sorted(kwargs.items())}"
    return hashlib.md5(key_data.encode()).hexdigest()


def cached(ttl_seconds: int | None = None) -> Callable:
    """
    Decorator para cache de resultados de função.

    Args:
        ttl_seconds: Tempo de vida do cache em segundos.
                     Se None, usa o valor de cache_ttl_seconds do config.

    Returns:
        Função decorada com cache

    Exemplo:
        @cached(ttl_seconds=60)
        def get_data(param: str) -> dict:
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            settings = get_settings()
            cache_ttl = ttl_seconds or settings.cache_ttl_seconds

            cache_key = _make_cache_key(func.__name__, args, kwargs)

            # Verificar se existe no cache e não expirou
            if cache_key in _cache:
                value, cached_at = _cache[cache_key]
                if time() - cached_at < cache_ttl:
                    logger.debug(f"Cache hit: {func.__name__}")
                    return value

            # Cache miss ou expirado - executar função
            logger.debug(f"Cache miss: {func.__name__}")
            result = func(*args, **kwargs)

            # Armazenar no cache
            _cache[cache_key] = (result, time())

            return result

        return wrapper

    return decorator


def invalidate_cache(func_name: str | None = None) -> int:
    """
    Invalida entradas do cache.

    Args:
        func_name: Nome da função para invalidar.
                   Se None, limpa todo o cache.

    Returns:
        Número de entradas removidas
    """
    global _cache

    if func_name is None:
        count = len(_cache)
        _cache.clear()
        logger.info(f"Cache limpo: {count} entradas removidas")
        return count

    # Remover entradas que contêm o nome da função
    keys_to_remove = [k for k in _cache if func_name in k]
    for key in keys_to_remove:
        del _cache[key]

    logger.info(f"Cache invalidado para {func_name}: {len(keys_to_remove)} entradas")
    return len(keys_to_remove)


def get_cache_stats() -> dict:
    """
    Retorna estatísticas do cache.

    Returns:
        Dict com total de entradas e uso de memória aproximado
    """
    return {
        "total_entries": len(_cache),
        "keys": list(_cache.keys())[:10],  # Primeiras 10 chaves
    }
