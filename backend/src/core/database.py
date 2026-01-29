from __future__ import annotations

from time import time

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from .config import get_settings

_engine: Engine | None = None

# Cache para verificação de existência de tabelas
_table_cache: dict[str, tuple[bool, float]] = {}

# Whitelist de tabelas permitidas para operações de deleção
ALLOWED_DELETE_TABLES = frozenset([
    "subestacoes_detectadas",
])


def get_engine() -> Engine:
    """Retorna engine SQLAlchemy com pool de conexões otimizado."""
    global _engine
    if _engine is None:
        settings = get_settings()
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL não configurada")
        _engine = create_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            pool_recycle=settings.db_pool_recycle,
        )
    return _engine

def get_db_connection():
    return get_engine().connect()

def table_exists(table_name: str, engine: Engine | None = None) -> bool:
    """
    Verificar se uma tabela existe no banco de dados.
    Usa cache com TTL configurável para evitar chamadas repetidas a inspect().
    """
    global _table_cache
    settings = get_settings()
    engine = engine or get_engine()
    cache_key = table_name

    # Verificar cache
    if cache_key in _table_cache:
        exists, cached_at = _table_cache[cache_key]
        if time() - cached_at < settings.cache_ttl_seconds:
            return exists

    # Cache expirado ou não existe - buscar do banco
    inspector = inspect(engine)
    exists = table_name in inspector.get_table_names()
    _table_cache[cache_key] = (exists, time())
    return exists


def invalidate_table_cache(table_name: str | None = None) -> None:
    """
    Invalida cache de tabelas.
    Se table_name for None, invalida todo o cache.
    """
    global _table_cache
    if table_name:
        _table_cache.pop(table_name, None)
    else:
        _table_cache.clear()

def delete_all_rows(table_name: str, engine: Engine | None = None) -> int:
    """
    Deletar todas as linhas de uma tabela.

    Args:
        table_name: Nome da tabela (deve estar na whitelist ALLOWED_DELETE_TABLES)
        engine: Engine SQLAlchemy (opcional)

    Returns:
        Número de linhas deletadas

    Raises:
        ValueError: Se a tabela não estiver na whitelist
    """
    if table_name not in ALLOWED_DELETE_TABLES:
        raise ValueError(
            f"Tabela '{table_name}' não permitida para deleção. "
            f"Tabelas permitidas: {', '.join(ALLOWED_DELETE_TABLES)}"
        )

    engine = engine or get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"DELETE FROM {table_name}"))
        conn.commit()
        # Invalidar cache da tabela modificada
        invalidate_table_cache(table_name)
        return result.rowcount