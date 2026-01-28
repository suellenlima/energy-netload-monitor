from __future__ import annotations

import os
from time import time

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL")

_engine: Engine | None = None

# Cache para verificação de existência de tabelas
_table_cache: dict[str, tuple[bool, float]] = {}
_CACHE_TTL = 300  # 5 minutos


def get_engine() -> Engine:
    """Retorna engine SQLAlchemy com pool de conexões otimizado."""
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _engine = create_engine(
            DATABASE_URL,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _engine

def get_db_connection():
    return get_engine().connect()

def table_exists(table_name: str, engine: Engine | None = None) -> bool:
    """
    Verificar se uma tabela existe no banco de dados.
    Usa cache com TTL de 5 minutos para evitar chamadas repetidas a inspect().
    """
    global _table_cache
    engine = engine or get_engine()
    cache_key = table_name

    # Verificar cache
    if cache_key in _table_cache:
        exists, cached_at = _table_cache[cache_key]
        if time() - cached_at < _CACHE_TTL:
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
    Nota: table_name deve ser validado antes de chamar esta função.
    """
    engine = engine or get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"DELETE FROM {table_name}"))
        conn.commit()
        # Invalidar cache da tabela modificada
        invalidate_table_cache(table_name)
        return result.rowcount