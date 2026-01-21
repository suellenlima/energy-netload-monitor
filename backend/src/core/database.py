from __future__ import annotations

import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL")

_engine: Engine | None = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _engine = create_engine(DATABASE_URL)
    return _engine

def get_db_connection():
    return get_engine().connect()

def table_exists(table_name: str, engine: Engine | None = None) -> bool:
    """Verificar se uma tabela existe no banco de dados."""
    engine = engine or get_engine()
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def delete_all_rows(table_name: str, engine: Engine | None = None) -> int:
    """Deletar todas as linhas de uma tabela."""
    engine = engine or get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"DELETE FROM {table_name}"))
        conn.commit()
        return result.rowcount