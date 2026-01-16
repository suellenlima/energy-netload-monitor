from __future__ import annotations

import os

from sqlalchemy import create_engine
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