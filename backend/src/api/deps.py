"""Dependências compartilhadas para injeção nos endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query
from sqlalchemy.engine import Engine

from ..core.database import get_engine
from ..repositories.analise_repository import AnaliseRepository
from ..repositories.subestacao_repository import SubestacaoRepository


def get_db_engine() -> Engine:
    """Dependência: Engine do banco de dados."""
    return get_engine()


# Type alias para injeção de Engine
EngineDepends = Annotated[Engine, Depends(get_db_engine)]


def get_subestacao_repository(engine: EngineDepends) -> SubestacaoRepository:
    """Dependência: Repositório de subestações."""
    return SubestacaoRepository(engine)


def get_analise_repository(engine: EngineDepends) -> AnaliseRepository:
    """Dependência: Repositório de análise."""
    return AnaliseRepository(engine)


# Type aliases para injeção de repositórios
SubestacaoRepoDepends = Annotated[
    SubestacaoRepository, Depends(get_subestacao_repository)
]
AnaliseRepoDepends = Annotated[
    AnaliseRepository, Depends(get_analise_repository)
]


# Query parameters comuns - sem default no Query (usar = no parâmetro)
LimiteQuery = Annotated[
    int,
    Query(ge=1, le=1000, description="Máximo de registros")
]
DistribuidoraQuery = Annotated[
    str | None,
    Query(description="Filtrar por distribuidora")
]
SubsistemaQuery = Annotated[
    str,
    Query(description="Subsistema elétrico")
]
