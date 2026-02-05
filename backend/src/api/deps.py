"""Dependências compartilhadas para injeção nos endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query
from sqlalchemy.engine import Engine

from ..core.database import get_engine
from ..domain.analise import AnaliseRepository
from ..infrastructure.persistence.analise import AnaliseRepositorySQLAlchemy
from ..infrastructure.persistence.realtime_estimation import SQLAlchemyRealTimeEstimationRepository
from ..infrastructure.persistence.load_calculation import SQLAlchemyLoadCalculationRepository


def get_db_engine() -> Engine:
    """Dependência: Engine do banco de dados."""
    return get_engine()


# Type alias para injeção de Engine
EngineDepends = Annotated[Engine, Depends(get_db_engine)]


def get_analise_repository(engine: EngineDepends) -> AnaliseRepositorySQLAlchemy:
    """Dependência: Repositório de análise."""
    return AnaliseRepositorySQLAlchemy(engine)


# Type alias para injeção de repositório Analise
AnaliseRepoDepends = Annotated[
    AnaliseRepositorySQLAlchemy, Depends(get_analise_repository)
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


# ========================================================================
# RealTimeEstimation Repository
# ========================================================================

def get_realtime_estimation_repository() -> SQLAlchemyRealTimeEstimationRepository:
    """Dependência: Repositório de estimação em tempo real."""
    return SQLAlchemyRealTimeEstimationRepository()


RealTimeEstimationRepoDepends = Annotated[
    SQLAlchemyRealTimeEstimationRepository, 
    Depends(get_realtime_estimation_repository)
]


# ========================================================================
# LoadCalculation Repository
# ========================================================================

def get_load_calculation_repository() -> SQLAlchemyLoadCalculationRepository:
    """Dependência: Repositório de cálculo de carga."""
    return SQLAlchemyLoadCalculationRepository()


LoadCalculationRepoDepends = Annotated[
    SQLAlchemyLoadCalculationRepository, 
    Depends(get_load_calculation_repository)
]
