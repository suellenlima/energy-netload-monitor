"""Infrastructure persistence module initialization."""

from .transformador.repository import SQLAlchemyTransformadorRepository
from .analise import AnaliseRepositorySQLAlchemy
from .satelite import SateliteRepositorySQLAlchemy
from .telhado_multifonte import TelhadoMultiFonteRepository
from .transformador_pipeline import TransformadorPipelineRepository
from .shared import BaseRepository

__all__ = [
    "SQLAlchemyTransformadorRepository",
    "AnaliseRepositorySQLAlchemy",
    "SateliteRepositorySQLAlchemy",
    "TelhadoMultiFonteRepository",
    "TransformadorPipelineRepository",
    "BaseRepository",
]
