"""Endpoints auxiliares para dados de suporte."""

import logging
from enum import Enum

from fastapi import APIRouter, Query

from ..core import DatabaseError
from .deps import AnaliseRepoDepends

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auxiliar", tags=["Auxiliar"])


class SubsistemaEnum(str, Enum):
    """Subsistemas elétricos do Brasil"""
    SUDESTE = "SUDESTE"
    NORTE = "NORTE"
    NORDESTE = "NORDESTE"
    SUL = "SUL"


@router.get("/distribuidoras", response_model=list[str])
def get_lista_distribuidoras(
    repo: AnaliseRepoDepends,
    subsistema: SubsistemaEnum | None = Query(
        default=None, 
        description="Filtrar por subsistema"
    ),
):
    """
    Lista distribuidoras disponíveis.

    - **subsistema**: Filtrar por subsistema (SUDESTE, NORTE, NORDESTE, SUL) - opcional
    """
    try:
        return repo.get_distribuidoras(subsistema.value if subsistema else None)
    except Exception as exc:
        logger.error(f"Erro ao listar distribuidoras: {exc}", exc_info=True)
        raise DatabaseError("Falha ao listar distribuidoras") from exc
