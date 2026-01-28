"""Endpoints auxiliares para dados de suporte."""

import logging

from fastapi import APIRouter, Query

from ..core import DatabaseError
from .deps import AnaliseRepoDepends

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auxiliar", tags=["Auxiliar"])


@router.get("/distribuidoras", response_model=list[str])
def get_lista_distribuidoras(
    repo: AnaliseRepoDepends,
    subsistema: str | None = Query(default=None, description="Filtrar por subsistema"),
):
    """
    Lista distribuidoras disponíveis.

    - **subsistema**: Filtrar por subsistema (opcional)
    """
    try:
        return repo.get_distribuidoras(subsistema)
    except Exception as exc:
        logger.error(f"Erro ao listar distribuidoras: {exc}", exc_info=True)
        raise DatabaseError("Falha ao listar distribuidoras") from exc
