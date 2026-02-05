"""Endpoints auxiliares para dados de suporte."""

import logging
from enum import Enum

from fastapi import APIRouter, Query

from ..core import DatabaseError
from .deps import AnaliseRepoDepends, SubsistemaRepoDepends

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auxiliar", tags=["Auxiliar"])


class SubsistemaEnum(str, Enum):
    """Subsistemas elétricos do Brasil"""
    SUDESTE = "SUDESTE"
    NORTE = "NORTE"
    NORDESTE = "NORDESTE"
    SUL = "SUL"


@router.get("/subsistemas", response_model=list[str])
def get_lista_subsistemas(repo: SubsistemaRepoDepends):
    """
    Lista todos os subsistemas disponíveis (ONS).
    
    Busca dados reais da tabela subsistema_ons_regiao através do repositório DDD.
    """
    try:
        subsistemas = repo.listar_nomes()
        
        if not subsistemas:
            logger.warning("Nenhum subsistema encontrado no repositório")
            return [s.value for s in SubsistemaEnum]
        
        return subsistemas
    except Exception as exc:
        logger.error(f"Erro ao listar subsistemas: {exc}", exc_info=True)
        logger.info("Retornando subsistemas padrão como fallback")
        return [s.value for s in SubsistemaEnum]


@router.get("/distribuidoras", response_model=list[str])
def get_lista_distribuidoras(
    repo: AnaliseRepoDepends,
    subsistema: str | None = Query(
        default=None, 
        description="Filtrar por subsistema (SUDESTE, NORTE, NORDESTE, SUL)"
    ),
):
    """
    Lista distribuidoras disponíveis.

    - **subsistema**: Filtrar por subsistema (SUDESTE, NORTE, NORDESTE, SUL) - opcional
    """
    try:
        # Normalize subsistema to uppercase for validation
        normalized_subsistema = subsistema.upper() if subsistema else None
        
        # Validate against enum values
        valid_values = {"SUDESTE", "NORTE", "NORDESTE", "SUL"}
        if normalized_subsistema and normalized_subsistema not in valid_values:
            raise ValueError(f"Subsistema inválido: {subsistema}. Valores válidos: {valid_values}")
        
        return repo.obter_distribuidoras(normalized_subsistema)
    except Exception as exc:
        logger.error(f"Erro ao listar distribuidoras: {exc}", exc_info=True)
        raise DatabaseError("Falha ao listar distribuidoras") from exc
