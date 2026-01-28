"""Endpoints para análise de carga e fraude."""

import logging

from fastapi import APIRouter

from ..core import DatabaseError
from ..schemas import (
    AlertaFraude,
    CargaOcultaItem,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    ResumoGranular,
)
from .deps import AnaliseRepoDepends, DistribuidoraQuery, SubsistemaQuery

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analise", tags=["Análise"])


@router.get("/carga-oculta", response_model=list[CargaOcultaItem])
def calcular_carga_oculta(
    repo: AnaliseRepoDepends,
    subsistema: SubsistemaQuery = "SUDESTE",
    distribuidora: DistribuidoraQuery = None,
):
    """
    Calcula carga oculta estimada (geração solar não medida).

    - **subsistema**: Subsistema elétrico (SUDESTE, NORTE, NORDESTE, SUL)
    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_carga_oculta(subsistema, distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao calcular carga oculta: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular carga oculta") from exc


@router.get("/classes-consumo", response_model=list[ClasseConsumoItem])
def get_classes_consumo(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna consumo por classe de consumidor.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_classes_consumo(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar classes de consumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar classes de consumo") from exc


@router.get("/alertas-fraude", response_model=AlertaFraude | dict)
def get_alertas_fraude(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna último alerta de fraude detectado.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_alerta_fraude(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar alertas de fraude: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar alertas de fraude") from exc


@router.get("/estabelecimentos/contagem", response_model=list[EstabelecimentoContagem])
def get_contagem_estabelecimentos(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna contagem de estabelecimentos por tipo.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_contagem_estabelecimentos(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar contagem: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar contagem de estabelecimentos") from exc


@router.get("/estabelecimentos/resumo", response_model=ResumoGranular | dict)
def get_resumo_estabelecimentos(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna resumo geral dos dados granulares.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_resumo_granular(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar resumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar resumo de estabelecimentos") from exc
