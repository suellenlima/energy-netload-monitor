from fastapi import APIRouter

from ..core.database import get_engine
from ..services.load_calc import (
    calculate_hidden_load,
    fetch_classes_consumption,
    fetch_fraud_alert,
    fetch_establishment_counts,
    fetch_granular_summary,
)

router = APIRouter(prefix="/analise")


@router.get("/carga-oculta")
def calcular_carga_oculta(subsistema: str = "SUDESTE", distribuidora: str | None = None):
    engine = get_engine()
    return calculate_hidden_load(engine, subsistema, distribuidora)


@router.get("/classes-consumo")
def get_classes_consumo(distribuidora: str | None = None):
    engine = get_engine()
    return fetch_classes_consumption(engine, distribuidora)


@router.get("/alertas-fraude")
def get_alertas_fraude(distribuidora: str | None = None):
    engine = get_engine()
    return fetch_fraud_alert(engine, distribuidora)


@router.get("/estabelecimentos/contagem")
def get_contagem_estabelecimentos(distribuidora: str | None = None):
    """Retorna contagem de estabelecimentos por tipo."""
    engine = get_engine()
    return fetch_establishment_counts(engine, distribuidora)


@router.get("/estabelecimentos/resumo")
def get_resumo_estabelecimentos(distribuidora: str | None = None):
    """Retorna resumo geral dos dados granulares."""
    engine = get_engine()
    return fetch_granular_summary(engine, distribuidora)