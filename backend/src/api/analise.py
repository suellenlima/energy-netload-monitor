from fastapi import APIRouter

from ..core.database import get_engine
from ..services.load_calc import (
    calculate_hidden_load,
    fetch_classes_consumption,
    fetch_fraud_alert,
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