"""Schemas para análise de carga e fraude."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CargaOcultaItem(BaseModel):
    """Item de carga oculta por hora."""

    hora: datetime
    carga_ons: float
    sol_wm2: float
    sol_wm2_final: float
    estimativa_solar_mw: float
    carga_real_estimada: float


class ClasseConsumoItem(BaseModel):
    """Consumo por classe de consumidor."""

    classe: str
    mw: float


class AlertaFraude(BaseModel):
    """Alerta de fraude detectado."""

    data: datetime | None = None
    local: str
    distribuidora: str | None = None
    classe_ia: str
    fraude_kw: float | None = None
    oficial_kw: float | None = None
    status: str | None = None


class EstabelecimentoContagem(BaseModel):
    """Contagem por tipo de estabelecimento."""

    tipo: str
    quantidade: int = 0
    total_unidades: int = 0
    total_mw: float = 0.0


class ResumoGranular(BaseModel):
    """Resumo geral dos dados granulares."""

    total_instalacoes: int = 0
    total_unidades_consumidoras: int = 0
    total_mw: float = 0.0
    por_tipo: dict[str, EstabelecimentoContagem] = Field(default_factory=dict)
