"""Schemas para subestações."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SubestacaoONSResponse(BaseModel):
    """Subestação do ONS (dados públicos oficiais)."""

    id: str
    nome: str
    sigla_se: str | None = None
    tensao_kv: float | None = None
    subsistema: str | None = None
    distribuidora: str | None = None
    latitude: float
    longitude: float
    fonte_dados: str | None = None

    class Config:
        from_attributes = True


class SubestacaoDetectadaResponse(BaseModel):
    """Subestação detectada automaticamente via clustering."""

    id: int
    cluster_id: int | None = None
    nome: str
    latitude: float
    longitude: float
    distribuidora: str | None = None
    subsistema: str | None = None
    quantidade_gd: int | None = None
    potencia_total_mw: float | None = None
    raio_deteccao_km: float | None = None
    data_deteccao: datetime | None = None

    class Config:
        from_attributes = True


class SubestacaoResumo(BaseModel):
    """Resumo de subestações por distribuidora."""

    distribuidora: str | None = None
    total_ons: int = 0
    total_detectadas: int = 0
    total: int = 0


class AtualizarDetectadasRequest(BaseModel):
    """Request para atualizar subestações detectadas via clustering."""

    distribuidora: str | None = Field(
        default=None,
        description="Processar apenas uma distribuidora específica"
    )
    eps_km: float = Field(
        default=5.0,
        ge=0.5,
        le=50.0,
        description="Área poligonal em km para clustering (baseada em bounding box)"
    )


class AtualizarDetectadasResponse(BaseModel):
    """Response da atualização de subestações detectadas."""

    status: str
    mensagem: str
    quantidade: int
    raio_km: float | None = None


class TaskAsyncResponse(BaseModel):
    """Response para tarefas executadas em background."""

    status: str
    task_id: str
    mensagem: str
