"""Schemas para análise de carga e fraude."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CargaOcultaItem(BaseModel):
    """
    Item de carga oculta por hora.

    Representa a separação entre carga líquida (ONS) e consumo real,
    mostrando o impacto da geração distribuída (MMGD).
    """

    hora: datetime = Field(..., description="Timestamp da medição")
    carga_ons: float = Field(
        ...,
        description="Carga líquida medida pelo ONS (MW) - potência média horária",
        ge=0
    )
    sol_wm2: float = Field(
        ...,
        description="Irradiância solar medida (W/m²)",
        ge=0,
        le=1500
    )
    sol_wm2_final: float = Field(
        ...,
        description="Irradiância corrigida/estimada (W/m²) - usa perfil típico se faltante",
        ge=0,
        le=1500
    )
    estimativa_solar_mw: float = Field(
        ...,
        description="Geração MMGD estimada (MW) - calculada com base em capacidade instalada e irradiância",
        ge=0
    )
    carga_real_estimada: float = Field(
        ...,
        description="Consumo real estimado (MW) = Carga Líquida ONS + Geração MMGD",
        ge=0
    )


class CargaDistribuidoraAtual(BaseModel):
    """Carga atual em tempo real de uma distribuidora."""

    distribuidora: str = Field(..., description="Nome da distribuidora")
    carga_mw: float = Field(..., description="Carga em MW", ge=0)
    data_medicao: datetime = Field(..., description="Timestamp da medição")
    subsistema: str | None = Field(None, description="Subsistema ONS")


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


class PerfilCarga(BaseModel):
    """Perfil de carga horário para uma classe de consumo."""

    classe: str = Field(..., description="Classe de consumo (residencial, comercial, etc.)")
    curva: list[float] = Field(..., description="24 fatores de carga (0h-23h), normalizados para média=1.0")
    hora_pico: int = Field(..., description="Hora do dia com maior fator (0-23)")
    fator_pico: float = Field(..., description="Fator de carga no pico")
    hora_vale: int = Field(..., description="Hora do dia com menor fator (0-23)")
    fator_vale: float = Field(..., description="Fator de carga no vale")
    amplitude: float = Field(..., description="Diferença entre pico e vale")


class PerfisResponse(BaseModel):
    """Resposta com perfis de carga de múltiplas classes."""

    perfis: list[PerfilCarga] = Field(..., description="Lista de perfis por classe")
    classes_disponiveis: list[str] = Field(
        default_factory=list,
        description="Todas as classes disponíveis no sistema"
    )
