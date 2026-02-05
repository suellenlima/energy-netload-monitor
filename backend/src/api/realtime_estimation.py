"""
Endpoints para gerenciar dados de tempo real do sistema de energia.

Refatorado em 4 camadas DDD:
- API Layer: Recebe requisições HTTP, valida entrada
- Application Layer: Use cases com lógica de negócio
- Domain Layer: Entidades, agregados, lógica de domínio
- Infrastructure Layer: Acesso a banco de dados

Use Cases:
- ObterEstadoAtualUseCase: Estado completo do sistema em tempo real
- ObterIrradianciaAtualUseCase: Irradiância solar atual
- ObterCargaONSUseCase: Carga medida pelo ONS
- ObterGeracaoMMGDUseCase: Geração de MMGD estimada
- ObterPrevisoesCargaUseCase: Previsões de carga
- SalvarEstadoSistemaUseCase: Salvar estado para histórico

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Path, Depends
from pydantic import BaseModel, Field

from ..application.realtime_estimation import (
    ObterEstadoAtualUseCase,
    ObterIrradianciaAtualUseCase,
    ObterCargaONSUseCase,
    ObterGeracaoMMGDUseCase,
    ObterPrevisoesCargaUseCase,
    SalvarEstadoSistemaUseCase,
)
from ..domain.realtime_estimation import (
    EstadoSistemaReal,
    Irradiancia,
    CargaONS,
    GeracaoMMGD,
    Previsao,
    RealTimeEstimationError,
)
from ..infrastructure.persistence.realtime_estimation import RealTimeEstimationMapper
from .deps import get_realtime_estimation_repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/realtime", tags=["Tempo Real"])


# ========================================================================
# MODELOS PYDANTIC
# ========================================================================

class IrradianciaResponse(BaseModel):
    """Resposta com dados de irradiância solar"""
    wm2: float = Field(..., description="Irradiância em W/m²")
    nuvem_percentual: float = Field(..., description="Percentual de cobertura de nuvens")
    confiabilidade: float = Field(..., description="Confiabilidade da medição (0-1)")
    esta_clara: bool = Field(..., description="Indica se o céu está claro (wm2 > 500)")


class CargaONSResponse(BaseModel):
    """Resposta com dados de carga do ONS"""
    carga_mw: float = Field(..., description="Carga em MW")
    hora_medicao: datetime = Field(..., description="Hora da medição")
    subsistema: str = Field(..., description="Subsistema (SE, S, NE, N)")
    precisao: float = Field(..., description="Precisão da medição (0-1)")


class GeracaoMMGDResponse(BaseModel):
    """Resposta com geração estimada de MMGD"""
    geracao_estimada_mw: float = Field(..., description="Geração estimada em MW")
    confiabilidade_estimativa: float = Field(..., description="Confiabilidade da estimativa (0-1)")
    hora_calculo: datetime = Field(..., description="Hora do cálculo")
    fatores_usados: Dict[str, Any] = Field(..., description="Fatores utilizados no cálculo")


class EstadoSistemaResponse(BaseModel):
    """Resposta com estado completo do sistema em tempo real"""
    timestamp: datetime = Field(..., description="Timestamp da captura")
    hora_atual: datetime = Field(..., description="Hora atual do sistema")
    carga_ons_mw: float = Field(..., description="Carga medida pelo ONS (MW)")
    geracao_mmgd_mw: float = Field(..., description="Geração MMGD estimada (MW)")
    consumo_estimado_mw: float = Field(..., description="Consumo estimado (MW)")
    irradiancia_wm2: float = Field(..., description="Irradiância solar (W/m²)")
    subsistema: str = Field(..., description="Subsistema")
    confiabilidade_geral: float = Field(..., description="Confiabilidade geral (0-1)")
    carga_liquida_mw: float = Field(..., description="Carga líquida = ONS - MMGD")


class PrevisaoResponse(BaseModel):
    """Resposta com previsão de carga"""
    proxima_hora_mw: float = Field(..., description="Previsão próxima hora")
    proximas_3horas_mw: float = Field(..., description="Previsão próximas 3 horas")
    proximas_24horas_mw: float = Field(..., description="Previsão próximas 24 horas")
    confiabilidade: float = Field(..., description="Confiabilidade da previsão (0-1)")
    data_geracao: datetime = Field(..., description="Data de geração da previsão")


# ========================================================================
# ENDPOINTS
# ========================================================================

@router.get(
    "/estado/{subsistema}",
    response_model=EstadoSistemaResponse,
    summary="Obter estado atual do sistema",
    description="Retorna o estado completo do sistema em tempo real para um subsistema"
)
async def obter_estado_atual(
    subsistema: str = Path(..., description="Subsistema: SE, S, NE ou N"),
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Obtém o estado atual completo do sistema (carga, geração, consumo, irradiância).
    
    **Parâmetros:**
    - subsistema: SE (Sudeste), S (Sul), NE (Nordeste), N (Norte)
    
    **Retorna:**
    Estado completo com timestamp, cargas, geração e confiabilidade
    """
    try:
        use_case = ObterEstadoAtualUseCase(repository=repository)
        estado = use_case.executar(subsistema)
        return RealTimeEstimationMapper.estado_to_response(estado)
    except RealTimeEstimationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter estado: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro interno ao obter estado")


@router.get(
    "/irradiancia",
    response_model=IrradianciaResponse,
    summary="Obter irradiância solar atual",
    description="Retorna dados de irradiância solar para coordenadas geográficas"
)
async def obter_irradiancia_atual(
    latitude: float = Query(..., ge=-33, le=5, description="Latitude (-33 a 5)"),
    longitude: float = Query(..., ge=-75, le=-35, description="Longitude (-75 a -35)"),
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Obtém a irradiância solar atual para uma localização.
    
    **Parâmetros:**
    - latitude: -33 a 5 (Brasil)
    - longitude: -75 a -35 (Brasil)
    
    **Retorna:**
    Irradiância em W/m², percentual de nuvens e confiabilidade
    """
    try:
        use_case = ObterIrradianciaAtualUseCase(repository=repository)
        irradiancia = use_case.executar(latitude, longitude)
        return {
            "wm2": irradiancia.wm2,
            "nuvem_percentual": irradiancia.nuvem_percentual,
            "confiabilidade": irradiancia.confiabilidade,
            "esta_clara": irradiancia.esta_clara(),
        }
    except Exception as e:
        logger.error(f"Erro ao obter irradiância: {str(e)}")
        raise HTTPException(status_code=400, detail="Erro ao obter irradiância")


@router.get(
    "/carga-ons/{subsistema}",
    response_model=CargaONSResponse,
    summary="Obter carga do ONS",
    description="Retorna a carga medida pelo ONS para um subsistema"
)
async def obter_carga_ons(
    subsistema: str = Path(..., description="Subsistema: SE, S, NE ou N"),
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Obtém a carga atual medida pelo ONS.
    
    **Parâmetros:**
    - subsistema: SE (Sudeste), S (Sul), NE (Nordeste), N (Norte)
    
    **Retorna:**
    Carga em MW, hora da medição e precisão
    """
    try:
        use_case = ObterCargaONSUseCase(repository=repository)
        carga_ons = use_case.executar(subsistema)
        return {
            "carga_mw": carga_ons.carga_mw,
            "hora_medicao": carga_ons.hora_medicao,
            "subsistema": carga_ons.subsistema,
            "precisao": carga_ons.precisao,
        }
    except RealTimeEstimationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter carga ONS: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter carga ONS")


@router.get(
    "/geracao-mmgd/{subsistema}",
    response_model=GeracaoMMGDResponse,
    summary="Obter geração MMGD estimada",
    description="Retorna a geração estimada de microgeração/minigeração distribuída"
)
async def obter_geracao_mmgd(
    subsistema: str = Path(..., description="Subsistema: SE, S, NE ou N"),
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Obtém a geração estimada de MMGD para um subsistema.
    
    **Parâmetros:**
    - subsistema: SE (Sudeste), S (Sul), NE (Nordeste), N (Norte)
    
    **Retorna:**
    Geração em MW, confiabilidade e fatores utilizados
    """
    try:
        use_case = ObterGeracaoMMGDUseCase(repository=repository)
        geracao = use_case.executar(subsistema)
        return {
            "geracao_estimada_mw": geracao.geracao_estimada_mw,
            "confiabilidade_estimativa": geracao.confiabilidade_estimativa,
            "hora_calculo": geracao.hora_calculo,
            "fatores_usados": geracao.fatores_usados,
        }
    except RealTimeEstimationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter geração MMGD: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter geração MMGD")


@router.get(
    "/previsoes/{subsistema}",
    response_model=List[PrevisaoResponse],
    summary="Obter previsões de carga",
    description="Retorna previsões de carga para as próximas horas"
)
async def obter_previsoes_carga(
    subsistema: str = Path(..., description="Subsistema: SE, S, NE ou N"),
    horas: int = Query(24, ge=1, le=168, description="Número de horas a prever (1-168)"),
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Obtém previsões de carga para as próximas horas/dias.
    
    **Parâmetros:**
    - subsistema: SE (Sudeste), S (Sul), NE (Nordeste), N (Norte)
    - horas: Número de horas (1-168, máximo 7 dias)
    
    **Retorna:**
    Lista de previsões com confianças
    """
    try:
        use_case = ObterPrevisoesCargaUseCase(repository=repository)
        previsoes = use_case.executar(subsistema, horas)
        return [RealTimeEstimationMapper.previsao_to_response(p) for p in previsoes]
    except Exception as e:
        logger.error(f"Erro ao obter previsões: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter previsões")


@router.post(
    "/salvar-estado",
    response_model=EstadoSistemaResponse,
    summary="Salvar estado do sistema",
    description="Salva o estado atual do sistema para histórico e análise"
)
async def salvar_estado_sistema(
    estado: EstadoSistemaResponse,
    repository = Depends(get_realtime_estimation_repository),
):
    """
    Salva o estado do sistema em tempo real para histórico.
    
    **Body:**
    Estado completo do sistema a ser salvo
    
    **Retorna:**
    Estado salvo com confirmação de timestamp
    """
    try:
        use_case = SalvarEstadoSistemaUseCase(repository=repository)
        estado_obj = EstadoSistemaReal(
            timestamp=estado.timestamp,
            hora_atual=estado.hora_atual,
            carga_ons_mw=estado.carga_ons_mw,
            geracao_mmgd_mw=estado.geracao_mmgd_mw,
            consumo_estimado_mw=estado.consumo_estimado_mw,
            irradiancia_wm2=estado.irradiancia_wm2,
            subsistema=estado.subsistema,
            confiabilidade_geral=estado.confiabilidade_geral,
        )
        estado_salvo = use_case.executar(estado_obj)
        return RealTimeEstimationMapper.estado_to_response(estado_salvo)
    except Exception as e:
        logger.error(f"Erro ao salvar estado: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao salvar estado")
