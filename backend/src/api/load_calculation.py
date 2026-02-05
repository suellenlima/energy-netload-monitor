"""
Endpoints para gerenciar cálculo de carga de energia.

Refatorado em 4 camadas DDD:
- API Layer: Recebe requisições HTTP, valida entrada
- Application Layer: Use cases com lógica de negócio
- Domain Layer: Entidades, agregados, lógica de domínio
- Infrastructure Layer: Acesso a banco de dados

Use Cases:
- ObterPerfilClasseUseCase: Perfil de carga por classe de consumo
- CalcularCargaHorarioUseCase: Calcular carga para cada hora do dia
- CalcularConsumiDiarioUseCase: Calcular consumo diário granular
- ObterMMGDSubsistemaUseCase: Dados de MMGD por subsistema
- SalvarCargaCalculadaUseCase: Salvar carga calculada para histórico

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Path, Body, Depends
from pydantic import BaseModel, Field

from ..application.load_calculation import (
    ObterPerfilClasseUseCase,
    CalcularCargaHorarioUseCase,
    CalcularConsumiDiarioUseCase,
    ObterMMGDSubsistemaUseCase,
    SalvarCargaCalculadaUseCase,
)
from ..domain.load_calculation import (
    CargaCalculada,
    LoadCalculationError,
)
from ..infrastructure.persistence.load_calculation import LoadCalculationMapper
from .deps import get_load_calculation_repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/load", tags=["Cálculo de Carga"])


# ========================================================================
# MODELOS PYDANTIC
# ========================================================================

class PerfilCargaResponse(BaseModel):
    """Resposta com perfil de carga horário"""
    classe: str = Field(..., description="Classe de consumo")
    fatores_24h: List[float] = Field(..., description="Fatores normalizados para 24 horas")
    pico_hora: int = Field(..., description="Hora do pico (0-23)")
    minima_hora: int = Field(..., description="Hora de mínimo (0-23)")
    fator_pico: float = Field(..., description="Fator de pico")


class ConsumoGranularResponse(BaseModel):
    """Resposta com dados granulares de consumo"""
    classe: str = Field(..., description="Classe de consumo")
    consumo_mwh: float = Field(..., description="Consumo total em MWh")
    quantidade_ucs: int = Field(..., description="Quantidade de unidades consumidoras")
    consumo_medio_por_uc_kwh: float = Field(..., description="Consumo médio por UC em kWh")


class MMGDResponse(BaseModel):
    """Resposta com dados de MMGD"""
    quantidade_instalacoes: int = Field(..., description="Quantidade de instalações")
    potencia_instalada_mw: float = Field(..., description="Potência instalada em MW")
    geracao_estimada_mw: float = Field(..., description="Geração estimada em MW")
    tipo_tecnologia: str = Field(..., description="Tipo de tecnologia (SOLAR_FV, etc)")


class CargaCalculadaResponse(BaseModel):
    """Resposta com carga calculada"""
    classe: str = Field(..., description="Classe de consumo")
    hora: int = Field(..., description="Hora do dia (0-23)")
    carga_base_mw: float = Field(..., description="Carga base em MW")
    carga_com_sazonalidade_mw: float = Field(..., description="Carga com sazonalidade em MW")
    carga_estimada_final_mw: float = Field(..., description="Carga estimada final em MW")
    confiabilidade: float = Field(..., description="Confiabilidade da estimativa (0-1)")


# ========================================================================
# ENDPOINTS
# ========================================================================

@router.get(
    "/perfil/{classe}",
    response_model=PerfilCargaResponse,
    summary="Obter perfil de carga por classe",
    description="Retorna o perfil de carga típico para uma classe de consumo"
)
async def obter_perfil_classe(
    classe: str = Path(..., description="Classe: RESIDENCIAL, COMERCIAL, INDUSTRIAL, RURAL, ILUMINACAO_PUBLICA, SERVICO_PUBLICO"),
    repository = Depends(get_load_calculation_repository),
):
    """
    Obtém o perfil de carga horário para uma classe de consumo.
    
    **Parâmetros:**
    - classe: RESIDENCIAL, COMERCIAL, INDUSTRIAL, RURAL, ILUMINACAO_PUBLICA ou SERVICO_PUBLICO
    
    **Retorna:**
    Perfil com 24 fatores normalizados e dados de pico/mínimo
    """
    try:
        use_case = ObterPerfilClasseUseCase(repository=repository)
        perfil = use_case.executar(classe)
        return LoadCalculationMapper.perfil_to_response(perfil)
    except LoadCalculationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter perfil: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter perfil de carga")


@router.get(
    "/consumo/{classe}",
    response_model=ConsumoGranularResponse,
    summary="Obter consumo granular por classe",
    description="Retorna dados granulares de consumo para uma classe"
)
async def obter_consumo_granular(
    classe: str = Path(..., description="Classe de consumo"),
    repository = Depends(get_load_calculation_repository),
):
    """
    Obtém dados granulares de consumo para uma classe.
    
    **Parâmetros:**
    - classe: Classe de consumo
    
    **Retorna:**
    Consumo total, quantidade de UCs e consumo médio por UC
    """
    try:
        use_case = CalcularConsumiDiarioUseCase(repository=repository)
        consumo = use_case.executar(classe)
        return LoadCalculationMapper.consumo_to_response(consumo)
    except LoadCalculationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter consumo: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter consumo granular")


@router.get(
    "/mmgd/{subsistema}",
    response_model=MMGDResponse,
    summary="Obter MMGD do subsistema",
    description="Retorna dados de microgeração/minigeração distribuída"
)
async def obter_mmgd_subsistema(
    subsistema: str = Path(..., description="Subsistema: SE, S, NE ou N"),
    repository = Depends(get_load_calculation_repository),
):
    """
    Obtém dados de MMGD para um subsistema.
    
    **Parâmetros:**
    - subsistema: SE (Sudeste), S (Sul), NE (Nordeste), N (Norte)
    
    **Retorna:**
    Quantidade de instalações, potência instalada e geração estimada
    """
    try:
        use_case = ObterMMGDSubsistemaUseCase(repository=repository)
        mmgd = use_case.executar(subsistema)
        return LoadCalculationMapper.mmgd_to_response(mmgd)
    except LoadCalculationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter MMGD: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao obter MMGD")


@router.get(
    "/carga/{classe}",
    response_model=Union[CargaCalculadaResponse, List[CargaCalculadaResponse]],
    summary="Calcular carga horária",
    description="Calcula a carga para cada hora do dia de uma classe"
)
async def calcular_carga_horario(
    classe: str = Path(..., description="Classe de consumo"),
    hora: Optional[int] = Query(None, ge=0, le=23, description="Hora específica (0-23) ou None para todas"),
    repository = Depends(get_load_calculation_repository),
):
    """
    Calcula a carga estimada para uma classe de consumo.
    
    **Parâmetros:**
    - classe: Classe de consumo
    - hora: Hora específica (0-23) ou deixar vazio para calcular todas as 24 horas
    
    **Retorna:**
    Carga calculada com calibração e confiabilidade
    """
    try:
        use_case = CalcularCargaHorarioUseCase(repository=repository)
        result = use_case.executar(classe, hora)
        
        if isinstance(result, list):
            return [LoadCalculationMapper.carga_to_response(c) for c in result]
        else:
            return LoadCalculationMapper.carga_to_response(result)
    except LoadCalculationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao calcular carga: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao calcular carga")


@router.post(
    "/salvar-carga",
    response_model=CargaCalculadaResponse,
    summary="Salvar carga calculada",
    description="Salva uma carga calculada para histórico e análise"
)
async def salvar_carga_calculada(
    carga: CargaCalculadaResponse = Body(...),
    repository = Depends(get_load_calculation_repository),
):
    """
    Salva uma carga calculada para histórico.
    
    **Body:**
    Carga calculada a ser salva
    
    **Retorna:**
    Carga salva com confirmação
    """
    try:
        use_case = SalvarCargaCalculadaUseCase(repository=repository)
        carga_obj = CargaCalculada(
            classe=carga.classe,
            hora=carga.hora,
            carga_base_mw=carga.carga_base_mw,
            carga_com_sazonalidade_mw=carga.carga_com_sazonalidade_mw,
            carga_estimada_final_mw=carga.carga_estimada_final_mw,
            confiabilidade=carga.confiabilidade,
        )
        carga_salva = use_case.executar(carga_obj)
        return LoadCalculationMapper.carga_to_response(carga_salva)
    except LoadCalculationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao salvar carga: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao salvar carga")
