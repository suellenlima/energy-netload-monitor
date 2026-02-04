"""
API REST para processamento de telhados/edifícios em imagens de satélite

Refatorado em 3 camadas:
- API: Recebe requisição HTTP, valida entrada, retorna resposta
- Service: Orquestra lógica de negócio, agregação de dados
- Repository: Acessa banco de dados (schema_aneel_bdgd.sql)

Database Schema: ANEEL BDGD
Tabelas utilizadas:
  - telhados_detectados_transformador: Telhados detectados (READ/WRITE)
  - transformadores_aneel: Dados de transformadores (READ)
  - subestacoes_aneel: Dados de subestações (READ)

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Path, Depends

from ..services.roof_service import RoofService as TelhadoService
from ..core import get_engine
from ..schemas.telhado import (
    ListaTelhadosSimples,
    EstatisticasSimples,
    TelhadosTransformadorResponse,
    EstatisticasSubestacao,
    DetalhesSubestacao,
    TelhadoSimples,
)

# Configure logging
logger = logging.getLogger(__name__)

# Criar router
router = APIRouter(
    prefix="/telhados",
    tags=["Telhados"],
    responses={404: {"description": "Não encontrado"}}
)


def get_telhado_service() -> TelhadoService:
    """Dependência para obter serviço de telhados."""
    engine = get_engine()
    return TelhadoService(engine)


# ============================================================================
# ENDPOINT 1: Listar Telhados com Filtros
# ============================================================================

@router.get(
    "/lista",
    response_model=ListaTelhadosSimples,
    summary="Listar telhados processados",
    description="Retorna lista paginada de telhados com filtros opcionais"
)
def listar_telhados(
    id_subestacao: Optional[int] = Query(None, description="Filtrar por subestação"),
    confianca_minima: float = Query(0.0, ge=0, le=1.0, description="Confiança mínima"),
    pagina: int = Query(1, ge=1, description="Número da página"),
    limite: int = Query(100, ge=1, le=10000, description="Itens por página"),
    service: TelhadoService = Depends(get_telhado_service)
) -> ListaTelhadosSimples:
    """
    Lista telhados com suporte a filtros e paginação.
    
    Args:
        id_subestacao: Filtro opcional de subestação
        confianca_minima: Filtro de confiança mínima
        pagina: Página (padrão: 1)
        limite: Itens por página (padrão: 100)
    
    Returns:
        ListaTelhadosSimples com telhados paginados
    """
    try:
        resultado = service.listar_telhados(
            id_subestacao=id_subestacao,
            confianca_minima=confianca_minima,
            pagina=pagina,
            limite=limite
        )
        
        # Converter telhados para response
        telhados_response = []
        for telhado in resultado['telhados']:
            telhados_response.append(TelhadoSimples(
                id_telhado=telhado['id'],
                transformador_id=telhado['transformador_id'],
                subestacao_id=telhado['subestacao_id'],
                latitude=telhado['latitude'],
                longitude=telhado['longitude'],
                area_m2=telhado['area_m2'],
                confianca=telhado['confianca'],
                timestamp_deteccao=telhado['timestamp_deteccao'],
                transformador_codigo=telhado.get('transformador_codigo'),
                subestacao_codigo=telhado.get('subestacao_codigo')
            ))
        
        return ListaTelhadosSimples(
            total_resultados=resultado['total'],
            pagina=resultado['pagina'],
            limite=resultado['limite'],
            total_paginas=resultado['total_paginas'],
            telhados=telhados_response
        )
        
    except ValueError as e:
        logger.error(f"Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao listar telhados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 2: Detalhes de Uma Subestação
# ============================================================================

@router.get(
    "/subestacao/{id_subestacao}",
    response_model=DetalhesSubestacao,
    summary="Obter detalhes de telhados de uma subestação",
    description="Retorna telhados de uma subestação específica"
)
def obter_detalhes_subestacao(
    id_subestacao: int = Path(..., description="ID da subestação"),
    service: TelhadoService = Depends(get_telhado_service)
) -> DetalhesSubestacao:
    """
    Obtém detalhes completos de uma subestação com seus telhados.
    
    Args:
        id_subestacao: ID da subestação
    
    Returns:
        DetalhesSubestacao com telhados
    """
    try:
        resultado = service.obter_detalhes_subestacao(id_subestacao)
        
        # Converter para response
        telhados_response = []
        for telhado in resultado.get('telhados', []):
            telhados_response.append(TelhadoSimples(
                id_telhado=telhado['id'],
                transformador_id=telhado['transformador_id'],
                subestacao_id=telhado['subestacao_id'],
                latitude=telhado['latitude'],
                longitude=telhado['longitude'],
                area_m2=telhado['area_m2'],
                confianca=telhado['confianca'],
                timestamp_deteccao=telhado['timestamp_deteccao'],
                transformador_codigo=telhado.get('transformador_codigo')
            ))
        
        return DetalhesSubestacao(
            subestacao_id=id_subestacao,
            timestamp_processamento=resultado['timestamp_processamento'],
            telhados_detectados=resultado['telhados_detectados'],
            area_total_m2=resultado['area_total_m2'],
            confianca_media=resultado['confianca_media'],
            transformadores_processados=resultado.get('transformadores_processados', 0),
            telhados=telhados_response
        )
        
    except Exception as e:
        logger.error(f"Erro ao obter detalhes subestação: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 3: Estatísticas Agregadas
# ============================================================================

@router.get(
    "/estatisticas",
    response_model=EstatisticasSimples,
    summary="Estatísticas agregadas de segmentação",
    description="Retorna estatísticas consolidadas de todos os telhados"
)
def obter_estatisticas(
    periodo: Optional[str] = Query(None, description="Período (ex: 2025-01)"),
    service: TelhadoService = Depends(get_telhado_service)
) -> EstatisticasSimples:
    """
    Calcula e retorna estatísticas agregadas.
    
    Args:
        periodo: Período no formato YYYY-MM
    
    Returns:
        EstatisticasSimples com estatísticas consolidadas
    """
    try:
        stats = service.obter_estatisticas_gerais(periodo)
        
        return EstatisticasSimples(
            total_subestacoes_processadas=stats.get('total_subestacoes', 0),
            total_telhados_detectados=stats.get('total_telhados', 0),
            media_confianca_deteccao=float(stats.get('confianca_media', 0.0)),
            media_area_telhado_m2=float(stats.get('area_media_m2', 0.0)),
            confianca_minima=float(stats.get('confianca_minima', 0.0)),
            confianca_maxima=float(stats.get('confianca_maxima', 1.0)),
            area_minima_m2=float(stats.get('area_minima_m2', 0.0)),
            area_maxima_m2=float(stats.get('area_maxima_m2', 0.0)),
            primeira_deteccao=stats.get('primeira_deteccao'),
            ultima_deteccao=stats.get('ultima_deteccao')
        )
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 4: Telhados de um Transformador
# ============================================================================

@router.get(
    "/transformador/{id_transformador}/telhados",
    response_model=TelhadosTransformadorResponse,
    summary="Telhados de um transformador específico",
    description="Retorna todos os telhados detectados em um transformador"
)
def obter_telhados_transformador(
    id_transformador: int = Path(..., description="ID do transformador"),
    service: TelhadoService = Depends(get_telhado_service)
) -> TelhadosTransformadorResponse:
    """
    Obtém telhados de um transformador específico.
    
    Args:
        id_transformador: ID do transformador
    
    Returns:
        TelhadosTransformadorResponse com telhados e estatísticas
    """
    try:
        resultado = service.obter_telhados_transformador(id_transformador)
        
        # Converter telhados
        telhados_response = []
        for telhado in resultado.get('telhados', []):
            telhados_response.append(TelhadoSimples(
                id_telhado=telhado['id'],
                transformador_id=telhado['transformador_id'],
                subestacao_id=telhado['subestacao_id'],
                latitude=telhado['latitude'],
                longitude=telhado['longitude'],
                area_m2=telhado['area_m2'],
                confianca=telhado['confianca'],
                timestamp_deteccao=telhado['timestamp_deteccao'],
                transformador_codigo=telhado.get('transformador_codigo')
            ))
        
        return TelhadosTransformadorResponse(
            transformador_id=id_transformador,
            total=resultado['total'],
            area_total_m2=resultado['area_total_m2'],
            confianca_media=resultado['confianca_media'],
            telhados=telhados_response
        )
        
    except Exception as e:
        logger.error(f"Erro ao obter telhados transformador: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 5: Estatísticas de Uma Subestação
# ============================================================================

@router.get(
    "/subestacao/{id_subestacao}/telhados-transformadores",
    response_model=EstatisticasSubestacao,
    summary="Estatísticas agregadas de uma subestação",
    description="Retorna estatísticas consolidadas de telhados por subestação"
)
def obter_estatisticas_subestacao(
    id_subestacao: int = Path(..., description="ID da subestação"),
    service: TelhadoService = Depends(get_telhado_service)
) -> EstatisticasSubestacao:
    """
    Obtém estatísticas agregadas de uma subestação.
    
    Args:
        id_subestacao: ID da subestação
    
    Returns:
        EstatisticasSubestacao com dados consolidados
    """
    try:
        resultado = service.obter_estatisticas_subestacao(id_subestacao)
        
        return EstatisticasSubestacao(
            subestacao_id=id_subestacao,
            transformadores=resultado.get('transformadores', 0),
            total_telhados=resultado.get('total_telhados', 0),
            area_total_m2=resultado.get('area_total_m2', 0.0),
            confianca_media=resultado.get('confianca_media', 0.0)
        )
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas subestação: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 6: Obter Telhado Específico (NOVO)
# ============================================================================

@router.get(
    "/{telhado_id}",
    response_model=TelhadoSimples,
    summary="Obter telhado específico",
    description="Retorna detalhes de um telhado específico"
)
def obter_telhado(
    telhado_id: int = Path(..., description="ID do telhado"),
    service: TelhadoService = Depends(get_telhado_service)
) -> TelhadoSimples:
    """
    Obtém detalhes de um telhado específico.
    
    Args:
        telhado_id: ID do telhado
    
    Returns:
        TelhadoSimples com detalhes do telhado
    """
    try:
        telhado = service.obter_telhado(telhado_id)
        
        if not telhado:
            raise HTTPException(status_code=404, detail=f"Telhado {telhado_id} não encontrado")
        
        return TelhadoSimples(
            id_telhado=telhado['id'],
            transformador_id=telhado['transformador_id'],
            subestacao_id=telhado['subestacao_id'],
            latitude=telhado['latitude'],
            longitude=telhado['longitude'],
            area_m2=telhado['area_m2'],
            confianca=telhado['confianca'],
            timestamp_deteccao=telhado['timestamp_deteccao'],
            transformador_codigo=telhado.get('transformador_codigo'),
            subestacao_codigo=telhado.get('subestacao_codigo')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao obter telhado: {e}")
        raise HTTPException(status_code=500, detail=str(e))


