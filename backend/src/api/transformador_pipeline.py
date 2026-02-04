"""
API Endpoint para processar pipeline completo de transformadores
Detecta telhados e painéis solares usando serviços separados

Refactored em 3 camadas:
- API: Recebe requisição, chama Service, retorna resposta
- Service: Orquestra pipeline, gerencia cache, coordena detecções
- Repository: Acessa banco de dados

Database Schema: ANEEL BDGD (schema_aneel_bdgd.sql)
Tabelas utilizadas:
  - transformadores_aneel: Dados dos transformadores
  - telhados_detectados_transformador: Telhados detectados por IA
  - paineis_solares_detectados: Painéis solares detectados
  - potencia_telhados: Resumo de potência por telhado
  - satelite_requisicoes_google_maps: Tracking de requisições de imagem

Author: Energy Netload Monitor
Date: 2026-02-04 (Updated for ANEEL BDGD schema)
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..core import get_engine
from ..schemas.painel_solar import EstimativaPotenciaResponse, TelhadorComPaineis
from ..services.transformador_pipeline_service import TransformadorPipelineService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/transformador",
    tags=["Transformador - Pipeline Completo"],
    responses={404: {"description": "Não encontrado"}}
)


class ProcessarTransformadorRequest(BaseModel):
    """Requisição para processar telhados e painéis de um transformador"""
    
    transformador_id: int = Field(..., description="ID do transformador a processar")
    confianca_minima_telhados: float = Field(0.5, description="Confiança mínima para detecção de telhados", ge=0.1, le=1.0)
    confianca_minima_paineis: float = Field(0.5, description="Confiança mínima para detecção de painéis", ge=0.1, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "transformador_id": 123,
                "confianca_minima_telhados": 0.5,
                "confianca_minima_paineis": 0.5
            }
        }


class ProcessarTransformadorResponse(BaseModel):
    """Resposta do processamento completo"""
    
    sucesso: bool
    transformador_id: int
    num_imagens_processadas: int
    total_telhados_detectados: int
    total_paineis_detectados: int
    telhados_com_paineis: list = Field(default_factory=list)
    potencia_total: Optional[EstimativaPotenciaResponse] = None
    erros: list = Field(default_factory=list)
    tempo_processamento_s: float
    timestamp: datetime = Field(default_factory=datetime.now)


def get_transformador_pipeline_service() -> TransformadorPipelineService:
    """Dependência para obter serviço de pipeline"""
    engine = get_engine()
    return TransformadorPipelineService(engine)


@router.post(
    "/processar-completo",
    response_model=ProcessarTransformadorResponse,
    summary="Processar telhados e painéis solares (pipeline unificado)",
    description="Detecta telhados e painéis solares em um transformador, reutilizando imagens baixadas"
)
async def processar_transformador_completo(
    request: ProcessarTransformadorRequest,
    service: TransformadorPipelineService = Depends(get_transformador_pipeline_service)
) -> ProcessarTransformadorResponse:
    """
    Endpoint principal para processar um transformador completo
    
    Realiza:
    1. Download de imagens de satélite em grid ao redor do transformador
    2. Detecção de telhados em cada imagem
    3. Detecção de painéis solares em cada telhado
    4. Estimativa de potência por painel
    5. Salvamento de resultados no banco de dados
    
    Returns:
        ProcessarTransformadorResponse com resultados completos
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"🔥 Iniciando processamento do transformador {request.transformador_id}")
        logger.info(f"   - Confiança telhados: {request.confianca_minima_telhados}")
        logger.info(f"   - Confiança painéis: {request.confianca_minima_paineis}")
        
        # Chamar serviço para fazer todo o trabalho
        resultado = service.processar_transformador_completo(
            transformador_id=request.transformador_id,
            confianca_minima_telhados=request.confianca_minima_telhados,
            confianca_minima_paineis=request.confianca_minima_paineis
        )
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Montar resposta
        return ProcessarTransformadorResponse(
            sucesso=True,
            transformador_id=request.transformador_id,
            num_imagens_processadas=resultado.get("num_imagens_processadas", 0),
            total_telhados_detectados=resultado.get("total_telhados_detectados", 0),
            total_paineis_detectados=resultado.get("total_paineis_detectados", 0),
            telhados_com_paineis=resultado.get("telhados_com_paineis", []),
            potencia_total=resultado.get("potencia_total"),
            erros=resultado.get("erros", []),
            tempo_processamento_s=elapsed_time,
            timestamp=datetime.now()
        )
    
    except ValueError as e:
        logger.error(f"❌ Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"❌ Erro durante processamento: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao processar transformador: {str(e)}")
