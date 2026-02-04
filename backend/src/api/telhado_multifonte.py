"""
API REST para Detecção Multi-Fonte de Telhados - REFATORADO

Refatorado em 3 camadas Clean Architecture:
- API Layer: Recebe requisição HTTP, valida entrada, retorna resposta
- Service Layer: Orquestra lógica de detecção com múltiplas fontes
- Repository Layer: Acessa banco de dados (schema_aneel_bdgd.sql)

Reutiliza serviços existentes:
  - TelhadoMultiFonteService: Orquestração de detecção
  - GoogleMapsQuotaService: Gestão de quota
  - TelhadoSegmentationService: Detecção em imagens
  - ImagemSalvamentoService: Armazenamento de imagens
  - ImagemMultiFonteService: Geração de URLs

Database Schema: ANEEL BDGD (infrastructure/database/schema_aneel_bdgd.sql)
Tabelas:
  - telhados_detectados_transformador: Telhados (READ/WRITE)
  - transformadores_aneel: Transformadores (READ)
  - subestacoes_aneel: Subestações (READ)
  - aneel_bdgd_processamento: Log processamento (WRITE)

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import logging
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..services.telhado_multifonte_service import TelhadoMultiFonteService
from ..core import get_engine


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/telhados",
    tags=["Telhados - Multi-Fonte"],
    responses={404: {"description": "Não encontrado"}}
)


# ========================================================================
# MODELOS PYDANTIC
# ========================================================================

class DetectarTelhados_MultiFonteRequest(BaseModel):
    """Request model para detecção multi-fonte de telhados."""
    
    transformador_id: int = Field(
        ...,
        description="ID do transformador"
    )
    subestacao_id: int = Field(
        ...,
        description="ID da subestação"
    )
    confianca_minima: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Score mínimo (0-1)"
    )
    tentar_google_maps_primeiro: bool = Field(
        default=True,
        description="Tentar Google Maps (zoom 19)"
    )
    tentar_cbers4a_fallback: bool = Field(
        default=True,
        description="Fallback CBERS-4A (gratuito)"
    )
    salvar_rois: bool = Field(
        default=False,
        description="Salvar ROIs em disco"
    )


class TelhadoDetectado_Response(BaseModel):
    """Telhado individual detectado."""
    
    latitude: float
    longitude: float
    area_m2: float
    confianca: float
    bbox: Dict[str, float]
    resolucao_cm: float


class DetectarTelhados_MultiFonteResponse(BaseModel):
    """Response model para detecção multi-fonte de telhados."""
    
    transformador_id: int
    subestacao_id: int
    sucesso: bool
    fonte_utilizada: str
    telhados_detectados: int
    telhados: list[TelhadoDetectado_Response]
    area_total_m2: float
    confianca_media: float
    url_imagem_utilizada: str
    timestamp: str
    mensagem: str
    detalhes_tentativas: Dict[str, Any]



# ========================================================================
# DEPENDÊNCIAS
# ========================================================================

def get_deteccao_multifonte_service() -> TelhadoMultiFonteService:
    """Dependência para obter serviço de detecção multi-fonte."""
    engine = get_engine()
    return TelhadoMultiFonteService(engine)


# ========================================================================
# ENDPOINTS
# ========================================================================

@router.post(
    "/detectar-multifonte",
    response_model=DetectarTelhados_MultiFonteResponse,
    summary="Detectar telhados com múltiplas fontes",
    description="""
    Detecta telhados usando múltiplas fontes com estratégia de fallback.
    
    **Fluxo:**
    1. Tenta Google Maps (prioritário, zoom 19, ~1m/pixel)
    2. Se não encontrar, fallback para CBERS-4A (gratuito, 2m/pixel)
    3. Salva telhados na tabela telhados_detectados_transformador
    
    **Custo:**
    - Google Maps: ~$0.007/requisição (quota gerenciada)
    - CBERS-4A: Gratuito (sem quota)
    """
)
async def detectar_telhados_multifonte(
    request: DetectarTelhados_MultiFonteRequest,
    service: TelhadoMultiFonteService = Depends(get_deteccao_multifonte_service)
) -> DetectarTelhados_MultiFonteResponse:
    """
    Detecta telhados usando múltiplas fontes com estratégia de fallback.
    
    Args:
        request: Parâmetros de detecção
        service: Serviço injetado de detecção multi-fonte
    
    Returns:
        Resposta com telhados detectados e fonte utilizada
    
    Raises:
        HTTPException: Se validação ou processamento falhar
    """
    
    try:
        logger.info(
            f"\n{'='*80}\n"
            f"[API] POST /telhados/detectar-multifonte\n"
            f"{'='*80}"
        )
        logger.info(f"Parâmetros:")
        logger.info(f"  - transformador_id: {request.transformador_id}")
        logger.info(f"  - subestacao_id: {request.subestacao_id}")
        logger.info(f"  - confianca_minima: {request.confianca_minima}")
        
        # ================================================================
        # EXECUTAR DETECÇÃO NO SERVICE
        # ================================================================
        
        resultado = service.detectar_telhados_multifonte(
            transformador_id=request.transformador_id,
            subestacao_id=request.subestacao_id,
            confianca_minima=request.confianca_minima,
            tentar_google_maps_primeiro=request.tentar_google_maps_primeiro,
            tentar_cbers4a_fallback=request.tentar_cbers4a_fallback,
            salvar_rois=request.salvar_rois
        )
        
        # ================================================================
        # CONSTRUIR RESPOSTA
        # ================================================================
        
        telhados_convertidos = []
        for telhado in resultado.get('telhados_dados', []):
            telhados_convertidos.append(
                TelhadoDetectado_Response(
                    latitude=telhado.get('latitude', 0),
                    longitude=telhado.get('longitude', 0),
                    area_m2=telhado.get('area_m2', 0),
                    confianca=telhado.get('confianca', 0),
                    bbox=telhado.get('bbox', {'x': 0, 'y': 0, 'w': 0, 'h': 0}),
                    resolucao_cm=telhado.get('resolucao_cm', 100)
                )
            )
        
        # Calcular agregações
        area_total = sum(t.area_m2 for t in telhados_convertidos)
        confianca_media = (
            sum(t.confianca for t in telhados_convertidos) / len(telhados_convertidos)
            if telhados_convertidos else 0
        )
        
        # Construir mensagem
        if resultado['sucesso']:
            mensagem = (
                f"Detectados {resultado['telhados_detectados']} telhados "
                f"com {resultado['fonte_utilizada']} "
                f"(área total: {area_total:.1f} m², confiança média: {confianca_media:.2f})"
            )
        else:
            mensagem = "Nenhum telhado detectado em nenhuma fonte"
        
        response = DetectarTelhados_MultiFonteResponse(
            transformador_id=request.transformador_id,
            subestacao_id=request.subestacao_id,
            sucesso=resultado['sucesso'],
            fonte_utilizada=resultado['fonte_utilizada'] or 'nenhuma',
            telhados_detectados=resultado['telhados_detectados'],
            telhados=telhados_convertidos,
            area_total_m2=area_total,
            confianca_media=confianca_media,
            url_imagem_utilizada=resultado['url_imagem_utilizada'] or '',
            timestamp=resultado['timestamp'] or "",
            mensagem=mensagem,
            detalhes_tentativas=resultado['detalhes_tentativas']
        )
        
        logger.info(f"\n[API] ✓ Resposta enviada: {mensagem}")
        logger.info(f"{'='*80}\n")
        
        return response
    
    except ValueError as e:
        logger.error(f"[API] ✗ Erro de validação: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Erro de validação: {str(e)}"
        )
    
    except Exception as e:
        logger.error(f"[API] ✗ Erro não tratado: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar detecção multi-fonte: {str(e)}"
        )


@router.get("/multifonte/health")
async def health_check() -> Dict[str, str]:
    """Health check para endpoint multi-fonte."""
    return {
        "status": "ok",
        "servico": "telhado_multifonte_refatorado",
        "versao": "2.0.0",
        "arquitetura": "3-layer-clean-architecture",
        "descricao": "Detecção com múltiplas fontes (Google Maps + CBERS-4A)"
    }


__all__ = ['router']
