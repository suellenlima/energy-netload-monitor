#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Endpoint para detectar telhados usando múltiplas fontes de imagens
Prioridade: Google Maps → CBERS-4A
"""

import logging
import time
import os
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy import text
from ..core.database import get_engine
from ..services.google_maps_quota_service import GoogleMapsQuotaService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/telhados",
    tags=["Telhados - Multi-Fonte"],
    responses={404: {"description": "Não encontrado"}}
)

class DetectarTelhados_MultiFonteRequest(BaseModel):
    """Request para detectar telhados com múltiplas fontes"""
    transformador_id: int
    subestacao_id: int
    confianca_minima: float = 0.5
    tentar_google_maps_primeiro: bool = True
    tentar_cbers4a_fallback: bool = True
    salvar_rois: bool = False

class TelhadoDetectado_Response(BaseModel):
    """Resposta com telhado detectado"""
    id_telhado: str
    bbox: Dict[str, float]
    area_m2: float
    confianca: float
    coordenada_geografica: Optional[Dict[str, float]] = None

class DetectarTelhados_MultiFonteResponse(BaseModel):
    """Response para detecção multi-fonte"""
    transformador_id: int
    subestacao_id: int
    sucesso: bool
    fonte_utilizada: str  # 'google_maps' ou 'cbers4a'
    telhados_detectados: int
    telhados: List[TelhadoDetectado_Response]
    area_total_m2: float
    confianca_media: float
    url_imagem_utilizada: str
    timestamp: datetime
    mensagem: str
    detalhes_tentativas: Dict[str, Any]

@router.post(
    "/detectar-multifonte",
    response_model=DetectarTelhados_MultiFonteResponse,
    summary="Detectar telhados com múltiplas fontes (Google Maps → CBERS-4A)",
    description="""
    Detecta telhados com prioridade em Google Maps (melhor para transformadores específicos)
    e fallback para CBERS-4A se necessário.
    
    Fluxo:
    1. Recupera coordenadas do transformador
    2. Tenta Google Maps (zoom 19) com área poligonal
    3. Se não encontrar, tenta CBERS-4A (fallback)
    4. Retorna resultado com metadata da fonte utilizada
    """,
    status_code=200
)
async def detectar_telhados_multifonte(
    request: DetectarTelhados_MultiFonteRequest
) -> DetectarTelhados_MultiFonteResponse:
    """
    Detecta telhados usando múltiplas fontes com prioridade
    
    Args:
        request: Requisição com transformador_id e opções
        
    Returns:
        Resposta com telhados detectados e fonte utilizada
    """
    
    try:
        from ..core import get_engine
        from ..services.imagem_multifonte_service import ImagemMultiFonteService
        from ..services.telhado_segmentation_service import TelhadoSegmentationService
        from ..services.google_maps_quota_service import GoogleMapsQuotaService
        from ..services.imagem_salvamento_service import ImagemSalvamentoService
        from sqlalchemy import text
        import os
        import time
        
        engine = get_engine()
        servico_salvamento = ImagemSalvamentoService(engine)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"[MULTI-FONTE] Detectando telhados para transformador {request.transformador_id}")
        logger.info(f"{'='*80}")
        
        detalhes_tentativas = {}
        imagens_salvas = {}
        
        # 1. Recuperar dados do transformador
        logger.info("[1/3] Recuperando dados do transformador...")
        with engine.connect() as conn:
            query = text("""
                SELECT id, latitude, longitude, nome
                FROM transformadores
                WHERE id = :id
            """)
            result = conn.execute(query, {"id": request.transformador_id})
            trafo = result.fetchone()
            
            if not trafo:
                raise HTTPException(status_code=404, detail=f"Transformador {request.transformador_id} não encontrado")
            
            trafo_id, lat, lon, trafo_nome = trafo
            logger.info(f"✓ Transformador encontrado: {trafo_nome} ({lat}, {lon})")
            
            # Recuperar área poligonal (se existir)
            vertices = []
            try:
                query_poly = text("""
                    SELECT ST_X(geom) as lon, ST_Y(geom) as lat
                    FROM (
                        SELECT (ST_DumpPoints(geom)).geom
                        FROM area_poligonal_transformador
                        WHERE transformador_id = :id
                        LIMIT 1
                    ) as points
                """)
                result_poly = conn.execute(query_poly, {"id": request.transformador_id})
                
                for row in result_poly:
                    vertices.append((row[1], row[0]))  # (lat, lon)
                
                if vertices:
                    logger.info(f"✓ Área poligonal encontrada: {len(vertices)} vértices")
            except Exception as e:
                logger.warning(f"⚠️  Tabela area_poligonal_transformador não existe ou erro ao consultar: {e}")
                vertices = []
        
        # 2. Gerar URLs de todas as fontes
        logger.info("[2/3] Gerando URLs de imagens...")
        google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        service_multifonte = ImagemMultiFonteService(google_maps_api_key)
        
        urls_por_fonte = service_multifonte.gerar_urls_todas_fontes(
            request.transformador_id, lat, lon, vertices
        )
        
        logger.info(f"✓ URLs geradas: {list(urls_por_fonte.keys())}")
        
        # 3. Tentar detecção com cada fonte
        logger.info("[3/3] Tentando detectar telhados...")
        
        resultado_deteccao = None
        fonte_utilizada = None
        url_utilizada = None
        
        # Tentar Google Maps primeiro
        if request.tentar_google_maps_primeiro and 'google_maps' in urls_por_fonte:
            logger.info("\n🔍 Tentativa 1: Google Maps...")
            try:
                url_google = urls_por_fonte['google_maps']['url']
                
                logger.info(f"  URL: {url_google[:100]}...")
                logger.info(f"  Zoom: 19, Resolução: ~1m/pixel")
                
                # Inicializar serviço de quota
                quota_service = GoogleMapsQuotaService(engine)
                tempo_inicio = time.time()
                
                # Usar o serviço de segmentação existente para detectar
                servico_segmentacao = TelhadoSegmentationService(use_gpu=True)
                resultado_google = servico_segmentacao.processar_telhados_lote(
                    url_imagem=url_google,
                    id_subestacao=f"trafo_{request.transformador_id}",
                    id_imagem_satelite=f"google_maps_{request.transformador_id}",
                    resolucao_m_por_pixel=1.0,  # Google Maps: ~1m/pixel
                    confianca_minima=request.confianca_minima,
                    diretorio_saida=None
                )
                
                # Registrar requisição na quota
                tempo_resposta_ms = int((time.time() - tempo_inicio) * 1000)
                status_requisicao = 'sucesso' if resultado_google.telhados_detectados > 0 else 'nenhum_resultado'
                
                resultado_quota = quota_service.registrar_requisicao(
                    transformador_id=request.transformador_id,
                    subestacao_id=request.subestacao_id,
                    tipo_requisicao='satellite',
                    zoom=19,
                    largura=640,
                    altura=640,
                    status=status_requisicao,
                    url=url_google,
                    tempo_ms=tempo_resposta_ms,
                    codigo_resposta=200
                )
                
                if resultado_quota['sucesso']:
                    logger.info(f"  Requisição registrada: Custo=${resultado_quota['custo_usd']:.4f}")
                
                # Salvar imagem Google Maps no banco
                try:
                    resultado_salvamento = servico_salvamento.salvar_imagem_google_maps(
                        subestacao_id=request.subestacao_id,
                        transformador_id=request.transformador_id,
                        url=url_google,
                        latitude=lat,
                        longitude=lon,
                        zoom=19,
                        largura=640,
                        altura=640,
                        vertices_poligono=vertices if vertices else None,
                        resolucao_m=1.0
                    )
                    
                    if resultado_salvamento['sucesso']:
                        imagens_salvas['google_maps'] = resultado_salvamento
                        logger.info(f"  📦 Google Maps salva com ID {resultado_salvamento['imagem_id']}")
                    else:
                        logger.warning(f"  ⚠️  Erro ao salvar Google Maps: {resultado_salvamento.get('erro')}")
                except Exception as e:
                    logger.warning(f"  ⚠️  Erro ao salvar Google Maps: {e}")
                
                if resultado_google.telhados_detectados > 0:
                    logger.info(f"  ✓ {resultado_google.telhados_detectados} telhados detectados!")
                    resultado_deteccao = resultado_google
                    fonte_utilizada = 'google_maps'
                    url_utilizada = url_google
                    
                    detalhes_tentativas['google_maps'] = {
                        'tentado': True,
                        'status': 'sucesso',
                        'telhados_detectados': resultado_google.telhados_detectados,
                        'url': url_google
                    }
                else:
                    logger.info(f"  ⚠️  Nenhum telhado detectado em Google Maps")
                    detalhes_tentativas['google_maps'] = {
                        'tentado': True,
                        'status': 'nenhum_telhado_detectado',
                        'url': url_google
                    }
                
            except Exception as e:
                logger.warning(f"  ✗ Erro em Google Maps: {e}")
                detalhes_tentativas['google_maps'] = {
                    'tentado': True,
                    'status': 'erro',
                    'erro': str(e),
                    'url': url_google if 'url_google' in locals() else None
                }
        
        # Fallback para CBERS-4A
        if not fonte_utilizada and request.tentar_cbers4a_fallback:
            logger.info("\n🔍 Tentativa 2: CBERS-4A (fallback)...")
            try:
                # Recuperar imagem CBERS-4A registrada
                with engine.connect() as conn:
                    query_cbers = text("""
                        SELECT si.id, sb.url
                        FROM satelite_imagens si
                        JOIN satelite_bandas sb ON si.id = sb.imagem_id
                        WHERE si.subestacao_id = :sub_id
                        AND sb.nome_banda = 'red'
                        ORDER BY si.data_aquisicao DESC
                        LIMIT 1
                    """)
                    result_cbers = conn.execute(query_cbers, {"sub_id": request.subestacao_id})
                    imagem_cbers = result_cbers.fetchone()
                    
                    if imagem_cbers:
                        imagem_id, url_cbers = imagem_cbers
                        logger.info(f"  URL: {url_cbers[:100]}...")
                        logger.info(f"  Resolução: 2m/pixel")
                        
                        # Recuperar todas as 5 bandas para armazenar
                        query_bandas = text("""
                            SELECT nome_banda, url
                            FROM satelite_bandas
                            WHERE imagem_id = :img_id
                        """)
                        result_bandas = conn.execute(query_bandas, {"img_id": imagem_id})
                        urls_bandas = {}
                        for banda_nome, banda_url in result_bandas:
                            urls_bandas[banda_nome.lower()] = banda_url
                        
                        # CBERS-4A não custa quota (é gratuito), então não registramos na quota
                        logger.info(f"  ℹ️  CBERS-4A não consome quota (é gratuito)")
                        
                        # Salvar referência de CBERS-4A no banco
                        try:
                            resultado_salvamento_cbers = servico_salvamento.salvar_imagem_cbers4a(
                                subestacao_id=request.subestacao_id,
                                transformador_id=request.transformador_id,
                                imagem_id_cbers=imagem_id,
                                urls_bandas=urls_bandas,
                                latitude=lat,
                                longitude=lon,
                                vertices_poligono=vertices if vertices else None,
                                resolucao_m=2.0
                            )
                            
                            if resultado_salvamento_cbers['sucesso']:
                                imagens_salvas['cbers4a'] = resultado_salvamento_cbers
                                logger.info(f"  📦 CBERS-4A (5 bandas) salva com ID {resultado_salvamento_cbers['imagem_id']}")
                            else:
                                logger.warning(f"  ⚠️  Erro ao salvar CBERS-4A: {resultado_salvamento_cbers.get('erro')}")
                        except Exception as e:
                            logger.warning(f"  ⚠️  Erro ao salvar CBERS-4A: {e}")
                        
                        fonte_utilizada = 'cbers4a'
                        url_utilizada = url_cbers
                        
                        detalhes_tentativas['cbers4a'] = {
                            'tentado': True,
                            'status': 'url_encontrada',
                            'imagem_id': imagem_id,
                            'url': url_cbers,
                            'custo_usd': 0.0  # CBERS-4A é gratuito
                        }
                    else:
                        logger.warning("  ✗ Nenhuma imagem CBERS-4A encontrada")
                        detalhes_tentativas['cbers4a'] = {
                            'tentado': True,
                            'status': 'nao_encontrada'
                        }
            
            except Exception as e:
                logger.error(f"  ✗ Erro em CBERS-4A: {e}")
                detalhes_tentativas['cbers4a'] = {
                    'tentado': True,
                    'status': 'erro',
                    'erro': str(e)
                }
        
        # Preparar dados de telhados detectados
        telhados_resposta = []
        if resultado_deteccao:
            for telhado in resultado_deteccao.telhados:
                telhados_resposta.append(
                    TelhadoDetectado_Response(
                        id_telhado=telhado.id_telhado,
                        bbox={
                            'x': telhado.bbox.get('x', 0),
                            'y': telhado.bbox.get('y', 0),
                            'w': telhado.bbox.get('w', 0),
                            'h': telhado.bbox.get('h', 0)
                        },
                        area_m2=telhado.area_m2,
                        confianca=telhado.confianca,
                        coordenada_geografica={'latitude': telhado.lat, 'longitude': telhado.lon} if (telhado.lat != 0 or telhado.lon != 0) else None
                    )
                )
        
        area_total = sum(t.area_m2 for t in telhados_resposta) if telhados_resposta else 0.0
        confianca_media = sum(t.confianca for t in telhados_resposta) / len(telhados_resposta) if telhados_resposta else 0.0
        
        # Retornar resposta com telhados detectados
        return DetectarTelhados_MultiFonteResponse(
            transformador_id=request.transformador_id,
            subestacao_id=request.subestacao_id,
            sucesso=resultado_deteccao is not None and len(telhados_resposta) > 0,
            fonte_utilizada=fonte_utilizada or 'nenhuma',
            telhados_detectados=len(telhados_resposta),
            telhados=telhados_resposta,
            area_total_m2=area_total,
            confianca_media=confianca_media,
            url_imagem_utilizada=url_utilizada or '',
            timestamp=datetime.now(),
            mensagem=f"Detectados {len(telhados_resposta)} telhados com {fonte_utilizada or 'nenhuma'}" if telhados_resposta else f"Nenhum telhado detectado",
            detalhes_tentativas={
                **detalhes_tentativas,
                'imagens_salvas': imagens_salvas,
                'resumo_imagens': {
                    'google_maps_salva': 'google_maps' in imagens_salvas,
                    'cbers4a_salva': 'cbers4a' in imagens_salvas,
                    'total_imagens_salvas': len(imagens_salvas)
                }
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro crítico na detecção multi-fonte: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quota-mes")
async def obter_quota_mes():
    """Obtém informações de quota de Google Maps do mês atual"""
    try:
        engine = get_engine()
        quota_service = GoogleMapsQuotaService(engine)
        
        resultado = quota_service.obter_quota_mes()
        
        return {
            "sucesso": True,
            "quota": resultado
        }
    
    except Exception as e:
        logger.error(f"Erro ao obter quota do mês: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/requisicoes-transformador/{transformador_id}")
async def obter_requisicoes_transformador(transformador_id: int):
    """Obtém histórico de requisições de um transformador"""
    try:
        engine = get_engine()
        quota_service = GoogleMapsQuotaService(engine)
        
        resultado = quota_service.obter_requisicoes_transformador(transformador_id)
        
        return {
            "sucesso": True,
            "transformador_id": transformador_id,
            "requisicoes": resultado
        }
    
    except Exception as e:
        logger.error(f"Erro ao obter requisições: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Exportar router
__all__ = ['router']
