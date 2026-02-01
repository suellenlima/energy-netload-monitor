"""
Endpoints para detecção de painéis solares

Author: Energy Netload Monitor  
Date: 2026-02-01
"""

import logging
import json
import cv2
import base64
from datetime import datetime
from typing import Optional
import numpy as np

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from ..core import get_engine

from ..schemas.painel_solar import (
    DetectarPainelSolarRequest,
    DetectarPainelSolarEmRoiRequest,
    DeteccaoPainelSolarResponse,
    PainelSolarResponse,
    EstimativaPotenciaResponse,
    TelhadorComPaineis
)
from ..services.painel_solar_detection_service import (
    PainelSolarDetectionService
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/paineis-solares",
    tags=["Painéis Solares - Detecção"],
    responses={404: {"description": "Não encontrado"}}
)

# Cache global do serviço
_servico_paineis: Optional[PainelSolarDetectionService] = None

def _obter_servico_paineis() -> PainelSolarDetectionService:
    """Obtém ou cria instância do serviço de painéis solares"""
    global _servico_paineis
    
    if _servico_paineis is None:
        logger.info("🔧 Inicializando serviço de detecção de painéis solares...")
        _servico_paineis = PainelSolarDetectionService()
    
    return _servico_paineis


@router.post(
    "/detectar",
    response_model=DeteccaoPainelSolarResponse,
    summary="Detectar painéis solares em um telhado ou transformador",
    description="Detecta painéis solares em um telhado OU em TODOS os telhados de um transformador"
)
async def detectar_paineis_solares(
    request: DetectarPainelSolarRequest
) -> DeteccaoPainelSolarResponse:
    """
    Detecta painéis solares em um telhado específico OU em todos os telhados de um transformador
    
    Args:
        request: Requisição com telhado_id (específico) OU transformador_id (todos)
        
    Returns:
        Resultado com painéis detectados, potência estimada e produção anual
    """
    try:
        import time
        tempo_inicio = time.time()
        
        # Validação: deve informar um dos dois
        if not request.telhado_id and not request.transformador_id:
            raise HTTPException(
                status_code=400, 
                detail="Informe 'telhado_id' (específico) OU 'transformador_id' (todos os telhados)"
            )
        
        if request.telhado_id and request.transformador_id:
            raise HTTPException(
                status_code=400, 
                detail="Informe apenas 'telhado_id' OU 'transformador_id', não ambos"
            )
        
        # Modo 1: Processamento de um telhado específico
        if request.telhado_id:
            if not request.url_imagem or not request.bbox_json:
                raise HTTPException(
                    status_code=400, 
                    detail="Para telhado_id, é necessário informar 'url_imagem' e 'bbox_json'"
                )
            
            logger.info(f"[PAINEIS] 🌞 Iniciando detecção em 1 telhado (ID: {request.telhado_id})")
            
            servico = _obter_servico_paineis()
            bbox = json.loads(request.bbox_json)
            
            resultado = servico.processar_telhado(
                url_imagem=request.url_imagem,
                bbox=bbox,
                confianca_minima=request.confianca_minima,
                potencia_por_m2=150.0
            )
            
            # Construir response
            paineis_response = []
            if resultado['paineis']:
                paineis_response = [
                    PainelSolarResponse(
                        id_painel=p['id_painel'],
                        bbox=p['bbox'],
                        centroide=p['centroide'],
                        area_pixeis=p['area_pixeis'],
                        area_m2=p['area_m2'],
                        confianca=p['confianca'],
                        tipo_painel=p['tipo_painel'],
                        timestamp_deteccao=datetime.fromisoformat(p['timestamp_deteccao'])
                    )
                    for p in resultado['paineis']
                ]
            
            potencia_response = None
            if resultado['potencia']:
                potencia_response = EstimativaPotenciaResponse(
                    **resultado['potencia']
                )
            
            response = DeteccaoPainelSolarResponse(
                sucesso=resultado['sucesso'],
                telhado_id=request.telhado_id,
                num_telhados_processados=1,
                paineis=paineis_response,
                potencia=potencia_response,
                erros=resultado['erros'],
                tempo_processamento_s=resultado['tempo_processamento_s']
            )
            
            logger.info(f"[PAINEIS] ✅ Detecção concluída em {time.time() - tempo_inicio:.2f}s")
            if paineis_response:
                logger.info(f"[PAINEIS] 📊 Detectados {len(paineis_response)} painéis, potência: {potencia_response.potencia_instalada_kw:.2f}kW")
            
            # Salvar no banco de dados
            if resultado['sucesso'] and paineis_response:
                try:
                    engine = get_engine()
                    logger.info(f"[PAINEIS] 💾 Salvando painéis no banco...")
                    
                    with engine.begin() as conn:
                        # Limpar painéis anteriores deste telhado
                        conn.execute(text("""
                            DELETE FROM paineis_solares_detectados 
                            WHERE telhado_id = :telhado_id
                        """), {'telhado_id': request.telhado_id})
                        
                        # Buscar dados do telhado
                        result = conn.execute(text("""
                            SELECT transformador_id, subestacao_id, latitude, longitude
                            FROM telhados_detectados_transformador
                            WHERE id = :telhado_id
                        """), {'telhado_id': request.telhado_id})
                        
                        telhado_data = result.fetchone()
                        if not telhado_data:
                            logger.warning(f"[PAINEIS] ⚠️ Telhado {request.telhado_id} não encontrado no banco")
                        else:
                            transformador_id, subestacao_id, _, _ = telhado_data
                            
                            # Salvar cada painel
                            paineis_salvos = 0
                            for painel in paineis_response:
                                potencia_w = painel.area_m2 * 150.0
                                
                                conn.execute(text("""
                                    INSERT INTO paineis_solares_detectados
                                    (telhado_id, transformador_id, subestacao_id, 
                                     bbox_json, centroide_json, area_pixeis, area_m2,
                                     confianca, tipo_painel, potencia_w, timestamp_deteccao)
                                    VALUES (:telhado_id, :trans_id, :sub_id,
                                            :bbox, :centroide, :area_px, :area_m2,
                                            :conf, :tipo, :potencia, :timestamp)
                                """), {
                                    'telhado_id': request.telhado_id,
                                    'trans_id': transformador_id,
                                    'sub_id': subestacao_id,
                                    'bbox': json.dumps(painel.bbox),
                                    'centroide': json.dumps(painel.centroide),
                                    'area_px': painel.area_pixeis,
                                    'area_m2': painel.area_m2,
                                    'conf': painel.confianca,
                                    'tipo': painel.tipo_painel,
                                    'potencia': potencia_w,
                                    'timestamp': painel.timestamp_deteccao
                                })
                                paineis_salvos += 1
                            
                            # Atualizar tabela de resumo de potência
                            conn.execute(text("""
                                DELETE FROM potencia_telhados 
                                WHERE telhado_id = :telhado_id
                            """), {'telhado_id': request.telhado_id})
                            
                            conn.execute(text("""
                                INSERT INTO potencia_telhados
                                (telhado_id, transformador_id, num_paineis, area_total_m2,
                                 potencia_instalada_kw, producao_diaria_kwh, producao_anual_kwh,
                                 economia_anual_brl, potencia_por_m2)
                                VALUES (:telhado_id, :trans_id, :num, :area,
                                        :pot_kw, :prod_dia, :prod_ano, :economia, :pot_m2)
                            """), {
                                'telhado_id': request.telhado_id,
                                'trans_id': transformador_id,
                                'num': len(paineis_response),
                                'area': potencia_response.total_area_m2,
                                'pot_kw': potencia_response.potencia_instalada_kw,
                                'prod_dia': potencia_response.producao_diaria_kwh,
                                'prod_ano': potencia_response.producao_anual_kwh,
                                'economia': potencia_response.economia_anual_brl,
                                'pot_m2': 150.0
                            })
                    
                    logger.info(f"[PAINEIS] ✅ {paineis_salvos} painéis salvos no banco")
                
                except Exception as e:
                    logger.error(f"[PAINEIS] ❌ Erro ao salvar painéis no banco: {e}")
            
            return response
        
        # Modo 2: Processamento de TODOS os telhados de um transformador
        else:
            logger.info(f"[PAINEIS] 🌞 Iniciando detecção em TODOS os telhados do transformador {request.transformador_id}")
            
            engine = get_engine()
            servico = _obter_servico_paineis()
            
            # Buscar todos os telhados do transformador
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, url_imagem_origem, bbox_json, transformador_id, subestacao_id
                    FROM telhados_detectados_transformador
                    WHERE transformador_id = :trans_id
                    ORDER BY id
                """), {'trans_id': request.transformador_id})
                
                telhados = result.fetchall()
            
            if not telhados:
                logger.warning(f"[PAINEIS] ⚠️ Nenhum telhado encontrado para transformador {request.transformador_id}")
                return DeteccaoPainelSolarResponse(
                    sucesso=False,
                    transformador_id=request.transformador_id,
                    num_telhados_processados=0,
                    paineis=[],
                    potencia=None,
                    erros=[f"Nenhum telhado encontrado para transformador {request.transformador_id}"],
                    tempo_processamento_s=time.time() - tempo_inicio
                )
            
            logger.info(f"[PAINEIS] 📋 Encontrados {len(telhados)} telhados para processamento")
            
            # Processar cada telhado
            todos_paineis = []
            potencia_total = None
            erros = []
            telhados_com_erro = 0
            potencia_por_telhado_list = []
            telhados_com_paineis_list = []  # Nova lista com todos os detalhes
            
            for idx, (telhado_id, url, bbox_json, trans_id, sub_id) in enumerate(telhados, 1):
                try:
                    logger.info(f"[PAINEIS] ⏳ Processando telhado {idx}/{len(telhados)} (ID: {telhado_id})")
                    
                    # Se bbox_json já é dict, usar direto. Senão, fazer parse JSON
                    if isinstance(bbox_json, dict):
                        bbox = bbox_json
                    else:
                        bbox = json.loads(bbox_json)
                    
                    # Processar telhado
                    resultado = servico.processar_telhado(
                        url_imagem=url,
                        bbox=bbox,
                        confianca_minima=request.confianca_minima,
                        potencia_por_m2=150.0
                    )
                    
                    if resultado['sucesso'] and resultado['paineis']:
                        # Construir respostas de painéis
                        paineis_response = [
                            PainelSolarResponse(
                                id_painel=p['id_painel'],
                                bbox=p['bbox'],
                                centroide=p['centroide'],
                                area_pixeis=p['area_pixeis'],
                                area_m2=p['area_m2'],
                                confianca=p['confianca'],
                                tipo_painel=p['tipo_painel'],
                                timestamp_deteccao=datetime.fromisoformat(p['timestamp_deteccao'])
                            )
                            for p in resultado['paineis']
                        ]
                        
                        todos_paineis.extend(paineis_response)
                        
                        potencia_response = EstimativaPotenciaResponse(**resultado['potencia']) if resultado['potencia'] else None
                        
                        if potencia_total is None:
                            potencia_total = resultado['potencia']
                        else:
                            # Acumular potência
                            potencia_total['total_area_m2'] += resultado['potencia']['total_area_m2']
                            potencia_total['num_paineis'] += resultado['potencia']['num_paineis']
                            potencia_total['potencia_instalada_kw'] += resultado['potencia']['potencia_instalada_kw']
                            potencia_total['producao_diaria_kwh'] += resultado['potencia']['producao_diaria_kwh']
                            potencia_total['producao_anual_kwh'] += resultado['potencia']['producao_anual_kwh']
                            potencia_total['economia_anual_brl'] += resultado['potencia']['economia_anual_brl']
                        
                        # Guardar potência por telhado
                        potencia_por_telhado_list.append({
                            'telhado_id': telhado_id,
                            'num_paineis': len(paineis_response),
                            'area_m2': potencia_response.total_area_m2 if potencia_response else 0,
                            'potencia_kw': potencia_response.potencia_instalada_kw if potencia_response else 0,
                            'producao_anual_kwh': potencia_response.producao_anual_kwh if potencia_response else 0
                        })
                        
                        # Guardar telhado com painéis detalhados
                        telhados_com_paineis_list.append(
                            TelhadorComPaineis(
                                telhado_id=telhado_id,
                                num_paineis=len(paineis_response),
                                area_total_m2=potencia_response.total_area_m2 if potencia_response else 0,
                                potencia_instalada_kw=potencia_response.potencia_instalada_kw if potencia_response else 0,
                                producao_anual_kwh=potencia_response.producao_anual_kwh if potencia_response else 0,
                                economia_anual_brl=potencia_response.economia_anual_brl if potencia_response else 0,
                                paineis=paineis_response
                            )
                        )
                        
                        # Salvar no banco de dados
                        try:
                            with engine.begin() as conn:
                                # Limpar painéis anteriores
                                conn.execute(text("""
                                    DELETE FROM paineis_solares_detectados 
                                    WHERE telhado_id = :telhado_id
                                """), {'telhado_id': telhado_id})
                                
                                # Salvar cada painel
                                for painel in paineis_response:
                                    potencia_w = painel.area_m2 * 150.0
                                    
                                    conn.execute(text("""
                                        INSERT INTO paineis_solares_detectados
                                        (telhado_id, transformador_id, subestacao_id, 
                                         bbox_json, centroide_json, area_pixeis, area_m2,
                                         confianca, tipo_painel, potencia_w, timestamp_deteccao)
                                        VALUES (:telhado_id, :trans_id, :sub_id,
                                                :bbox, :centroide, :area_px, :area_m2,
                                                :conf, :tipo, :potencia, :timestamp)
                                    """), {
                                        'telhado_id': telhado_id,
                                        'trans_id': trans_id,
                                        'sub_id': sub_id,
                                        'bbox': json.dumps(painel.bbox),
                                        'centroide': json.dumps(painel.centroide),
                                        'area_px': painel.area_pixeis,
                                        'area_m2': painel.area_m2,
                                        'conf': painel.confianca,
                                        'tipo': painel.tipo_painel,
                                        'potencia': potencia_w,
                                        'timestamp': painel.timestamp_deteccao
                                    })
                                
                                # Atualizar tabela de resumo de potência
                                conn.execute(text("""
                                    DELETE FROM potencia_telhados 
                                    WHERE telhado_id = :telhado_id
                                """), {'telhado_id': telhado_id})
                                
                                conn.execute(text("""
                                    INSERT INTO potencia_telhados
                                    (telhado_id, transformador_id, num_paineis, area_total_m2,
                                     potencia_instalada_kw, producao_diaria_kwh, producao_anual_kwh,
                                     economia_anual_brl, potencia_por_m2)
                                    VALUES (:telhado_id, :trans_id, :num, :area,
                                            :pot_kw, :prod_dia, :prod_ano, :economia, :pot_m2)
                                """), {
                                    'telhado_id': telhado_id,
                                    'trans_id': trans_id,
                                    'num': len(paineis_response),
                                    'area': potencia_response.total_area_m2 if potencia_response else 0,
                                    'pot_kw': potencia_response.potencia_instalada_kw if potencia_response else 0,
                                    'prod_dia': potencia_response.producao_diaria_kwh if potencia_response else 0,
                                    'prod_ano': potencia_response.producao_anual_kwh if potencia_response else 0,
                                    'economia': potencia_response.economia_anual_brl if potencia_response else 0,
                                    'pot_m2': 150.0
                                })
                        
                        except Exception as e:
                            logger.error(f"[PAINEIS] ❌ Erro ao salvar telhado {telhado_id}: {e}")
                        
                        logger.info(f"[PAINEIS] ✅ Telhado {telhado_id}: {len(paineis_response)} painéis detectados")
                    
                    else:
                        logger.info(f"[PAINEIS] ⚪ Telhado {telhado_id}: nenhum painel detectado")
                
                except Exception as e:
                    logger.error(f"[PAINEIS] ❌ Erro ao processar telhado {telhado_id}: {e}")
                    erros.append(f"Telhado {telhado_id}: {str(e)}")
                    telhados_com_erro += 1
            
            # Construir resposta final
            potencia_response = None
            if potencia_total:
                potencia_response = EstimativaPotenciaResponse(**potencia_total)
            
            response = DeteccaoPainelSolarResponse(
                sucesso=len(erros) == 0,
                transformador_id=request.transformador_id,
                num_telhados_processados=len(telhados),
                paineis=todos_paineis,
                potencia=potencia_response,
                telhados_com_paineis=telhados_com_paineis_list,
                potencia_por_telhado=potencia_por_telhado_list,
                erros=erros,
                tempo_processamento_s=time.time() - tempo_inicio
            )
            
            logger.info(f"[PAINEIS] ✅ Processamento de transformador concluído em {time.time() - tempo_inicio:.2f}s")
            logger.info(f"[PAINEIS] 📊 Total: {len(todos_paineis)} painéis em {len(telhados)} telhados")
            if potencia_response:
                logger.info(f"[PAINEIS] 💡 Potência total: {potencia_response.potencia_instalada_kw:.2f}kW, Produção anual: {potencia_response.producao_anual_kwh:.0f}kWh")
            
            return response
    
    except Exception as e:
        logger.error(f"[PAINEIS] ❌ Erro ao detectar painéis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/detectar-roi",
    response_model=DeteccaoPainelSolarResponse,
    summary="Detectar painéis solares em uma ROI (base64)",
    description="Detecta painéis solares em uma imagem ROI fornecida em base64"
)
async def detectar_paineis_solares_roi(
    request: DetectarPainelSolarEmRoiRequest
) -> DeteccaoPainelSolarResponse:
    """
    Detecta painéis solares diretamente em uma ROI fornecida em base64
    
    Args:
        request: Requisição com imagem em base64
        
    Returns:
        Resultado com painéis detectados e potência estimada
    """
    try:
        import time
        tempo_inicio = time.time()
        
        logger.info(f"[PAINEIS-ROI] 🌞 Iniciando detecção em ROI base64")
        
        # Decodificar base64
        img_data = base64.b64decode(request.roi_base64)
        roi = cv2.imdecode(np.frombuffer(img_data, np.uint8), cv2.IMREAD_COLOR)
        
        if roi is None:
            raise HTTPException(status_code=400, detail="Imagem base64 inválida")
        
        logger.info(f"[PAINEIS-ROI] 🖼️ Imagem decodificada: {roi.shape}")
        
        # Obter serviço
        servico = _obter_servico_paineis()
        
        # Detectar painéis
        logger.info(f"[PAINEIS-ROI] 🔍 Detectando painéis...")
        paineis = servico.detectar_paineis(roi, request.confianca_minima)
        
        # Estimar potência
        potencia = servico.estimar_potencia(paineis, 150.0)
        
        # Construir response
        paineis_response = [
            PainelSolarResponse(
                id_painel=p.id_painel,
                bbox=p.bbox,
                centroide=p.centroide,
                area_pixeis=p.area_pixeis,
                area_m2=p.area_m2,
                confianca=p.confianca,
                tipo_painel=p.tipo_painel,
                timestamp_deteccao=p.timestamp_deteccao
            )
            for p in paineis
        ]
        
        response = DeteccaoPainelSolarResponse(
            sucesso=True,
            paineis=paineis_response,
            potencia=EstimativaPotenciaResponse(**potencia.to_dict()),
            erros=[],
            tempo_processamento_s=time.time() - tempo_inicio
        )
        
        logger.info(f"[PAINEIS-ROI] ✅ Detecção concluída em {time.time() - tempo_inicio:.2f}s")
        
        return response
    
    except Exception as e:
        logger.error(f"[PAINEIS-ROI] ❌ Erro ao detectar painéis em ROI: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
