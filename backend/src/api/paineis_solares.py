"""Painéis Solares API endpoints.

Endpoints para detecção de painéis solares em telhados usando YOLO.
"""

import os
import time
import logging
from typing import List, Dict
from datetime import datetime
import json

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
import requests
import numpy as np
import cv2
from PIL import Image
from io import BytesIO

from src.core import get_engine, get_settings
from src.schemas.painel_solar import DeteccaoPainelRequest, DeteccaoPainelResponse
from sqlalchemy import text

router = APIRouter(prefix="/api/v1/paineis-solares", tags=["paineis-solares"])
logger = logging.getLogger(__name__)


@router.post("/detectar", response_model=DeteccaoPainelResponse)
async def detectar_paineis_solares_transformador(
    request: DeteccaoPainelRequest,
    background_tasks: BackgroundTasks = None
):
    """
    Detecta painéis solares em telhados já detectados de um transformador.
    
    Este endpoint:
    1. Busca todos os telhados já detectados do transformador
    2. Para cada telhado, baixa a imagem do Google Maps na coordenada do telhado
    3. Detecta painéis solares usando modelo YOLO treinado
    4. Salva os painéis detectados no banco de dados
    5. Calcula a potência total instalada
    
    **Parâmetros:**
    - **transformador_id**: ID do transformador no banco
    - **confianca_minima**: Confiança mínima para detecção (0-1), default=0.5
    - **potencia_por_m2**: Potência por m² dos painéis (W/m²), default=200
    - **processar_todos_telhados**: Se False, processa apenas telhados sem painéis já detectados
    
    **Retorna:**
    - Status da detecção
    - Número de telhados processados
    - Número de painéis detectados
    - IDs dos painéis salvos no banco
    - Área total e potência total
    - Tempo de processamento
    """
    tempo_inicio = time.time()
    engine = get_engine()
    
    erros = []
    avisos = []
    paineis_salvos = []
    telhados_processados = 0
    area_total_paineis = 0.0
    potencia_total_kw = 0.0
    
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Iniciando detecção de painéis solares para transformador {request.transformador_id}")
        logger.info(f"Configuração: confiança>={request.confianca_minima}, potência={request.potencia_por_m2}W/m²")
        logger.info(f"{'='*80}\n")
        
        # ===================================================================
        # 1. BUSCAR TELHADOS DO TRANSFORMADOR
        # ===================================================================
        logger.info("[1/4] Buscando telhados do transformador...")
        
        with engine.connect() as conn:
            query = """
                SELECT 
                    t.id, t.latitude, t.longitude, t.area_m2, t.confianca,
                    t.url_imagem_origem, t.subestacao_id, t.resolucao_cm
                FROM telhados_detectados_transformador t
                WHERE t.transformador_id = :transformador_id
            """
            
            if not request.processar_todos_telhados:
                # Apenas telhados sem painéis
                query += """
                    AND NOT EXISTS (
                        SELECT 1 FROM paineis_solares_detectados p 
                        WHERE p.telhado_id = t.id
                    )
                """
            
            query += " ORDER BY t.id"
            
            result = conn.execute(
                text(query),
                {"transformador_id": request.transformador_id}
            )
            
            telhados = []
            for row in result:
                telhados.append({
                    'id': row[0],
                    'latitude': row[1],
                    'longitude': row[2],
                    'area_m2': row[3],
                    'confianca': row[4],
                    'url_imagem_origem': row[5],
                    'subestacao_id': row[6],
                    'resolucao_cm': row[7] if row[7] else 60.0
                })
        
        if not telhados:
            avisos.append(f"Nenhum telhado encontrado para o transformador {request.transformador_id}")
            logger.warning(avisos[-1])
            
            return DeteccaoPainelResponse(
                sucesso=False,
                transformador_id=request.transformador_id,
                telhados_processados=0,
                paineis_detectados=0,
                tempo_processamento_s=round(time.time() - tempo_inicio, 2),
                paineis_salvos=[],
                avisos=avisos,
                erros=erros,
                detalhes={}
            )
        
        logger.info(f"✓ {len(telhados)} telhados encontrados")
        
        # ===================================================================
        # 2. INICIALIZAR MODELO YOLO DE PAINÉIS SOLARES
        # ===================================================================
        logger.info("[2/4] Inicializando modelo YOLO de painéis solares...")
        
        from ultralytics import YOLO
        
        modelo_path = r"C:\Hackathon\Git\energy-netload-monitor\notebooks\runs\detect\stage2_panel_solar_refined\weights\best.pt"
        
        if not os.path.exists(modelo_path):
            raise HTTPException(
                status_code=500,
                detail=f"Modelo de painéis solares não encontrado: {modelo_path}"
            )
        
        modelo = YOLO(modelo_path)
        logger.info(f"✓ Modelo carregado: {modelo_path}")
        
        # ===================================================================
        # 3. PROCESSAR CADA TELHADO
        # ===================================================================
        logger.info(f"[3/4] Processando {len(telhados)} telhados...")
        
        settings = get_settings()
        google_api_key = settings.google_maps_api_key
        
        for idx, telhado in enumerate(telhados):
            try:
                logger.info(f"\n  Telhado {idx+1}/{len(telhados)} (ID={telhado['id']})")
                logger.info(f"  Coords: ({telhado['latitude']:.6f}, {telhado['longitude']:.6f})")
                logger.info(f"  Área: {telhado['area_m2']:.2f} m²")
                
                # Gerar URL do Google Maps para o telhado específico
                zoom = 21  # Zoom máximo para melhor detecção de painéis
                size = 640
                
                url = f"https://maps.googleapis.com/maps/api/staticmap"
                url += f"?center={telhado['latitude']},{telhado['longitude']}"
                url += f"&zoom={zoom}&size={size}x{size}"
                url += f"&maptype=satellite&scale=2"
                
                if google_api_key:
                    url += f"&key={google_api_key}"
                
                # Baixar imagem
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                img_pil = Image.open(BytesIO(response.content))
                img_array = np.array(img_pil.convert('RGB'))
                
                # Detectar painéis solares
                resultados = modelo.predict(
                    img_array,
                    conf=request.confianca_minima,
                    verbose=False
                )
                
                paineis_telhado = []
                
                for resultado in resultados:
                    if resultado.boxes is not None:
                        for box in resultado.boxes:
                            x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                            confianca = box.conf[0].item()
                            
                            # Calcular centroide
                            cx = (x1 + x2) / 2
                            cy = (y1 + y2) / 2
                            
                            # Calcular área em pixels
                            area_pixeis = int((x2 - x1) * (y2 - y1))
                            
                            # Converter para m² usando resolução
                            resolucao_m = telhado['resolucao_cm'] / 100.0
                            area_m2 = area_pixeis * (resolucao_m ** 2)
                            
                            # Calcular potência estimada
                            potencia_w = area_m2 * request.potencia_por_m2
                            
                            paineis_telhado.append({
                                'telhado_id': telhado['id'],
                                'transformador_id': request.transformador_id,
                                'subestacao_id': telhado['subestacao_id'],
                                'bbox_json': json.dumps({
                                    'x': float(x1),
                                    'y': float(y1),
                                    'x2': float(x2),
                                    'y2': float(y2),
                                    'w': float(x2 - x1),
                                    'h': float(y2 - y1)
                                }),
                                'centroide_json': json.dumps({
                                    'x': float(cx),
                                    'y': float(cy)
                                }),
                                'area_pixeis': area_pixeis,
                                'area_m2': area_m2,
                                'confianca': confianca,
                                'tipo_painel': 'fotovoltaico',
                                'potencia_w': potencia_w
                            })
                
                logger.info(f"  ✓ Detectados: {len(paineis_telhado)} painéis")
                
                # Salvar painéis no banco
                if paineis_telhado:
                    with engine.begin() as conn:
                        for painel in paineis_telhado:
                            result = conn.execute(
                                text("""
                                    INSERT INTO paineis_solares_detectados 
                                    (telhado_id, transformador_id, subestacao_id, 
                                     bbox_json, centroide_json, area_pixeis, area_m2,
                                     confianca, tipo_painel, potencia_w, timestamp_deteccao)
                                    VALUES 
                                    (:telhado_id, :transformador_id, :subestacao_id,
                                     CAST(:bbox_json AS jsonb), CAST(:centroide_json AS jsonb),
                                     :area_pixeis, :area_m2, :confianca, :tipo_painel, 
                                     :potencia_w, NOW())
                                    RETURNING id
                                """),
                                painel
                            )
                            painel_id = result.scalar()
                            paineis_salvos.append(painel_id)
                            
                            area_total_paineis += painel['area_m2']
                            potencia_total_kw += painel['potencia_w'] / 1000.0
                    
                    logger.info(f"  ✓ {len(paineis_telhado)} painéis salvos no banco")
                
                telhados_processados += 1
                
            except Exception as e:
                erro_msg = f"Erro ao processar telhado {telhado['id']}: {str(e)}"
                logger.error(erro_msg)
                erros.append(erro_msg)
                continue
        
        # ===================================================================
        # 4. FINALIZAR
        # ===================================================================
        logger.info("[4/4] Finalizando...")
        
        tempo_total = time.time() - tempo_inicio
        
        logger.info(f"\n{'='*80}")
        logger.info("DETECÇÃO DE PAINÉIS CONCLUÍDA")
        logger.info(f"Telhados processados: {telhados_processados}/{len(telhados)}")
        logger.info(f"Painéis detectados: {len(paineis_salvos)}")
        logger.info(f"Área total: {area_total_paineis:.2f} m²")
        logger.info(f"Potência total: {potencia_total_kw:.2f} kW")
        logger.info(f"Tempo total: {tempo_total:.2f}s")
        logger.info(f"{'='*80}\n")
        
        return DeteccaoPainelResponse(
            sucesso=len(paineis_salvos) > 0,
            transformador_id=request.transformador_id,
            telhados_processados=telhados_processados,
            paineis_detectados=len(paineis_salvos),
            tempo_processamento_s=round(tempo_total, 2),
            paineis_salvos=paineis_salvos,
            area_total_paineis_m2=round(area_total_paineis, 2),
            potencia_total_kw=round(potencia_total_kw, 2),
            erros=erros,
            avisos=avisos,
            detalhes={
                'telhados_disponiveis': len(telhados),
                'media_paineis_por_telhado': round(len(paineis_salvos) / max(telhados_processados, 1), 2),
                'potencia_por_m2': request.potencia_por_m2
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro crítico na detecção de painéis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno ao processar detecção de painéis: {str(e)}"
        )
