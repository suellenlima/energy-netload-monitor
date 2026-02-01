"""
Endpoint unificado para detectar telhados e painéis solares em um transformador
Reutiliza imagens para maximizar eficiência

Author: Energy Netload Monitor
Date: 2026-02-01
"""

import logging
import json
import time
import os
from datetime import datetime
from typing import Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
import cv2
import numpy as np
from PIL import Image
from io import BytesIO
import requests

from ..core import get_engine
from ..schemas.painel_solar import (
    PainelSolarResponse,
    EstimativaPotenciaResponse,
    TelhadorComPaineis,
    DeteccaoPainelSolarResponse
)
from ..services.telhado_segmentation_service import TelhadoSegmentationService
from ..services.painel_solar_detection_service import PainelSolarDetectionService

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


class ResultadoTelhadoComPaineis(BaseModel):
    """Resultado de um telhado com seus painéis detectados"""
    
    telhado_id: int
    num_telhados_detectados: int
    num_paineis_detectados: int
    area_telhado_m2: float
    area_paineis_total_m2: float
    potencia_paineis_kw: float
    producao_anual_kwh: float
    economia_anual_brl: float


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


# Cache global dos serviços
_servico_telhados: Optional[TelhadoSegmentationService] = None
_servico_paineis: Optional[PainelSolarDetectionService] = None


def _obter_servico_telhados() -> TelhadoSegmentationService:
    """Obtém ou cria instância do serviço de telhados"""
    global _servico_telhados
    
    if _servico_telhados is None:
        logger.info("🔧 Inicializando serviço de segmentação de telhados...")
        _servico_telhados = TelhadoSegmentationService()
    
    return _servico_telhados


def _obter_servico_paineis() -> PainelSolarDetectionService:
    """Obtém ou cria instância do serviço de painéis solares"""
    global _servico_paineis
    
    if _servico_paineis is None:
        logger.info("🔧 Inicializando serviço de detecção de painéis solares...")
        _servico_paineis = PainelSolarDetectionService()
    
    return _servico_paineis


def _criar_dir_cache() -> Path:
    """Cria diretório de cache se não existir"""
    cache_dir = Path("data/cache/imagens_grid")
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _salvar_imagem_em_cache(url_imagem: str, transformador_id: int, indice: int) -> Optional[str]:
    """
    Baixa e salva imagem em cache
    
    Returns:
        Caminho da imagem salva ou None se erro
    """
    try:
        cache_dir = _criar_dir_cache()
        filename = f"trafo_{transformador_id}_img_{indice:03d}.png"
        filepath = cache_dir / filename
        
        # Se já existe, usar
        if filepath.exists():
            logger.info(f"[CACHE] 💾 Imagem encontrada em cache: {filepath}")
            return str(filepath)
        
        # Baixar imagem
        logger.info(f"[CACHE] 📥 Baixando imagem...")
        response = requests.get(url_imagem, timeout=30)
        response.raise_for_status()
        
        # Salvar em disco
        imagem = Image.open(BytesIO(response.content))
        if imagem.mode != 'RGB':
            imagem = imagem.convert('RGB')
        
        imagem.save(filepath)
        logger.info(f"[CACHE] ✅ Imagem salva em: {filepath}")
        
        return str(filepath)
    
    except Exception as e:
        logger.error(f"[CACHE] ❌ Erro ao salvar imagem em cache: {e}")
        return None


def _carregar_imagem_do_cache(caminho_imagem: str) -> Optional[np.ndarray]:
    """Carrega imagem do disco como numpy array"""
    try:
        img = cv2.imread(caminho_imagem)
        if img is None:
            logger.error(f"[CACHE] ❌ Não foi possível carregar: {caminho_imagem}")
            return None
        return img
    except Exception as e:
        logger.error(f"[CACHE] ❌ Erro ao carregar imagem: {e}")
        return None


def _salvar_ref_imagem_banco(transformador_id: int, indice: int, url: str, caminho_disco: str) -> bool:
    """Salva referência da imagem no banco (satelite_requisicoes_google_maps)"""
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO satelite_requisicoes_google_maps 
                (transformador_id, tipo_requisicao, url_satellite, url_hybrid, zoom, tamanho_pixels, status)
                VALUES (:trans_id, 'grid_' || :idx, :caminho, :url, 20, '640x640', 'processada')
                ON CONFLICT DO NOTHING
            """), {
                'trans_id': transformador_id,
                'idx': indice,
                'url': url,
                'caminho': caminho_disco
            })
        return True
    except Exception as e:
        logger.warning(f"[CACHE] ⚠️ Erro ao salvar ref de imagem no banco: {e}")
        return False


@router.post(
    "/processar-completo",
    response_model=ProcessarTransformadorResponse,
    summary="Processar telhados E painéis solares (pipeline unificado)",
    description="Detecta telhados e painéis solares em um transformador, reutilizando imagens baixadas"
)
async def processar_transformador_completo(
    request: ProcessarTransformadorRequest
) -> ProcessarTransformadorResponse:
    """
    Pipeline unificado que:
    1. Baixa imagens do grid do transformador (UMA VEZ)
    2. Detecta telhados em cada imagem
    3. Para cada telhado: detecta painéis solares
    4. Reutiliza imagens em cache
    
    Muito mais eficiente que chamar 2 endpoints separados!
    """
    try:
        tempo_inicio = time.time()
        
        logger.info(f"[PIPELINE] 🚀 Iniciando pipeline unificado para transformador {request.transformador_id}")
        
        # Obter serviços
        servico_telhados = _obter_servico_telhados()
        servico_paineis = _obter_servico_paineis()
        engine = get_engine()
        
        # Buscar transformador
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT latitude, longitude, subestacao_id
                FROM transformadores
                WHERE id = :trans_id
            """), {'trans_id': request.transformador_id})
            
            trafo_data = result.fetchone()
            if not trafo_data:
                raise HTTPException(status_code=404, detail=f"Transformador {request.transformador_id} não encontrado")
        
        lat_trafo, lon_trafo, subestacao_id = trafo_data
        
        logger.info(f"[PIPELINE] 📍 Transformador encontrado: lat={lat_trafo}, lon={lon_trafo}")
        
        # Gerar grid de imagens
        logger.info(f"[PIPELINE] 🗺️ Gerando grid de imagens...")
        imagens_grid = servico_telhados.gerar_imagens_grid(lat_trafo, lon_trafo)
        logger.info(f"[PIPELINE] ✓ Encontradas {len(imagens_grid)} imagens grid")
        
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM paineis_solares_detectados 
                WHERE transformador_id = :trans_id
            """), {'trans_id': request.transformador_id})
            
            conn.execute(text("""
                DELETE FROM potencia_telhados 
                WHERE transformador_id = :trans_id
            """), {'trans_id': request.transformador_id})
        
        # Processar cada imagem do grid
        total_telhados = 0
        total_paineis = 0
        todos_telhados_com_paineis = []
        potencia_total = None
        erros = []
        
        for idx, imagem in enumerate(imagens_grid, 1):
            try:
                logger.info(f"[PIPELINE] ⏳ Processando imagem {idx}/{len(imagens_grid)}")
                
                # ====== ETAPA 0: BAIXAR E SALVAR IMAGEM EM CACHE ======
                logger.info(f"[PIPELINE] 💾 Salvando imagem em cache...")
                caminho_imagem_cache = _salvar_imagem_em_cache(imagem['url'], request.transformador_id, idx)
                
                if not caminho_imagem_cache:
                    logger.warning(f"[PIPELINE] ⚠️ Não foi possível salvar imagem em cache")
                    erros.append(f"Imagem {idx}: Erro ao salvar em cache")
                    continue
                
                # Salvar referência no banco
                _salvar_ref_imagem_banco(request.transformador_id, idx, imagem['url'], caminho_imagem_cache)
                
                # ====== ETAPA 1: DETECTAR TELHADOS (usando imagem do cache) ======
                logger.info(f"[PIPELINE] 🏠 Detectando telhados...")
                
                # Carregar imagem do cache
                img_array = _carregar_imagem_do_cache(caminho_imagem_cache)
                if img_array is None:
                    logger.warning(f"[PIPELINE] ⚠️ Não foi possível carregar imagem do cache")
                    erros.append(f"Imagem {idx}: Erro ao carregar do cache")
                    continue
                
                # Detectar telhados usando o serviço
                resultado_telhados = servico_telhados.processar_telhados_lote(
                    caminho_imagem_cache,  # Usar caminho do disco
                    latitude_centro=lat_trafo,
                    longitude_centro=lon_trafo,
                    transformador_id=request.transformador_id,
                    subestacao_id=subestacao_id,
                    confianca_minima=request.confianca_minima_telhados,
                    sem_autenticacao=False
                )
                
                if resultado_telhados['sucesso']:
                    num_telhados_imagem = resultado_telhados['total_telhados_segmentados']
                    logger.info(f"[PIPELINE] ✅ Detectados {num_telhados_imagem} telhados na imagem {idx}")
                    total_telhados += num_telhados_imagem
                
                # ====== ETAPA 2: DETECTAR PAINÉIS NOS TELHADOS ======
                if resultado_telhados['sucesso'] and resultado_telhados['telhados_segmentados']:
                    logger.info(f"[PIPELINE] ☀️ Detectando painéis solares nos telhados...")
                    
                    for telhado in resultado_telhados['telhados_segmentados']:
                        try:
                            telhado_id = telhado['id']
                            bbox = telhado['bbox']
                            
                            # Processar telhado para painéis (usar imagem do cache)
                            resultado_paineis = servico_paineis.processar_telhado(
                                url_imagem=caminho_imagem_cache,  # Usar caminho do disco
                                bbox=bbox,
                                confianca_minima=request.confianca_minima_paineis,
                                potencia_por_m2=150.0
                            )
                            
                            if resultado_paineis['sucesso'] and resultado_paineis['paineis']:
                                num_paineis = len(resultado_paineis['paineis'])
                                logger.info(f"[PIPELINE] ✅ Detectados {num_paineis} painéis no telhado {telhado_id}")
                                total_paineis += num_paineis
                                
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
                                    for p in resultado_paineis['paineis']
                                ]
                                
                                potencia_response = EstimativaPotenciaResponse(
                                    **resultado_paineis['potencia']
                                ) if resultado_paineis['potencia'] else None
                                
                                # Acumular potência total
                                if potencia_total is None:
                                    potencia_total = resultado_paineis['potencia'].copy() if resultado_paineis['potencia'] else None
                                else:
                                    if resultado_paineis['potencia']:
                                        potencia_total['total_area_m2'] += resultado_paineis['potencia']['total_area_m2']
                                        potencia_total['num_paineis'] += resultado_paineis['potencia']['num_paineis']
                                        potencia_total['potencia_instalada_kw'] += resultado_paineis['potencia']['potencia_instalada_kw']
                                        potencia_total['producao_diaria_kwh'] += resultado_paineis['potencia']['producao_diaria_kwh']
                                        potencia_total['producao_anual_kwh'] += resultado_paineis['potencia']['producao_anual_kwh']
                                        potencia_total['economia_anual_brl'] += resultado_paineis['potencia']['economia_anual_brl']
                                
                                # Guardar telhado com painéis
                                todos_telhados_com_paineis.append(
                                    TelhadorComPaineis(
                                        telhado_id=telhado_id,
                                        num_paineis=num_paineis,
                                        area_total_m2=potencia_response.total_area_m2 if potencia_response else 0,
                                        potencia_instalada_kw=potencia_response.potencia_instalada_kw if potencia_response else 0,
                                        producao_anual_kwh=potencia_response.producao_anual_kwh if potencia_response else 0,
                                        economia_anual_brl=potencia_response.economia_anual_brl if potencia_response else 0,
                                        paineis=paineis_response
                                    )
                                )
                                
                                # Salvar painéis no banco
                                try:
                                    with engine.begin() as conn:
                                        # Limpar painéis anteriores deste telhado
                                        conn.execute(text("""
                                            DELETE FROM paineis_solares_detectados 
                                            WHERE telhado_id = :telhado_id
                                        """), {'telhado_id': telhado_id})
                                        
                                        # Buscar dados do telhado
                                        result = conn.execute(text("""
                                            SELECT transformador_id, subestacao_id
                                            FROM telhados_detectados_transformador
                                            WHERE id = :telhado_id
                                        """), {'telhado_id': telhado_id})
                                        
                                        telhado_data = result.fetchone()
                                        if telhado_data:
                                            trans_id, sub_id = telhado_data
                                            
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
                                                'num': num_paineis,
                                                'area': potencia_response.total_area_m2 if potencia_response else 0,
                                                'pot_kw': potencia_response.potencia_instalada_kw if potencia_response else 0,
                                                'prod_dia': potencia_response.producao_diaria_kwh if potencia_response else 0,
                                                'prod_ano': potencia_response.producao_anual_kwh if potencia_response else 0,
                                                'economia': potencia_response.economia_anual_brl if potencia_response else 0,
                                                'pot_m2': 150.0
                                            })
                                    
                                    logger.info(f"[PIPELINE] 💾 {num_paineis} painéis salvos para telhado {telhado_id}")
                                
                                except Exception as e:
                                    logger.error(f"[PIPELINE] ❌ Erro ao salvar painéis: {e}")
                            
                            else:
                                logger.info(f"[PIPELINE] ⚪ Nenhum painel detectado no telhado {telhado_id}")
                        
                        except Exception as e:
                            logger.error(f"[PIPELINE] ❌ Erro ao processar telhado: {e}")
                            erros.append(f"Telhado: {str(e)}")
            
            except Exception as e:
                logger.error(f"[PIPELINE] ❌ Erro ao processar imagem {idx}: {e}")
                erros.append(f"Imagem {idx}: {str(e)}")
        
        # Construir resposta final
        potencia_response = None
        if potencia_total:
            potencia_response = EstimativaPotenciaResponse(**potencia_total)
        
        response = ProcessarTransformadorResponse(
            sucesso=len(erros) == 0,
            transformador_id=request.transformador_id,
            num_imagens_processadas=len(imagens_grid),
            total_telhados_detectados=total_telhados,
            total_paineis_detectados=total_paineis,
            telhados_com_paineis=todos_telhados_com_paineis,
            potencia_total=potencia_response,
            erros=erros,
            tempo_processamento_s=time.time() - tempo_inicio
        )
        
        logger.info(f"[PIPELINE] ✅ Pipeline concluído em {time.time() - tempo_inicio:.2f}s")
        logger.info(f"[PIPELINE] 📊 Total: {total_telhados} telhados, {total_paineis} painéis")
        if potencia_response:
            logger.info(f"[PIPELINE] 💡 Potência: {potencia_response.potencia_instalada_kw:.2f}kW, Produção anual: {potencia_response.producao_anual_kwh:.0f}kWh")
        
        return response
    
    except Exception as e:
        logger.error(f"[PIPELINE] ❌ Erro no pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
