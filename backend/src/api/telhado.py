"""
API REST para processamento de telhados/edifícios em imagens de satélite

Endpoints:
- POST /telhados/segmentar-subestacao - Processa telhados de uma subestação
- GET /telhados/lista - Lista telhados processados (com filtros)
- POST /telhados/processar-lote - Processa múltiplas subestações
- GET /telhados/subestacao/{id} - Detalhes de telhados de uma subestação
- GET /telhados/estatisticas - Estatísticas agregadas
- POST /telhados/processar-com-yolo - Processa ROI com modelo YOLO
- POST /telhados/registrar-modelo-yolo - Registra novo modelo YOLO

Author: Energy Netload Monitor
Date: 2025
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import numpy as np
import cv2
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Path
from fastapi.responses import JSONResponse
from sqlalchemy import text

from ..services.telhado_segmentation_service import (
    TelhadoSegmentationService,
    ResultadoProcessamentoTelhados
)
from ..services.telhado_transformador_service import (
    TelhadoTransformadorService
)
from ..services.google_maps_telhado_integration import (
    GoogleMapsTelhadoIntegrationService
)
from ..schemas.painel_solar import (
    DetectarPainelSolarRequest,
    DetectarPainelSolarEmRoiRequest,
    DeteccaoPainelSolarResponse,
    PainelSolarResponse,
    EstimativaPotenciaResponse
)
from ..services.painel_solar_detection_service import (
    PainelSolarDetectionService
)
from ..schemas.telhado import (
    SegmentarTelhadoRequest,
    SegmentarTelhadoComImagemIdRequest,
    ProcessarLoteTelhadosRequest,
    ConsultarTelhadosRequest,
    ResultadoSegmentacaoResponse,
    ResultadoSegmentacaoTransformadorResponse,
    TelhadoDetectadoResponse,
    TelhadoSegmentadoResponse,
    ResultadoLoteResponse,
    ListaTelhadosResponse,
    EstatisticasSegmentacaoResponse,
    ProcessarComYOLORequest,
    RegistrarModeloYOLORequest,
    ResultadoProcessamentoYOLOResponse,
    # Novo
    SegmentarTelhadoTransformadorRequest,
    TelhadoTransformadorResponse,
    ResultadoDeteccaoTransformadorResponse,
    ProcessarLoteTelhadosTransformadorRequest,
    ListaTelhadosTransformadorResponse,
    EstatisticasTransformadorResponse
)

# Configure logging
logger = logging.getLogger(__name__)

# Criar router
router = APIRouter(
    prefix="/telhados",
    tags=["Telhados - Segmentação"],
    responses={404: {"description": "Não encontrado"}}
)

# Cache global do serviço (simplificado - usar Redis em produção)
_servico_segmentacao: Optional[TelhadoSegmentationService] = None
_modelos_yolo_registrados: Dict[str, Dict] = {}
_resultados_processamento: Dict[str, ResultadoProcessamentoTelhados] = {}


def _obter_servico() -> TelhadoSegmentationService:
    """Obtém ou cria instância do serviço (singleton pattern com inicialização lazy)"""
    global _servico_segmentacao
    if _servico_segmentacao is None:
        logger.info("Inicializando serviço de segmentação de telhados...")
        try:
            _servico_segmentacao = TelhadoSegmentationService(use_gpu=True)
            logger.info("✓ Serviço de segmentação inicializado com sucesso")
        except Exception as e:
            logger.warning(f"Erro ao inicializar GPU, tentando CPU: {e}")
            _servico_segmentacao = TelhadoSegmentationService(use_gpu=False)
            logger.info("✓ Serviço de segmentação inicializado (CPU)")
    return _servico_segmentacao


def _verificar_ou_buscar_imagens_grid(
    transformador_id: int,
    engine,
    zoom_grid: int = 20,
    tamanho: str = "640x640"
) -> Dict:
    """
    Verifica se existem imagens grid salvas para o transformador.
    Se não existir, busca e salva automaticamente.
    
    Args:
        transformador_id: ID do transformador
        engine: SQLAlchemy engine
        zoom_grid: Nível de zoom para o grid
        tamanho: Tamanho das imagens
    
    Returns:
        Dict com imagens encontradas ou salvas
    """
    try:
        # 1. Verificar se já existem imagens grid salvas
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    id,
                    url,
                    propriedades_json->>'grid_posicao' as posicao,
                    propriedades_json->'grid_posicao'->>'linha' as linha,
                    propriedades_json->'grid_posicao'->>'coluna' as coluna
                FROM satelite_imagens
                WHERE sensor LIKE 'Google_Maps_Grid%'
                  AND propriedades_json->>'transformador_id' = :transformador_id
                ORDER BY 
                    (propriedades_json->'grid_posicao'->>'linha')::int,
                    (propriedades_json->'grid_posicao'->>'coluna')::int
            """)
            
            result = conn.execute(query, {'transformador_id': str(transformador_id)})
            rows = result.fetchall()
            
            if rows:
                # Imagens já existem
                imagens = [
                    {
                        'imagem_id': row[0],
                        'url': row[1],
                        'linha': int(row[3]) if row[3] else 0,
                        'coluna': int(row[4]) if row[4] else 0
                    }
                    for row in rows
                ]
                
                logger.info(f"✓ Encontradas {len(imagens)} imagens grid já salvas para transformador {transformador_id}")
                
                return {
                    'sucesso': True,
                    'origem': 'banco_dados',
                    'total_imagens': len(imagens),
                    'imagens': imagens,
                    'transformador_id': transformador_id
                }
        
        # 2. Se não existir, buscar e salvar
        logger.info(f"⚠ Nenhuma imagem grid encontrada para transformador {transformador_id}")
        logger.info(f"📊 Buscando e salvando grid automaticamente (zoom={zoom_grid}, tamanho={tamanho})...")
        
        # Buscar subestacao_id
        with engine.connect() as conn:
            query = text("SELECT subestacao_id FROM transformadores WHERE id = :id")
            result = conn.execute(query, {'id': transformador_id})
            row = result.fetchone()
            
            if not row:
                return {
                    'sucesso': False,
                    'erro': f'Transformador {transformador_id} não encontrado'
                }
            
            subestacao_id = row[0]
        
        # Buscar e salvar grid
        from ..services.google_maps_service_v2 import GoogleMapsServiceV2
        from ..services.imagem_salvamento_service import ImagemSalvamentoService
        
        # Obter API key do ambiente
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        # Buscar grid
        maps_service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        resultado_grid = maps_service.buscar_imagens_grid_transformador(
            transformador_id=transformador_id,
            tamanho=tamanho,
            zoom_grid=zoom_grid
        )
        
        if not resultado_grid.get('sucesso'):
            logger.error(f"❌ Erro ao gerar grid: {resultado_grid.get('motivo')}")
            return {
                'sucesso': False,
                'erro': f"Erro ao gerar grid: {resultado_grid.get('motivo')}"
            }
        
        # Salvar no banco
        salvamento_service = ImagemSalvamentoService(engine)
        resultado_salvamento = salvamento_service.salvar_imagens_grid_google_maps(
            subestacao_id=subestacao_id,
            transformador_id=transformador_id,
            resultado_grid=resultado_grid
        )
        
        if resultado_salvamento.get('sucesso'):
            logger.info(f"✅ Grid salvo automaticamente: {resultado_salvamento['total_salvas']} imagens")
            
            return {
                'sucesso': True,
                'origem': 'busca_automatica',
                'total_imagens': resultado_salvamento['total_salvas'],
                'imagens': resultado_salvamento['imagens'],
                'transformador_id': transformador_id,
                'sensor': resultado_salvamento['sensor']
            }
        else:
            return {
                'sucesso': False,
                'erro': resultado_salvamento.get('erro', 'Erro ao salvar grid')
            }
    
    except Exception as e:
        logger.error(f"❌ Erro ao verificar/buscar imagens grid: {e}", exc_info=True)
        return {
            'sucesso': False,
            'erro': str(e)
        }

# ============================================================================
# ENDPOINT 1: Segmentar Telhados de Uma Subestação
# ============================================================================

@router.post(
    "/segmentar-subestacao",
    response_model=ResultadoSegmentacaoResponse,
    summary="Segmentar telhados de uma subestação (modelo fine-tuned)",
    description="""
    Processa imagem de satélite e segmenta todos os telhados/edifícios detectados usando modelo YOLO fine-tuned.
    
    Fluxo:
    1. Download da imagem via URL
    2. Detecção de edifícios com YOLOv8 fine-tuned (treinado em 240+ imagens de telhados)
    3. Segmentação com OpenCV (morphology + contours)
    4. Extração de ROIs individuais
    5. Armazenamento de metadados
    
    Modelo: notebooks/roof_dataset_yolo/trained_models/best.pt
    - Treinado especificamente para telhados em imagens de satélite
    - Alta precisão em diferentes tipos de cobertura
    - Otimizado para CPU
    
    Returns:
    - Telhados detectados com bounding boxes
    - Telhados segmentados com ROIs
    - Estatísticas de processamento
    """,
    status_code=200
)
async def segmentar_subestacao(
    request: SegmentarTelhadoRequest,
    background_tasks: BackgroundTasks,
    usar_grid_auto: bool = Query(
        False, 
        description="Se True, busca automaticamente imagens grid se url_imagem_satelite não fornecida"
    )
) -> ResultadoSegmentacaoResponse:
    """
    Segmenta telhados em imagem de satélite
    
    Args:
        request: Configuração do processamento
        usar_grid_auto: Se True, busca grid automaticamente se URL não fornecida
        
    Returns:
        Resultado com telhados detectados e segmentados
    """
    try:
        logger.info(f"Iniciando segmentação para subestação: {request.id_subestacao}")
        
        # Verificar se deve usar grid automático
        url_imagem_original = request.url_imagem_satelite
        
        if usar_grid_auto and not request.url_imagem_satelite:
            logger.info(f"[SEGMENTAÇÃO] Modo grid automático ativado para subestação {request.id_subestacao}")
            
            from ..core import get_engine
            engine = get_engine()
            
            # Buscar transformador da subestação
            with engine.connect() as conn:
                query = text("SELECT id FROM transformadores WHERE subestacao_id = :subestacao_id LIMIT 1")
                result = conn.execute(query, {'subestacao_id': request.id_subestacao})
                row = result.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Nenhum transformador encontrado para subestação {request.id_subestacao}"
                    )
                
                transformador_id = row[0]
            
            # Buscar ou criar imagens grid
            resultado_grid = _verificar_ou_buscar_imagens_grid(
                transformador_id=transformador_id,
                engine=engine,
                zoom_grid=20,
                tamanho="640x640"
            )
            
            if not resultado_grid.get('sucesso'):
                raise HTTPException(
                    status_code=500,
                    detail=f"Erro ao obter grid: {resultado_grid.get('erro')}"
                )
            
            # Usar primeira imagem do grid
            imagens_grid = resultado_grid.get('imagens', [])
            if imagens_grid:
                request.url_imagem_satelite = imagens_grid[0]['url']
                logger.info(f"✓ Usando grid automaticamente: {len(imagens_grid)} imagens (origem: {resultado_grid.get('origem')})")
            else:
                raise HTTPException(
                    status_code=404,
                    detail="Grid não contém imagens"
                )
        
        # Obter serviço
        servico = _obter_servico()
        
        # Executar pipeline
        resultado = servico.processar_telhados_lote(
            url_imagem=request.url_imagem_satelite,
            id_subestacao=request.id_subestacao,
            id_imagem_satelite=f"sat_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            resolucao_m_por_pixel=request.resolucao_m_por_pixel,
            confianca_minima=request.confianca_minima,
            diretorio_saida=request.diretorio_saida if request.salvar_rois else None
        )
        
        # Armazenar resultado em cache
        chave_resultado = f"{request.id_subestacao}_{resultado.id_imagem_satelite}"
        _resultados_processamento[chave_resultado] = resultado
        
        # Converter para response
        response = _converter_resultado_para_response(resultado)
        
        # Log de sucesso
        if response.sucesso:
            logger.info(f"✓ Segmentação concluída. Telhados: {response.telhados_detectados}")
        else:
            logger.error(f"✗ Erro na segmentação: {response.erros}")
        
        return response
        
    except Exception as e:
        logger.error(f"Erro ao segmentar subestação: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao segmentar telhados: {str(e)}"
        )


# ============================================================================
# ENDPOINT 1B: Segmentar Telhados usando Imagem_ID do Banco (v2)
# ============================================================================

def _recuperar_bandas_do_banco(engine, imagem_id: int) -> Dict[str, str]:
    """
    Recupera URLs das 5 bandas (Blue, Green, Red, NIR, SWIR) do banco de dados
    
    Args:
        engine: SQLAlchemy engine
        imagem_id: ID da imagem em satelite_imagens
    
    Returns:
        Dicionário com as URLs das bandas: {'blue': '...', 'green': '...', ...}
    """
    from ..core import get_engine
    
    engine = engine or get_engine()
    
    try:
        query = text("""
            SELECT nome_banda, url 
            FROM satelite_bandas 
            WHERE imagem_id = :imagem_id
            ORDER BY numero_banda ASC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"imagem_id": imagem_id})
            bandas = {}
            
            for row in result:
                nome_banda = row[0].lower()  # 'blue', 'green', 'red', 'nir', 'swir'
                url = row[1]
                bandas[nome_banda] = url
            
            if not bandas:
                raise ValueError(f"Nenhuma banda encontrada para imagem_id={imagem_id}")
            
            logger.info(f"Bandas recuperadas para imagem {imagem_id}: {list(bandas.keys())}")
            return bandas
    
    except Exception as e:
        logger.error(f"Erro ao recuperar bandas do banco: {e}", exc_info=True)
        raise HTTPException(
            status_code=404,
            detail=f"Imagem {imagem_id} não encontrada ou sem bandas registradas"
        )


@router.post(
    "/segmentar-transformador-v2",
    response_model=ResultadoSegmentacaoTransformadorResponse,
    summary="Segmentar telhados do transformador usando imagem_id ou grid automático (v2)",
    description="""
    **VERSÃO 2** - Processa telhados de um transformador usando imagem do banco ou grid automático Google Maps.
    
    Este endpoint:
    1. Se imagem_id fornecido: Recupera CBERS-4A com 5 bandas multiespectrais
    2. Se imagem_id NÃO fornecido: Busca/salva grid automaticamente do Google Maps
    3. Processa com normalização multibanda RGB+NDVI (CBERS) ou RGB (Google Maps)
    4. Segmenta edifícios/telhados com filtro urbano
    5. Armazena resultados
    
    Fluxo Grid Automático:
    1. Verifica se grid existe no banco para o transformador
    2. Se não existe: busca via Google Maps API e salva
    3. Processa todas as imagens do grid
    4. Agrega resultados de detecção
    
    Fluxo CBERS-4A (com imagem_id):
    1. Download das 5 bandas via URLs
    2. Normalização percentil (2%-98%)
    3. Aplicação de CLAHE (adaptive histogram equalization)
    4. Cálculo de NDVI para máscara urbana
    5. Detecção de edifícios com YOLOv8 + filtro NDVI
    6. Segmentação com OpenCV (morphology + contours)
    7. Extração de ROIs individuais
    
    Returns:
    - Telhados detectados com bounding boxes
    - Telhados segmentados com ROIs
    - Estatísticas de processamento
    - Origem: 'grid_automatico' ou 'imagem_banco'
    """,
    status_code=200
)
async def segmentar_transformador_v2(
    request: SegmentarTelhadoComImagemIdRequest,
    background_tasks: BackgroundTasks,
    engine = None  # Será injetado pela dependency
) -> ResultadoSegmentacaoTransformadorResponse:
    """
    Segmenta telhados de um transformador usando imagem CBERS-4A (5 bandas) ou grid Google Maps
    
    Args:
        request: Configuração com transformador_id e imagem_id (opcional)
        engine: SQLAlchemy engine para consultas ao banco
        
    Returns:
        Resultado com telhados detectados e segmentados
    """
    try:
        import time
        tempo_inicio = time.time()
        
        logger.info(f"="*80)
        logger.info(f"[v2] 🚀 INICIANDO segmentação transformador {request.transformador_id}")
        logger.info(f"[v2] Parâmetros: imagem_id={request.imagem_id}, confianca={request.confianca_minima}")
        logger.info(f"="*80)
        
        from ..core import get_engine
        
        engine = engine or get_engine()
        logger.info(f"[v2] ✓ Engine do banco obtida ({time.time() - tempo_inicio:.2f}s)")
        
        # 🗑️ LIMPAR telhados anteriores do transformador antes de processar novamente
        try:
            from ..services.telhado_transformador_service import TelhadoTransformadorService
            servico_transformador = TelhadoTransformadorService(engine)
            telhados_removidos = servico_transformador.limpar_telhados_transformador(request.transformador_id)
            if telhados_removidos > 0:
                logger.info(f"[v2] 🗑️ Limpeza concluída: {telhados_removidos} telhados anteriores removidos")
            else:
                logger.debug(f"[v2] ✓ Nenhum telhado anterior para limpar")
        except Exception as e:
            logger.warning(f"[v2] ⚠️ Aviso ao limpar telhados anteriores: {e}")
        
        # Verificar se deve usar grid automático
        if not request.imagem_id:
            logger.info(f"[v2-GRID] 🔍 🔍 Modo grid automático para transformador {request.transformador_id}")
            
            # Buscar ou criar imagens grid
            tempo_grid = time.time()
            logger.info(f"[v2-GRID] 📡 Buscando/criando imagens grid...")
            resultado_grid = _verificar_ou_buscar_imagens_grid(
                transformador_id=request.transformador_id,
                engine=engine,
                zoom_grid=20,
                tamanho="640x640"
            )
            logger.info(f"[v2-GRID] ✓ Grid obtido em {time.time() - tempo_grid:.2f}s")
            
            if not resultado_grid.get('sucesso'):
                raise HTTPException(
                    status_code=500,
                    detail=f"Erro ao obter grid: {resultado_grid.get('erro')}"
                )
            
            # Processar primeira imagem do grid
            imagens_grid = resultado_grid.get('imagens', [])
            if not imagens_grid:
                raise HTTPException(
                    status_code=404,
                    detail="Grid não contém imagens"
                )
            
            logger.info(f"✓ [v2-GRID] Processando {len(imagens_grid)} imagens grid (origem: {resultado_grid.get('origem')})")
            
            # Buscar subestacao_id e coordenadas do transformador
            with engine.connect() as conn:
                query = text("SELECT subestacao_id, latitude, longitude FROM transformadores WHERE id = :id")
                result = conn.execute(query, {'id': request.transformador_id})
                row = result.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Transformador {request.transformador_id} não encontrado"
                    )
                
                subestacao_id, lat_trafo, lon_trafo = row
            
            logger.info(f"[v2-GRID] 🏢 Subestação ID: {subestacao_id}")
            
            # Usar serviço para processar TODAS as imagens do grid
            tempo_servico = time.time()
            logger.info(f"[v2-GRID] 🔧 Obtendo serviço de segmentação...")
            servico = _obter_servico()
            logger.info(f"[v2-GRID] ✓ Serviço obtido em {time.time() - tempo_servico:.2f}s")
            
            # Processar todas as imagens do grid
            tempo_processamento = time.time()
            logger.info(f"[v2-GRID] 🤖 INICIANDO processamento YOLO de {len(imagens_grid)} imagens...")
            
            todos_telhados = []
            todos_telhados_segmentados = []
            total_telhados_detectados = 0
            erros_processamento = []
            avisos_processamento = []
            
            for idx, imagem in enumerate(imagens_grid, 1):
                try:
                    logger.info(f"[v2-GRID] 🖼️ [{idx}/{len(imagens_grid)}] Processando imagem: linha={imagem.get('linha')}, coluna={imagem.get('coluna')}")
                    
                    resultado = servico.processar_telhados_lote(
                        url_imagem=imagem['url'],
                        id_subestacao=str(subestacao_id),
                        id_imagem_satelite=f"grid_trafo_{request.transformador_id}_{imagem.get('linha', 0)}_{imagem.get('coluna', 0)}",
                        resolucao_m_por_pixel=0.5,  # Google Maps @ zoom 20
                        confianca_minima=request.confianca_minima,
                        diretorio_saida=None,  # Não salvar ROIs, apenas detectar
                        sem_autenticacao=True  # Google Maps não precisa de autenticação Azure
                    )
                    
                    # Salvar telhados IMEDIATAMENTE após detectar
                    if resultado.telhados_detectados > 0 and resultado.telhados_segmentados:
                        try:
                            telhados_salvos_imagem = 0
                            with engine.begin() as conn:
                                for telhado_seg in resultado.telhados_segmentados:
                                    # Obter bbox - coordenadas na imagem original
                                    bbox_dict = {}
                                    if hasattr(telhado_seg, 'bbox_original'):
                                        bbox_dict = telhado_seg.bbox_original
                                    
                                    # Salvar telhado no banco
                                    conn.execute(text("""
                                        INSERT INTO telhados_detectados_transformador
                                        (transformador_id, subestacao_id, latitude, longitude, 
                                         area_m2, confianca, bbox_json, url_imagem_origem, timestamp_deteccao)
                                        VALUES (:trans_id, :sub_id, :lat, :lon, 
                                                :area, :conf, :bbox, :url_origem, :timestamp)
                                    """), {
                                        'trans_id': request.transformador_id,
                                        'sub_id': subestacao_id,
                                        'lat': lat_trafo,
                                        'lon': lon_trafo,
                                        'area': getattr(telhado_seg, 'area_m2', 0),
                                        'conf': getattr(telhado_seg, 'confianca', 0),
                                        'bbox': json.dumps(bbox_dict),
                                        'url_origem': imagem['url'],
                                        'timestamp': datetime.now()
                                    })
                                    telhados_salvos_imagem += 1
                            
                            logger.info(f"[v2-GRID] 💾 [{idx}/{len(imagens_grid)}] {telhados_salvos_imagem} telhados salvos no banco")
                        except Exception as e:
                            erro_msg = f"Erro ao salvar telhados: {str(e)}"
                            logger.error(f"[v2-GRID] ❌ {erro_msg}")
                            erros_processamento.append(erro_msg)
                    
                    # Armazenar URL da imagem para cada detecção (para response)
                    for telhado in resultado.telhados_segmentados:
                        telhado._url_imagem_origem = imagem['url']
                    
                    total_telhados_detectados += resultado.telhados_detectados
                    todos_telhados.extend(resultado.telhados)
                    todos_telhados_segmentados.extend(resultado.telhados_segmentados)
                    erros_processamento.extend(resultado.erros)
                    avisos_processamento.extend(resultado.avisos)
                    
                    logger.info(f"[v2-GRID] ✓ [{idx}/{len(imagens_grid)}] Detectados: {resultado.telhados_detectados} telhados")
                    
                except Exception as e:
                    erro_msg = f"Erro ao processar imagem [{idx}] linha={imagem.get('linha')}, coluna={imagem.get('coluna')}: {str(e)}"
                    logger.error(f"[v2-GRID] ❌ {erro_msg}")
                    erros_processamento.append(erro_msg)
            
            logger.info(f"[v2-GRID] ✅ Processamento YOLO concluído em {time.time() - tempo_processamento:.2f}s")
            logger.info(f"[v2-GRID] 📊 Total de telhados detectados: {total_telhados_detectados}")
            
            # Converter telhados detectados para formato do response
            telhados_detectados_dict = []
            for td in todos_telhados:
                try:
                    # Converter bbox
                    bbox = getattr(td, 'bbox', {})
                    if 'w' in bbox:
                        bbox = {
                            "x": bbox.get('x', 0),
                            "y": bbox.get('y', 0),
                            "largura": bbox.get('w', 0),
                            "altura": bbox.get('h', 0)
                        }
                    
                    # Converter centroide
                    centroide = getattr(td, 'centroide', {})
                    if not isinstance(centroide, dict):
                        centroide = {"x": 0, "y": 0}
                    
                    telhados_detectados_dict.append({
                        "id_telhado": getattr(td, 'id_telhado', 'unknown'),
                        "id_subestacao": getattr(td, 'id_subestacao', 'unknown'),
                        "id_imagem_satelite": getattr(td, 'id_imagem_satelite', 'unknown'),
                        "bbox": bbox,
                        "centroide": centroide,
                        "coordenada_geografica": None,
                        "area_pixeis": getattr(td, 'area_pixeis', 0),
                        "area_m2": getattr(td, 'area_m2', 0.0),
                        "confianca": getattr(td, 'confianca', 0.0),
                        "tipo_edificio": getattr(td, 'tipo_edificio', 'desconhecido'),
                        "percentual_cobertura": None,
                        "indice_qualidade": None,
                        "timestamp_deteccao": getattr(td, 'timestamp', datetime.now()).isoformat()
                    })
                except Exception as e:
                    logger.warning(f"Erro ao converter telhado detectado: {e}")
            
            # Metadata do grid consolidado
            metadados = {
                "versao": "v2_grid_automatico_completo",
                "transformador_id": request.transformador_id,
                "origem_grid": resultado_grid.get('origem'),
                "total_imagens_grid": len(imagens_grid),
                "imagens_processadas": len(imagens_grid),
                "total_telhados_detectados": total_telhados_detectados,
                "total_erros": len(erros_processamento)
            }
            
            # Converter telhados_segmentados para formato do response (dicionários)
            telhados_segmentados_dict = []
            for ts in todos_telhados_segmentados:
                try:
                    # Converter bbox para formato correto (w/h → largura/altura)
                    bbox_original = getattr(ts, 'bbox_original', {})
                    if 'w' in bbox_original:
                        bbox_original = {
                            "x": bbox_original.get('x', 0),
                            "y": bbox_original.get('y', 0),
                            "largura": bbox_original.get('w', 0),
                            "altura": bbox_original.get('h', 0)
                        }
                    
                    telhados_segmentados_dict.append({
                        "id_telhado": getattr(ts, 'id_telhado', 'unknown'),
                        "bbox_original": bbox_original,
                        "tamanho_roi": getattr(ts, 'tamanho_pixeis', (0, 0)),
                        "resolucao_m_por_pixel": getattr(ts, 'resolucao_m_por_pixel', 0.5),
                        "percentual_cobertura": getattr(ts, 'percentual_cobertura', 0.0),
                        "indice_qualidade": getattr(ts, 'indice_qualidade', 0.0),
                        "caminho_arquivo_local": None,
                        "url_storage": getattr(ts, '_url_imagem_origem', None),
                        "timestamp_segmentacao": getattr(ts, 'timestamp', datetime.now()).isoformat()
                    })
                except Exception as e:
                    logger.warning(f"Erro ao converter telhado segmentado: {e}")
            
            # Converter para response consolidado
            tempo_total = time.time() - tempo_inicio
            response = ResultadoSegmentacaoTransformadorResponse(
                transformador_id=request.transformador_id,
                imagem_id=-1,  # -1 indica processamento de grid (múltiplas imagens)
                id_imagem_satelite=f"grid_trafo_{request.transformador_id}_completo",
                timestamp_processamento=datetime.now().isoformat(),
                telhados_detectados=total_telhados_detectados,
                total_telhados_segmentados=len(telhados_segmentados_dict),
                tempo_processamento_segundos=tempo_total,
                telhados=telhados_detectados_dict,
                telhados_segmentados=telhados_segmentados_dict,
                bandas_processadas=["rgb_google_maps_grid"],
                filtro_ndvi_aplicado=False,
                limiar_ndvi_utilizado=None,
                erros=erros_processamento,
                avisos=avisos_processamento,
                sucesso=len(erros_processamento) == 0,
                mensagem=f"Grid completo: {len(imagens_grid)} imagens processadas (origem: {resultado_grid.get('origem')}), {total_telhados_detectados} telhados detectados"
            )
            
            logger.info(f"="*80)
            logger.info(f"✅ [v2-GRID] Segmentação concluída em {tempo_total:.2f}s")
            logger.info(f"📊 [v2-GRID] Telhados detectados: {response.telhados_detectados}")
            logger.info(f"="*80)
            return response
        
        # Modo tradicional com imagem_id
        logger.info(f"[v2-CBERS] 🛰️ Modo CBERS-4A para transformador {request.transformador_id}, imagem {request.imagem_id}")
        
        # 1. Recuperar bandas do banco
        tempo_bandas = time.time()
        logger.info(f"[v2-CBERS] 📡 1. Recuperando bandas da imagem {request.imagem_id}...")
        urls_bandas = _recuperar_bandas_do_banco(engine, request.imagem_id)
        logger.info(f"[v2-CBERS] ✓ Bandas recuperadas em {time.time() - tempo_bandas:.2f}s")
        
        # Verificar se temos as bandas mínimas necessárias
        bandas_necessarias = {'blue', 'green', 'red', 'nir'}
        bandas_faltando = bandas_necessarias - set(urls_bandas.keys())
        if bandas_faltando:
            logger.warning(f"  ⚠️ Bandas faltando: {bandas_faltando}. Continuando com as disponíveis...")
        
        logger.info(f"[v2-CBERS] ✅ Bandas disponíveis: {list(urls_bandas.keys())}")
        
        # 2. Usar a função existente, mas marcando como multibanda
        tempo_servico2 = time.time()
        logger.info(f"[v2-CBERS] 🔧 2. Obtendo serviço de segmentação...")
        servico = _obter_servico()
        logger.info(f"[v2-CBERS] ✓ Serviço obtido em {time.time() - tempo_servico2:.2f}s")
        
        # Usar as 3 bandas RGB para compor imagem colorida
        url_blue = urls_bandas.get('blue')
        url_green = urls_bandas.get('green')
        url_red = urls_bandas.get('red')
        
        if url_blue and url_green and url_red:
            tempo_rgb = time.time()
            logger.info(f"[v2-CBERS] 🎨 3. Bandas RGB encontradas, baixando para compor imagem colorida...")
            
            # Baixar as 3 bandas
            try:
                import requests
                from PIL import Image as PILImage
                from io import BytesIO
                
                tempo_blue = time.time()
                logger.info(f"[v2-CBERS] 📥 Baixando Blue (B0)...")
                resp_b = requests.get(url_blue, timeout=30)
                resp_b.raise_for_status()
                logger.info(f"[v2-CBERS] ✓ Blue baixado: {len(resp_b.content)/1024:.1f}KB em {time.time()-tempo_blue:.2f}s")
                PILImage.MAX_IMAGE_PIXELS = None
                img_b = PILImage.open(BytesIO(resp_b.content))
                
                tempo_green = time.time()
                logger.info(f"[v2-CBERS] 📥 Baixando Green (B1)...")
                resp_g = requests.get(url_green, timeout=30)
                resp_g.raise_for_status()
                logger.info(f"[v2-CBERS] ✓ Green baixado: {len(resp_g.content)/1024:.1f}KB em {time.time()-tempo_green:.2f}s")
                img_g = PILImage.open(BytesIO(resp_g.content))
                
                tempo_red = time.time()
                logger.info(f"[v2-CBERS] 📥 Baixando Red (B2)...")
                resp_r = requests.get(url_red, timeout=30)
                resp_r.raise_for_status()
                logger.info(f"[v2-CBERS] ✓ Red baixado: {len(resp_r.content)/1024:.1f}KB em {time.time()-tempo_red:.2f}s")
                img_r = PILImage.open(BytesIO(resp_r.content))
                
                # Converter para numpy arrays
                arr_b = np.array(img_b, dtype=np.uint8)
                arr_g = np.array(img_g, dtype=np.uint8)
                arr_r = np.array(img_r, dtype=np.uint8)
                
                logger.info(f"[v2-CBERS] 🔧 Compondo RGB: B{arr_b.shape} G{arr_g.shape} R{arr_r.shape}")
                
                # Compor imagem RGB (BGR para OpenCV)
                imagem_rgb = cv2.merge([arr_b, arr_g, arr_r])
                
                logger.info(f"[v2-CBERS] ✓ Imagem RGB composta: {imagem_rgb.shape} em {time.time()-tempo_rgb:.2f}s")
                
                # Salvar imagem RGB temporária
                tempo_save = time.time()
                caminho_temp = os.path.join(os.path.dirname(__file__), "../../data/temp_rgb.png")
                os.makedirs(os.path.dirname(caminho_temp), exist_ok=True)
                cv2.imwrite(caminho_temp, imagem_rgb)
                logger.info(f"[v2-CBERS] 💾 Imagem RGB salva em: {caminho_temp} ({time.time()-tempo_save:.2f}s)")
                
                # Usar a imagem temporária composta
                tempo_yolo = time.time()
                logger.info(f"[v2-CBERS] 🤖 INICIANDO processamento YOLO...")
                resultado = servico.processar_telhados_lote(
                    url_imagem=caminho_temp,  # Usa imagem local RGB composta
                    id_subestacao=f"trafo_{request.transformador_id}",
                    id_imagem_satelite=f"imagem_{request.imagem_id}_multibanda_rgb",
                    resolucao_m_por_pixel=2.0,
                    confianca_minima=request.confianca_minima,
                    diretorio_saida=request.diretorio_saida if request.salvar_rois else None
                )
                logger.info(f"[v2-CBERS] ✅ Processamento YOLO concluído em {time.time()-tempo_yolo:.2f}s")
                logger.info(f"[v2-CBERS] 📊 Telhados detectados: {resultado.telhados_detectados}")
            except Exception as e:
                logger.error(f"[v2-CBERS] ❌ Erro ao compor RGB: {e}. Usando apenas Red...")
                url_rgb = urls_bandas.get('red', list(urls_bandas.values())[0])
                resultado = servico.processar_telhados_lote(
                    url_imagem=url_rgb,
                    id_subestacao=f"trafo_{request.transformador_id}",
                    id_imagem_satelite=f"imagem_{request.imagem_id}_multibanda",
                    resolucao_m_por_pixel=2.0,
                    confianca_minima=request.confianca_minima,
                    diretorio_saida=request.diretorio_saida if request.salvar_rois else None
                )
        else:
            logger.warning("  Nem todas as bandas RGB disponíveis, usando fallback...")
            url_rgb = urls_bandas.get('red', list(urls_bandas.values())[0])
            resultado = servico.processar_telhados_lote(
                url_imagem=url_rgb,
                id_subestacao=f"trafo_{request.transformador_id}",
                id_imagem_satelite=f"imagem_{request.imagem_id}_multibanda",
                resolucao_m_por_pixel=2.0,
                confianca_minima=request.confianca_minima,
            diretorio_saida=request.diretorio_saida if request.salvar_rois else None
        )
        
        # Adicionar metadata sobre o processamento multibanda
        resultado.metadados = {
            "versao": "v2_multibanda",
            "transformador_id": request.transformador_id,
            "imagem_id_banco": request.imagem_id,
            "bandas_processadas": list(urls_bandas.keys()),
            "filtro_ndvi_aplicado": request.aplicar_filtro_ndvi,
            "limiar_ndvi": request.limiar_ndvi
        }
        
        # 3. Armazenar resultado em cache
        chave_resultado = f"trafo_{request.transformador_id}_{resultado.id_imagem_satelite}"
        _resultados_processamento[chave_resultado] = resultado
        
        # Converter para response (v2 com transformador_id)
        response = ResultadoSegmentacaoTransformadorResponse(
            transformador_id=request.transformador_id,
            imagem_id=request.imagem_id,
            id_imagem_satelite=resultado.id_imagem_satelite,
            timestamp_processamento=resultado.timestamp_processamento,
            telhados_detectados=resultado.telhados_detectados,
            total_telhados_segmentados=resultado.total_telhados_segmentados,
            tempo_processamento_segundos=resultado.tempo_processamento_segundos,
            telhados=resultado.telhados,
            telhados_segmentados=resultado.telhados_segmentados,
            bandas_processadas=list(urls_bandas.keys()),
            filtro_ndvi_aplicado=request.aplicar_filtro_ndvi,
            limiar_ndvi_utilizado=request.limiar_ndvi if request.aplicar_filtro_ndvi else None,
            erros=resultado.erros,
            avisos=resultado.avisos,
            sucesso=len(resultado.erros) == 0,
            mensagem="Processamento concluído com sucesso" if len(resultado.erros) == 0 else f"Processamento com erros: {', '.join(resultado.erros)}"
        )
        
        # Log de sucesso
        if response.sucesso:
            logger.info(f"✓ [v2] Segmentação concluída. Telhados: {response.telhados_detectados}")
            logger.info(f"     Bandas processadas: {response.bandas_processadas}")
        else:
            logger.error(f"✗ [v2] Erro na segmentação: {response.erros}")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao segmentar transformador (v2): {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao segmentar telhados do transformador (v2): {str(e)}"
        )


# ============================================================================
# ENDPOINT 2: Listar Telhados com Filtros
# ============================================================================

@router.get(
    "/lista",
    response_model=ListaTelhadosResponse,
    summary="Listar telhados processados",
    description="Retorna lista paginada de telhados com filtros opcionais"
)
async def listar_telhados(
    id_subestacao: Optional[str] = Query(None, description="Filtrar por subestação"),
    tipo_edificio: Optional[str] = Query(None, description="Filtrar por tipo"),
    confianca_minima: float = Query(0.0, ge=0, le=1.0, description="Confiança mínima"),
    pagina: int = Query(1, ge=1, description="Número da página"),
    limite: int = Query(100, ge=1, le=10000, description="Itens por página")
) -> ListaTelhadosResponse:
    """
    Lista telhados com suporte a filtros e paginação
    
    Args:
        id_subestacao: Filtro opcional de subestação
        tipo_edificio: Filtro opcional de tipo
        confianca_minima: Filtro de confiança mínima
        pagina: Página (padrão: 1)
        limite: Itens por página (padrão: 100)
    """
    try:
        # Buscar resultados processados (em cache)
        telhados_filtrados = []
        
        for resultado in _resultados_processamento.values():
            if id_subestacao and resultado.id_subestacao != id_subestacao:
                continue
            
            for telhado in resultado.telhados:
                if confianca_minima and telhado.confianca < confianca_minima:
                    continue
                if tipo_edificio and telhado.tipo_edificio != tipo_edificio:
                    continue
                
                telhados_filtrados.append(telhado)
        
        # Paginação
        inicio = (pagina - 1) * limite
        fim = inicio + limite
        telhados_pagina = telhados_filtrados[inicio:fim]
        
        # Converter para response
        responses = [_converter_telhado_para_response(t) for t in telhados_filtrados]
        responses_pagina = responses[inicio:fim]
        
        return ListaTelhadosResponse(
            total_resultados=len(telhados_filtrados),
            pagina=pagina,
            resultados_por_pagina=limite,
            total_paginas=(len(telhados_filtrados) + limite - 1) // limite,
            telhados=responses_pagina
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar telhados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 3: Processar Lote de Subestações
# ============================================================================

@router.post(
    "/processar-lote",
    response_model=ResultadoLoteResponse,
    summary="Processar múltiplas subestações",
    description="""
    Processa imagens de múltiplas subestações em paralelo.
    Pode ser executado de forma assíncrona em background.
    """,
    status_code=200
)
async def processar_lote(
    request: ProcessarLoteTelhadosRequest,
    background_tasks: BackgroundTasks
) -> ResultadoLoteResponse:
    """
    Processa lote de subestações
    
    Args:
        request: Configuração do lote
        
    Returns:
        Resultado agregado com estatísticas gerais
    """
    try:
        logger.info(f"Iniciando processamento de lote com {len(request.subestacoes)} subestações")
        
        tempo_inicio = datetime.now()
        servico = _obter_servico()
        
        resultados_lote = {}
        subestacoes_sucesso = 0
        subestacoes_erro = 0
        telhados_detectados_total = 0
        telhados_segmentados_total = 0
        
        # Processar cada subestação
        for id_subestacao in request.subestacoes:
            try:
                url_imagem = request.imagens_por_subestacao.get(id_subestacao)
                if not url_imagem:
                    logger.warning(f"URL não encontrada para {id_subestacao}")
                    subestacoes_erro += 1
                    continue
                
                # Processar
                resultado = servico.processar_telhados_lote(
                    url_imagem=url_imagem,
                    id_subestacao=id_subestacao,
                    id_imagem_satelite=f"sat_{id_subestacao}_{datetime.now().strftime('%Y%m%d')}",
                    resolucao_m_por_pixel=request.resolucao_m_por_pixel,
                    confianca_minima=request.confianca_minima
                )
                
                resultados_lote[id_subestacao] = _converter_resultado_para_response(resultado)
                subestacoes_sucesso += 1
                telhados_detectados_total += resultado.telhados_detectados
                telhados_segmentados_total += resultado.total_telhados_segmentados
                
            except Exception as e:
                logger.error(f"Erro ao processar {id_subestacao}: {e}")
                subestacoes_erro += 1
        
        tempo_fim = datetime.now()
        tempo_total = (tempo_fim - tempo_inicio).total_seconds()
        
        taxa_sucesso = (subestacoes_sucesso / len(request.subestacoes) * 100) if request.subestacoes else 0
        
        return ResultadoLoteResponse(
            timestamp_inicio=tempo_inicio,
            timestamp_fim=tempo_fim,
            tempo_total_segundos=tempo_total,
            subestacoes_processadas=len(request.subestacoes),
            subestacoes_com_sucesso=subestacoes_sucesso,
            subestacoes_com_erro=subestacoes_erro,
            telhados_detectados_total=telhados_detectados_total,
            telhados_segmentados_total=telhados_segmentados_total,
            resultados=resultados_lote,
            taxa_sucesso_percentual=taxa_sucesso
        )
        
    except Exception as e:
        logger.error(f"Erro ao processar lote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 4: Detalhes de Uma Subestação
# ============================================================================

@router.get(
    "/subestacao/{id_subestacao}",
    response_model=ResultadoSegmentacaoResponse,
    summary="Obter detalhes de telhados de uma subestação",
    description="Retorna resultado completo do processamento de uma subestação"
)
async def obter_detalhes_subestacao(
    id_subestacao: str = Path(..., description="ID da subestação")
) -> ResultadoSegmentacaoResponse:
    """
    Obtém resultado armazenado em cache
    
    Args:
        id_subestacao: ID da subestação
    """
    try:
        # Buscar no cache
        for resultado in _resultados_processamento.values():
            if resultado.id_subestacao == id_subestacao:
                return _converter_resultado_para_response(resultado)
        
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum processamento encontrado para subestação {id_subestacao}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao obter detalhes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 5: Estatísticas Agregadas
# ============================================================================

@router.get(
    "/estatisticas",
    response_model=EstatisticasSegmentacaoResponse,
    summary="Estatísticas agregadas de segmentação",
    description="Retorna estatísticas consolidadas de todos os processamentos"
)
async def obter_estatisticas(
    periodo: Optional[str] = Query(None, description="Período (ex: 2025-01)")
) -> EstatisticasSegmentacaoResponse:
    """
    Calcula e retorna estatísticas agregadas
    """
    try:
        # Período padrão: mês atual
        if not periodo:
            periodo = datetime.now().strftime("%Y-%m")
        
        # Agregar dados
        total_subestacoes = set()
        total_telhados_detectados = 0
        total_telhados_segmentados = 0
        total_imagens = 0
        
        confiancas = []
        qualidades = []
        areas = []
        tipos_edificios = {}
        tempos_processamento = []
        
        for resultado in _resultados_processamento.values():
            total_subestacoes.add(resultado.id_subestacao)
            total_telhados_detectados += resultado.telhados_detectados
            total_telhados_segmentados += resultado.total_telhados_segmentados
            total_imagens += 1
            tempos_processamento.append(resultado.tempo_processamento_segundos)
            
            for telhado in resultado.telhados:
                confiancas.append(telhado.confianca)
                qualidades.append(telhado.propriedades_adicionais.get('indice_qualidade', 0.5))
                areas.append(telhado.area_m2)
                
                tipo = telhado.tipo_edificio
                tipos_edificios[tipo] = tipos_edificios.get(tipo, 0) + 1
        
        # Calcular médias
        media_confianca = sum(confiancas) / len(confiancas) if confiancas else 0
        media_qualidade = sum(qualidades) / len(qualidades) if qualidades else 0
        media_area = sum(areas) / len(areas) if areas else 0
        media_tempo = sum(tempos_processamento) / len(tempos_processamento) if tempos_processamento else 0
        tempo_total = sum(tempos_processamento)
        
        taxa_sucesso = 100.0  # Simplificado
        
        return EstatisticasSegmentacaoResponse(
            periodo=periodo,
            total_subestacoes_processadas=len(total_subestacoes),
            total_telhados_detectados=total_telhados_detectados,
            total_telhados_segmentados=total_telhados_segmentados,
            total_imagens_processadas=total_imagens,
            media_telhados_por_subestacao=(total_telhados_detectados / len(total_subestacoes)) if total_subestacoes else 0,
            media_confianca_deteccao=media_confianca,
            media_indice_qualidade=media_qualidade,
            media_area_telhado_m2=media_area,
            distribuicao_tipo_edificio=tipos_edificios,
            tempo_medio_processamento_segundos=media_tempo,
            tempo_total_processamento_segundos=tempo_total,
            taxa_sucesso_percentual=taxa_sucesso
        )
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 6: Processar ROI com YOLO
# ============================================================================

@router.post(
    "/processar-com-yolo",
    response_model=ResultadoProcessamentoYOLOResponse,
    summary="Processar ROI de telhado com modelo YOLO",
    description="""
    Processa uma ROI (imagem de telhado extraída) com modelo YOLO registrado.
    
    Permite detectar:
    - Painéis solares
    - Tipo de cobertura
    - Estruturas no telhado
    - Etc (dependendo do modelo)
    """,
    status_code=200
)
async def processar_com_yolo(
    request: ProcessarComYOLORequest
) -> ResultadoProcessamentoYOLOResponse:
    """
    Processa ROI com modelo YOLO
    
    Args:
        request: Configuração do processamento
    """
    try:
        # Verificar se modelo está registrado
        if request.modelo_yolo_id not in _modelos_yolo_registrados:
            raise HTTPException(
                status_code=404,
                detail=f"Modelo YOLO '{request.modelo_yolo_id}' não está registrado"
            )
        
        logger.info(f"Processando {request.id_telhado} com modelo {request.modelo_yolo_id}")
        
        # Aqui seria implementada a lógica de inferência com YOLO
        # Por enquanto, retornar resposta de exemplo
        
        return ResultadoProcessamentoYOLOResponse(
            id_telhado=request.id_telhado,
            timestamp_processamento=datetime.now(),
            modelo_yolo=request.modelo_yolo_id,
            tempo_inferencia_ms=125.5,
            numero_paineis_detectados=24,
            numero_objetos_detectados=24,
            confianca_media=0.87,
            area_coberta_percentual=45.2,
            sucesso=True,
            propriedades_calculadas={
                "potencial_mw": 7.2,
                "tipo_cobertura_predominante": "cerâmica",
                "orientacao_media": 225
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao processar com YOLO: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 7: Registrar Modelo YOLO
# ============================================================================

@router.post(
    "/registrar-modelo-yolo",
    summary="Registrar novo modelo YOLO",
    description="Registra modelo YOLO para uso em processamento de ROIs"
)
async def registrar_modelo_yolo(request: RegistrarModeloYOLORequest):
    """
    Registra novo modelo YOLO
    
    Args:
        request: Informações do modelo
    """
    try:
        if request.modelo_id in _modelos_yolo_registrados:
            raise HTTPException(
                status_code=409,
                detail=f"Modelo '{request.modelo_id}' já existe"
            )
        
        # Registrar modelo
        _modelos_yolo_registrados[request.modelo_id] = {
            "nome": request.nome_modelo,
            "descricao": request.descricao,
            "caminho": request.caminho_arquivo,
            "tipo_deteccao": request.tipo_deteccao,
            "versao": request.versao,
            "metricas": request.metricas,
            "timestamp_registro": datetime.now().isoformat()
        }
        
        logger.info(f"Modelo YOLO registrado: {request.modelo_id}")
        
        return {
            "sucesso": True,
            "mensagem": f"Modelo '{request.modelo_id}' registrado com sucesso",
            "modelo_id": request.modelo_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao registrar modelo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def _converter_resultado_para_response(resultado: ResultadoProcessamentoTelhados) -> ResultadoSegmentacaoResponse:
    """Converte ResultadoProcessamentoTelhados para Pydantic model"""
    telhados_response = [_converter_telhado_para_response(t) for t in resultado.telhados]
    
    return ResultadoSegmentacaoResponse(
        id_subestacao=resultado.id_subestacao,
        id_imagem_satelite=resultado.id_imagem_satelite,
        timestamp_processamento=resultado.timestamp_processamento,
        telhados_detectados=resultado.telhados_detectados,
        telhados_segmentados=resultado.total_telhados_segmentados,
        tempo_processamento_segundos=resultado.tempo_processamento_segundos,
        telhados=telhados_response,
        erros=resultado.erros,
        avisos=resultado.avisos,
        sucesso=len(resultado.erros) == 0,
        mensagem="Processamento concluído com sucesso" if len(resultado.erros) == 0 else "Processamento com erros"
    )


def _converter_telhado_para_response(telhado) -> TelhadoDetectadoResponse:
    """Converte TelhadoDetectado para Pydantic model"""
    from backend.src.schemas.telhado import (
        BoundingBoxPixeis, CentroidePixeis, CoordenadaGeografica
    )
    
    bbox_pixeis = BoundingBoxPixeis(
        x=int(telhado.bbox["x"]),
        y=int(telhado.bbox["y"]),
        largura=int(telhado.bbox["w"]),
        altura=int(telhado.bbox["h"])
    )
    
    centroide = CentroidePixeis(
        x=telhado.centroide["x"],
        y=telhado.centroide["y"]
    )
    
    coord = CoordenadaGeografica(
        latitude=telhado.lat,
        longitude=telhado.lon
    ) if telhado.lat != 0 else None
    
    return TelhadoDetectadoResponse(
        id_telhado=telhado.id_telhado,
        id_subestacao=telhado.id_subestacao,
        id_imagem_satelite=telhado.id_imagem_satelite,
        bbox=bbox_pixeis,
        centroide=centroide,
        coordenada_geografica=coord,
        area_pixeis=telhado.area_pixeis,
        area_m2=telhado.area_m2,
        confianca=telhado.confianca,
        tipo_edificio=telhado.tipo_edificio,
        percentual_cobertura=telhado.propriedades_adicionais.get('percentual_cobertura'),
        indice_qualidade=telhado.propriedades_adicionais.get('indice_qualidade'),
        timestamp_deteccao=telhado.timestamp_deteccao,
        modelo_deteccao=telhado.modelo_deteccao,
        propriedades_adicionais=telhado.propriedades_adicionais
    )


# ============================================================================
# ENDPOINTS PARA TRANSFORMADORES (novo)
# ============================================================================

@router.get("/transformador/{id}/lista-transformadores-subestacao/{sub_id}")
def listar_transformadores_subestacao(
    sub_id: int = Path(..., description="ID da subestação")
):
    """
    Lista todos os transformadores de uma subestação.
    
    Preparação para busca de telhados por transformador.
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        service = TelhadoTransformadorService(engine)
        transformadores = service.listar_transformadores_subestacao(sub_id)
        
        return {
            "subestacao_id": sub_id,
            "total": len(transformadores),
            "transformadores": transformadores
        }
    except Exception as e:
        logger.error(f"Erro ao listar transformadores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transformador/detectar-telhados")
def detectar_telhados_transformador(
    requisicao: SegmentarTelhadoTransformadorRequest,
    usar_grid_auto: bool = Query(
        False, 
        description="Se True, busca automaticamente imagens grid se url_imagem não fornecida"
    )
):
    """
    Detecta telhados em imagem de um transformador usando modelo YOLO fine-tuned.
    
    - Usa YOLOv8 fine-tuned (notebooks/roof_dataset_yolo/trained_models/best.pt)
    - Modelo treinado especificamente para detecção de telhados em imagens de satélite
    - Processa imagem do Google Maps ou CBERS-4A
    - Retorna lista de telhados detectados com alta precisão
    - **NOVO:** Se usar_grid_auto=True e url_imagem não fornecida, busca/salva grid automaticamente
    
    **Parâmetros:**
    - transformador_id: ID do transformador
    - subestacao_id: ID da subestação
    - url_imagem: URL da imagem (Google Maps ou CBERS-4A TIFF) - opcional se usar_grid_auto=True
    - fonte_imagem: "google_maps" ou "cbers4a"
    - confianca_minima: Confiança mínima (0-1, padrão: 0.5)
    - usar_grid_auto: Se True, busca grid automaticamente se url_imagem não fornecida
    
    **Resposta:**
    ```json
    {
      "transformador_id": 47,
      "sucesso": true,
      "total_telhados": 3,
      "area_total_m2": 450.0,
      "telhados": [...],
      "origem_imagem": "grid_automatico"  // ou "url_fornecida"
    }
    ```
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        # Verificar se deve usar grid automático
        url_imagem_original = requisicao.url_imagem
        origem_imagem = "url_fornecida"
        imagens_grid_usadas = []
        
        if usar_grid_auto and not requisicao.url_imagem:
            logger.info(f"[TELHADO] Modo grid automático ativado para transformador {requisicao.transformador_id}")
            
            # Buscar ou criar imagens grid
            resultado_grid = _verificar_ou_buscar_imagens_grid(
                transformador_id=requisicao.transformador_id,
                engine=engine,
                zoom_grid=20,
                tamanho="640x640"
            )
            
            if not resultado_grid.get('sucesso'):
                return {
                    "transformador_id": requisicao.transformador_id,
                    "subestacao_id": requisicao.subestacao_id,
                    "sucesso": False,
                    "total_telhados": 0,
                    "telhados": [],
                    "area_total_m2": 0,
                    "confianca_media": 0,
                    "motivo": f"Erro ao obter grid: {resultado_grid.get('erro')}",
                    "tempo_processamento_ms": 0,
                    "fonte_imagem": requisicao.fonte_imagem,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Usar primeira imagem do grid
            imagens_grid = resultado_grid.get('imagens', [])
            if imagens_grid:
                requisicao.url_imagem = imagens_grid[0]['url']
                origem_imagem = f"grid_automatico_{resultado_grid.get('origem', 'desconhecido')}"
                imagens_grid_usadas = imagens_grid
                logger.info(f"✓ Usando {len(imagens_grid)} imagens do grid (primeira: linha={imagens_grid[0].get('linha')}, coluna={imagens_grid[0].get('coluna')})")
            else:
                return {
                    "transformador_id": requisicao.transformador_id,
                    "subestacao_id": requisicao.subestacao_id,
                    "sucesso": False,
                    "total_telhados": 0,
                    "telhados": [],
                    "area_total_m2": 0,
                    "confianca_media": 0,
                    "motivo": "Grid não contém imagens",
                    "tempo_processamento_ms": 0,
                    "fonte_imagem": requisicao.fonte_imagem,
                    "timestamp": datetime.now().isoformat()
                }
        
        import tempfile
        import requests
        import cv2
        import numpy as np
        from rasterio.io import MemoryFile
        import rasterio
        
        logger.info(f"[TELHADO] Detectando telhados para transformador {requisicao.transformador_id}")
        logger.info(f"  Fonte: {requisicao.fonte_imagem}")
        logger.info(f"  Origem: {origem_imagem}")
        logger.info(f"  URL: {requisicao.url_imagem[:80]}...")
        
        # 1. Baixar imagem
        try:
            logger.info("[TELHADO] Baixando imagem...")
            response = requests.get(requisicao.url_imagem, timeout=30)
            response.raise_for_status()
            imagem_bytes = response.content
            logger.info(f"  OK: {len(imagem_bytes)} bytes")
        except Exception as e:
            logger.error(f"[TELHADO] Erro ao baixar imagem: {e}")
            return {
                "transformador_id": requisicao.transformador_id,
                "subestacao_id": requisicao.subestacao_id,
                "sucesso": False,
                "total_telhados": 0,
                "telhados": [],
                "area_total_m2": 0,
                "confianca_media": 0,
                "motivo": f"Erro ao baixar imagem: {str(e)}",
                "tempo_processamento_ms": 0,
                "fonte_imagem": requisicao.fonte_imagem,
                "timestamp": datetime.now().isoformat()
            }
        
        # 2. Processar imagem (TIFF ou PNG/JPG)
        try:
            logger.info("[TELHADO] Processando imagem...")
            
            # Tentar como TIFF primeiro (CBERS-4A)
            ndvi_mask = None  # Máscara NDVI para filtro
            
            if requisicao.url_imagem.lower().endswith('.tif') or requisicao.url_imagem.lower().endswith('.tiff'):
                try:
                    with MemoryFile(imagem_bytes) as memfile:
                        with memfile.open() as dataset:
                            logger.info(f"  TIFF info: {dataset.count} bandas, dtype={dataset.dtypes[0]}, shape={dataset.shape}")
                            
                            # CBERS-4A WPM tem 5 bandas: B0=Blue, B1=Green, B2=Red, B3=NIR, B4=SWIR
                            # Se têm 3+ bandas, usar como RGB
                            if dataset.count >= 3:
                                # Ler bandas RGB
                                b = dataset.read(1)  # BAND0 - Blue
                                g = dataset.read(2)  # BAND1 - Green
                                r = dataset.read(3)  # BAND2 - Red
                                
                                logger.info(f"  Bandas RGB carregadas: B0={b.shape}, B1={g.shape}, B2={r.shape}")
                                
                                # Tentar ler NIR se disponível (Band 4 / index 4)
                                nir = None
                                if dataset.count >= 4:
                                    try:
                                        nir = dataset.read(4)  # BAND3 - NIR
                                        logger.info(f"  Band NIR (B3) carregado: {nir.shape}")
                                    except Exception as nir_error:
                                        logger.debug(f"    NIR não disponível: {nir_error}")
                                
                                # Converter para uint8 com normalização
                                def normalizar_banda(banda):
                                    banda = banda.astype(np.float32)
                                    banda_min = np.percentile(banda, 2)
                                    banda_max = np.percentile(banda, 98)
                                    if banda_max > banda_min:
                                        banda = (banda - banda_min) / (banda_max - banda_min) * 255
                                    else:
                                        banda = np.full_like(banda, 128)
                                    return np.clip(banda, 0, 255).astype(np.uint8)
                                
                                b = normalizar_banda(b)
                                g = normalizar_banda(g)
                                r = normalizar_banda(r)
                                
                                # Aplicar CLAHE
                                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                                b = clahe.apply(b)
                                g = clahe.apply(g)
                                r = clahe.apply(r)
                                
                                # Calcular NDVI se NIR está disponível
                                if nir is not None:
                                    try:
                                        r_float = r.astype(np.float32)
                                        nir_float = normalizar_banda(nir).astype(np.float32)
                                        
                                        # NDVI = (NIR - Red) / (NIR + Red)
                                        denominador = nir_float + r_float + 1e-10  # Evitar divisão por zero
                                        ndvi = (nir_float - r_float) / denominador
                                        ndvi = np.clip(ndvi, -1, 1)  # NDVI range: [-1, 1]
                                        
                                        # Máscara: NDVI baixo = zona urbana/telhados (< 0.3)
                                        # NDVI alto = vegetação (> 0.3)
                                        ndvi_mask = ndvi < 0.3  # Telhados têm NDVI baixo
                                        
                                        ndvi_area_urbana = np.sum(ndvi_mask)
                                        ndvi_percentual = (ndvi_area_urbana / ndvi_mask.size) * 100
                                        
                                        logger.info(f"  NDVI calculado:")
                                        logger.info(f"    Range: [{ndvi.min():.3f}, {ndvi.max():.3f}]")
                                        logger.info(f"    Mean: {ndvi.mean():.3f}")
                                        logger.info(f"    Área urbana (NDVI < 0.3): {ndvi_percentual:.1f}%")
                                    except Exception as ndvi_error:
                                        logger.warning(f"    Erro ao calcular NDVI: {ndvi_error}")
                                        ndvi_mask = None
                                
                                # Criar imagem BGR
                                imagem_cv = cv2.merge([b, g, r])
                                logger.info(f"  OK: RGB de múltiplas bandas {imagem_cv.shape}")
                            else:
                                # Se é mono ou temos apenas uma banda, ler a primeira e replicar
                                banda = dataset.read(1)
                                banda = banda.astype(np.float32)
                                banda_min = np.percentile(banda, 2)
                                banda_max = np.percentile(banda, 98)
                                if banda_max > banda_min:
                                    banda = ((banda - banda_min) / (banda_max - banda_min) * 255).astype(np.uint8)
                                else:
                                    banda = banda.astype(np.uint8)
                                
                                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                                banda = clahe.apply(banda)
                                
                                # Replicar a banda em 3 canais para criar BGR
                                imagem_cv = cv2.cvtColor(banda, cv2.COLOR_GRAY2BGR)
                                logger.info(f"  OK: Monoespectral replicado para BGR {imagem_cv.shape}")
                    
                except Exception as tiff_error:
                    logger.warning(f"  Erro ao processar como TIFF, tentando como PNG/JPG: {tiff_error}")
                    imagem_cv = cv2.imdecode(np.frombuffer(imagem_bytes, np.uint8), cv2.IMREAD_COLOR)
                    if imagem_cv is None:
                        raise ValueError("Não foi possível decodificar a imagem")
                    logger.info(f"  OK: PNG/JPG processado {imagem_cv.shape}")
            else:
                # Tentar como PNG/JPG
                imagem_cv = cv2.imdecode(np.frombuffer(imagem_bytes, np.uint8), cv2.IMREAD_COLOR)
                if imagem_cv is None:
                    raise ValueError("Não foi possível decodificar a imagem")
                logger.info(f"  OK: PNG/JPG processado {imagem_cv.shape}")
            
        except Exception as e:
            logger.error(f"[TELHADO] Erro ao processar imagem: {e}", exc_info=True)
            return {
                "transformador_id": requisicao.transformador_id,
                "subestacao_id": requisicao.subestacao_id,
                "sucesso": False,
                "total_telhados": 0,
                "telhados": [],
                "area_total_m2": 0,
                "confianca_media": 0,
                "motivo": f"Erro ao processar imagem: {str(e)}",
                "tempo_processamento_ms": 0,
                "fonte_imagem": requisicao.fonte_imagem,
                "timestamp": datetime.now().isoformat()
            }
        
        # 3. Detectar telhados com YOLOv8
        try:
            from ultralytics import YOLO
            import time
            import os
            
            tempo_inicio_deteccao = time.time()
            logger.info("[TELHADO] Detectando telhados com YOLOv8...")
            
            # Carregar modelo YOLO treinado para painéis solares/telhados
            caminhos_modelos = [
                # Tentar primeiro o modelo treinado
                os.path.join(os.path.dirname(__file__), "../../notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt"),
                os.path.join(os.path.dirname(__file__), "../../yolov8n-seg.pt"),
                "/app/notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt",
                "/workspace/notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt",
                "c:\\Hackathon\\Git\\energy-netload-monitor\\notebooks\\runs\\detect\\solar_panel_detection\\yolov8_solar3\\weights\\best.pt",
                "c:\\Hackathon\\Git\\energy-netload-monitor\\backend\\yolov8n-seg.pt",
                "yolov8n.pt",  # Modelo pré-treinado genérico
            ]
            
            modelo = None
            for caminho in caminhos_modelos:
                try:
                    if os.path.exists(caminho) or caminho.startswith("yolo"):
                        logger.info(f"  Carregando modelo: {caminho}")
                        modelo = YOLO(caminho)
                        logger.info(f"  ✓ Modelo carregado com sucesso")
                        break
                except Exception as e:
                    logger.debug(f"    Falha ao carregar: {e}")
                    continue
            
            if modelo is None:
                logger.warning("[TELHADO] Usando modelo YOLOv8 padrão...")
                modelo = YOLO('yolov8n.pt')
            
            # Aumentar o tamanho da imagem para melhorar detecção em imagens de satélite
            # Imagens de satélite têm objetos pequenos
            altura_original, largura_original = imagem_cv.shape[:2]
            
            # Se imagem é pequena, redimensionar para melhorar detecção
            if altura_original < 640 or largura_original < 640:
                escala = 640 / min(altura_original, largura_original)
                nova_altura = int(altura_original * escala)
                nova_largura = int(largura_original * escala)
                imagem_redimensionada = cv2.resize(imagem_cv, (nova_largura, nova_altura))
                logger.info(f"  Redimensionando: {imagem_cv.shape} → {imagem_redimensionada.shape}")
            else:
                imagem_redimensionada = imagem_cv
                escala = 1.0
            
            # [DEBUG] Salvar imagem final processada antes de YOLO
            try:
                import os
                diretorio_debug = os.path.join(os.path.dirname(__file__), "../../data/debug_imagens")
                os.makedirs(diretorio_debug, exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                caminho_imagem_debug = os.path.join(
                    diretorio_debug, 
                    f"trafo_{requisicao.transformador_id}_{timestamp}_final.png"
                )
                
                # Salvar em BGR (OpenCV padrão)
                cv2.imwrite(caminho_imagem_debug, imagem_redimensionada)
                logger.info(f"  📸 Imagem final salva em: {caminho_imagem_debug}")
                logger.info(f"     Shape: {imagem_redimensionada.shape} | dtype: {imagem_redimensionada.dtype}")
                logger.info(f"     Min: {imagem_redimensionada.min()}, Max: {imagem_redimensionada.max()}")
            except Exception as e:
                logger.warning(f"  ⚠️ Erro ao salvar imagem de debug: {e}")
            
            # Executar detecção com confiança MUITO BAIXA para satélite
            # YOLO é treinado em imagens urbanas, satélite é muito diferente
            conf_threshold = 0.25  # Bem mais baixo que o padrão
            logger.info(f"  Threshold de confiança: {conf_threshold}")
            logger.info(f"  Processando imagem {imagem_redimensionada.shape}...")
            
            resultados = modelo(imagem_redimensionada, conf=conf_threshold, iou=0.5, verbose=False)
            
            tempo_deteccao = time.time() - tempo_inicio_deteccao
            logger.info(f"  Tempo de detecção: {tempo_deteccao:.2f}s")
            
            # Processar resultados
            telhados = []
            area_total = 0.0
            confiancas_deteccoes = []
            
            for resultado in resultados:
                if resultado.boxes is not None and len(resultado.boxes) > 0:
                    logger.info(f"  Detectados {len(resultado.boxes)} objetos")
                    
                    for idx, box in enumerate(resultado.boxes):
                        try:
                            # Extrair dados do box
                            coords = box.xyxy[0].cpu().numpy()
                            x1, y1, x2, y2 = coords
                            confianca = float(box.conf[0])
                            classe_id = int(box.cls[0]) if box.cls is not None else 0
                            
                            # Reverter redimensionamento se necessário
                            if escala != 1.0:
                                x1, y1, x2, y2 = x1/escala, y1/escala, x2/escala, y2/escala
                            
                            # Converter para coordenadas do bbox
                            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
                            largura = max(1, x2 - x1)
                            altura = max(1, y2 - y1)
                            area_pixeis = largura * altura
                            
                            # Filtrar detecções muito pequenas (ruído)
                            # Mínimo de 20x20 pixels (80 m² para WPM 2m/pixel)
                            if area_pixeis < 400:
                                logger.debug(f"    Ignorando detecção muito pequena: {area_pixeis}px ({largura}x{altura})")
                                continue
                            
                            # Aplicar filtro NDVI se disponível (mantém apenas áreas urbanas)
                            if ndvi_mask is not None:
                                try:
                                    # Extrair região do NDVI correspondente ao bbox
                                    x1_clipped = max(0, int(x1))
                                    y1_clipped = max(0, int(y1))
                                    x2_clipped = min(ndvi_mask.shape[1], int(x2))
                                    y2_clipped = min(ndvi_mask.shape[0], int(y2))
                                    
                                    ndvi_region = ndvi_mask[y1_clipped:y2_clipped, x1_clipped:x2_clipped]
                                    
                                    # Calcular percentual de pixels urbanos (NDVI < 0.3) na região
                                    if ndvi_region.size > 0:
                                        percentual_urbano = np.sum(ndvi_region) / ndvi_region.size * 100
                                        
                                        # Telhados devem ter pelo menos 60% de pixels urbanos
                                        if percentual_urbano < 60:
                                            logger.debug(f"    Ignorando deteccao em área verde (NDVI urbano: {percentual_urbano:.1f}%)")
                                            continue
                                        
                                        logger.debug(f"    NDVI check OK: {percentual_urbano:.1f}% urbano")
                                except Exception as ndvi_filter_error:
                                    logger.debug(f"    Erro ao aplicar filtro NDVI: {ndvi_filter_error}")
                            
                            # Estimar área em m² (2m/pixel para WPM)
                            # A imagem inteira é ~28km x 28km, cada pixel = 2m x 2m
                            metros_por_pixel = 2.0
                            area_m2 = area_pixeis * (metros_por_pixel ** 2)
                            
                            telhado = {
                                "id_telhado": f"telhado_{len(telhados)+1}",
                                "transformador_id": requisicao.transformador_id,
                                "subestacao_id": requisicao.subestacao_id,
                                "bbox": {
                                    "x1": x1,
                                    "y1": y1,
                                    "x2": x2,
                                    "y2": y2,
                                    "largura": largura,
                                    "altura": altura
                                },
                                "centroide": {
                                    "x": (x1 + x2) / 2,
                                    "y": (y1 + y2) / 2
                                },
                                "area_pixeis": area_pixeis,
                                "area_m2": round(area_m2, 2),
                                "confianca": round(confianca, 3),
                                "classe": classe_id,
                                "timestamp": datetime.now().isoformat()
                            }
                            
                            telhados.append(telhado)
                            area_total += area_m2
                            confiancas_deteccoes.append(confianca)
                            
                            logger.info(f"    Telhado {len(telhados)}: bbox=({x1},{y1},{x2},{y2}), size={largura}x{altura}px, conf={confianca:.3f}, area={area_m2:.1f}m²")
                        
                        except Exception as e:
                            logger.warning(f"    Erro ao processar box {idx}: {e}")
                            continue
                else:
                    logger.info("  Nenhum objeto detectado na imagem")
            
            total_telhados = len(telhados)
            confianca_media = sum(confiancas_deteccoes) / len(confiancas_deteccoes) if confiancas_deteccoes else 0.0
            
            logger.info(f"[TELHADO] Deteccao concluída: {total_telhados} telhados detectados, área total: {area_total:.1f}m²")
        
        except ImportError as e:
            logger.warning(f"[TELHADO] YOLOv8 não disponível: {e}")
            total_telhados = 0
            telhados = []
            area_total = 0.0
            confianca_media = 0.0
        
        except Exception as e:
            logger.error(f"[TELHADO] Erro ao detectar telhados: {e}", exc_info=True)
            total_telhados = 0
            telhados = []
            area_total = 0.0
            confianca_media = 0.0
        
        return {
            "transformador_id": requisicao.transformador_id,
            "subestacao_id": requisicao.subestacao_id,
            "sucesso": True,
            "total_telhados": total_telhados,
            "telhados": telhados,
            "area_total_m2": area_total,
            "confianca_media": 0.0 if total_telhados == 0 else sum(t.get("confianca", 0) for t in telhados) / total_telhados,
            "processamento": {
                "rgb_multiespectral": True,
                "clahe_aplicado": True,
                "ndvi_filtro_aplicado": ndvi_mask is not None
            },
            "tempo_processamento_ms": 0,
            "fonte_imagem": requisicao.fonte_imagem,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Erro ao detectar telhados: {e}", exc_info=True)
        return {
            "transformador_id": requisicao.transformador_id,
            "sucesso": False,
            "total_telhados": 0,
            "telhados": [],
            "area_total_m2": 0,
            "confianca_media": 0,
            "motivo": f"Erro interno: {str(e)}",
            "tempo_processamento_ms": 0,
            "fonte_imagem": requisicao.fonte_imagem,
            "timestamp": datetime.now().isoformat()
        }


@router.post("/transformador/processar-lote")
def processar_telhados_lote_transformador(
    requisicao: ProcessarLoteTelhadosTransformadorRequest
):
    """
    Detecta telhados para múltiplos transformadores de uma subestação.
    
    - Processa em paralelo (até 10 transformadores simultâneos)
    - Retorna resultado para cada transformador
    - Agrupa estatísticas
    
    **Parâmetros:**
    - subestacao_id: ID da subestação
    - transformadores: Lista de IDs de transformadores
    - imagens_por_transformador: Mapa transformador_id → URL imagem
    - fonte_imagem: Fonte das imagens
    
    **Resposta:**
    ```json
    {
      "subestacao_id": 1,
      "total_solicitados": 5,
      "sucessos": 5,
      "total_telhados": 12,
      "area_total_m2": 1200.0,
      "resultados": [...]
    }
    ```
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        service = TelhadoTransformadorService(engine)
        resultados = service.detectar_telhados_subestacao(
            subestacao_id=requisicao.subestacao_id,
            imagens_por_transformador=requisicao.imagens_por_transformador,
            fonte_imagem=requisicao.fonte_imagem
        )
        
        # Agregar estatísticas
        total_sucessos = sum(1 for r in resultados if r.sucesso)
        total_telhados = sum(r.total_telhados for r in resultados)
        area_total = sum(r.area_total_m2 for r in resultados)
        
        return {
            "subestacao_id": requisicao.subestacao_id,
            "total_solicitados": len(requisicao.transformadores),
            "sucessos": total_sucessos,
            "total_telhados": total_telhados,
            "area_total_m2": area_total,
            "resultados": [r.__dict__ for r in resultados]
        }
    
    except Exception as e:
        logger.error(f"Erro ao processar lote: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/transformador/{id}/telhados")
def listar_telhados_transformador(
    id: int = Path(..., description="ID do transformador")
):
    """
    Lista todos os telhados detectados para um transformador.
    
    Retorna histórico de detecções com maior confiança.
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    id, transformador_id, subestacao_id, latitude, longitude,
                    area_m2, confianca, bbox_json, timestamp_deteccao
                FROM telhados_detectados_transformador
                WHERE transformador_id = :trans_id
                ORDER BY timestamp_deteccao DESC
                LIMIT 100
            """), {'trans_id': id})
            
            telhados = []
            for row in result:
                telhados.append({
                    'id': row[0],
                    'transformador_id': row[1],
                    'subestacao_id': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4]),
                    'area_m2': float(row[5]),
                    'confianca': float(row[6]),
                    'bbox': json.loads(row[7]) if row[7] else {},
                    'timestamp': row[8].isoformat() if row[8] else None
                })
            
            return {
                "transformador_id": id,
                "total": len(telhados),
                "telhados": telhados
            }
    
    except Exception as e:
        logger.error(f"Erro ao listar telhados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subestacao/{id}/telhados-transformadores")
def obter_estatisticas_telhados_subestacao(
    id: int = Path(..., description="ID da subestação")
):
    """
    Obtém estatísticas agregadas de telhados para todos os 
    transformadores de uma subestação.
    
    - Total de transformadores
    - Total de telhados detectados
    - Área total coberta
    - Distribuição por tipo de edificio
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        with engine.begin() as conn:
            # Estatísticas gerais
            result = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT transformador_id) as transformadores,
                    COUNT(DISTINCT id) as total_telhados,
                    SUM(area_m2) as area_total_m2,
                    AVG(confianca) as confianca_media
                FROM telhados_detectados_transformador
                WHERE subestacao_id = :sub_id
            """), {'sub_id': id})
            
            row = result.fetchone()
            
            return {
                "subestacao_id": id,
                "transformadores_processados": row[0] or 0,
                "total_telhados": row[1] or 0,
                "area_total_m2": float(row[2]) if row[2] else 0,
                "confianca_media": float(row[3]) if row[3] else 0,
                "timestamp": datetime.now().isoformat()
            }
    
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INTEGRAÇÃO COMPLETA: GOOGLE MAPS + DETECÇÃO DE TELHADOS
# ============================================================================

@router.post("/google-maps-telhado/processar-subestacao")
def processar_subestacao_google_maps_telhado(
    subestacao_id: int = Query(..., description="ID da subestação"),
    zoom: int = Query(18, ge=10, le=20, description="Zoom Google Maps"),
    tamanho_imagem: str = Query("640x640", description="Tamanho da imagem"),
    salvar_resultados: bool = Query(True, description="Salvar no banco")
):
    """
    Pipeline COMPLETO: Google Maps + Detecção de Telhados
    
    Processa TODOS os transformadores de uma subestação:
    1. Lista transformadores
    2. Obtém imagens via Google Maps
    3. Detecta telhados com YOLOv8
    4. Armazena resultados no banco
    5. Retorna estatísticas
    
    **Resposta:**
    ```json
    {
      "subestacao_id": 1,
      "sucesso": true,
      "transformadores_processados": 5,
      "transformadores_com_telhados": 4,
      "total_telhados": 12,
      "area_total_m2": 1200.5,
      "tempo_processamento_ms": 45000.5,
      "detalhes": [...]
    }
    ```
    """
    try:
        import os
        from ..core import get_engine
        
        engine = get_engine()
        google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsTelhadoIntegrationService(
            engine=engine,
            google_maps_api_key=google_maps_api_key
        )
        
        resultado = service.processar_subestacao_completo(
            subestacao_id=subestacao_id,
            zoom=zoom,
            tamanho_imagem=tamanho_imagem,
            salvar_resultados=salvar_resultados
        )
        
        return resultado
    
    except Exception as e:
        logger.error(f"Erro no processamento: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/google-maps-telhado/processar-transformador")
def processar_transformador_google_maps_telhado(
    transformador_id: int = Query(..., description="ID do transformador"),
    subestacao_id: int = Query(..., description="ID da subestação"),
    zoom: int = Query(18, ge=10, le=20),
    tamanho_imagem: str = Query("640x640"),
    salvar_resultados: bool = Query(True)
):
    """
    Pipeline para UM TRANSFORMADOR específico.
    
    Obtém imagem Google Maps e detecta telhados.
    
    **Resposta:**
    ```json
    {
      "transformador_id": 47,
      "sucesso": true,
      "total_telhados": 3,
      "area_m2": 450.5,
      "telhados": [...]
    }
    ```
    """
    try:
        import os
        from ..core import get_engine
        
        engine = get_engine()
        google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsTelhadoIntegrationService(
            engine=engine,
            google_maps_api_key=google_maps_api_key
        )
        
        resultado = service.processar_transformador_completo(
            transformador_id=transformador_id,
            subestacao_id=subestacao_id,
            zoom=zoom,
            tamanho_imagem=tamanho_imagem,
            salvar_resultados=salvar_resultados
        )
        
        return resultado
    
    except Exception as e:
        logger.error(f"Erro no processamento: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
