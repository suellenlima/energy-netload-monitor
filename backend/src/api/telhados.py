"""Telhado API endpoints (DDD Architecture).

This module defines the HTTP API endpoints for roof detection operations.
It uses dependency injection to provide use cases and handles error conversion.
"""

import os
import time
import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
import requests
import numpy as np
import cv2
from PIL import Image
from io import BytesIO

from src.application.telhado import (
    CalcularPotencialSolarUseCase,
    GetTelhadoEstatisticasUseCase,
    ListarTelhadosPorAreaUseCase,
    ListarTelhadosPorConfiancaUseCase,
    ListarTelhadosPorOrientacaoUseCase,
    ListarTelhadosPorTransformadorUseCase,
    ListarTelhadosUseCase,
    ObtenerTelhadoUseCase,
)
from src.core import get_engine, get_settings
from src.domain.telhado import TelhadoNotFoundError
from src.infrastructure.mappers.telhado_mapper import TelhadoMapper
from src.infrastructure.persistence.telhado import SQLAlchemyTelhadoRepository
from src.infrastructure.persistence.telhado_multifonte import TelhadoMultiFonteRepository
from src.infrastructure.external.google_maps_grid_service import GoogleMapsGridService
from src.infrastructure.ml.roof_detection_service import RoofDetectionService
from src.schemas.telhado import DeteccaoTelhadoRequest, DeteccaoTelhadoResponse

router = APIRouter(prefix="/api/v1/telhados", tags=["telhados"])
logger = logging.getLogger(__name__)


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================


def get_repository():
    """Dependency: Telhado repository."""
    engine = get_engine()
    return SQLAlchemyTelhadoRepository(engine)


def get_obter_telhado_use_case(
    repository=Depends(get_repository),
) -> ObtenerTelhadoUseCase:
    """Dependency: Get telhado use case."""
    return ObtenerTelhadoUseCase(repository)


def get_listar_telhados_use_case(
    repository=Depends(get_repository),
) -> ListarTelhadosUseCase:
    """Dependency: List telhados use case."""
    return ListarTelhadosUseCase(repository)


def get_listar_por_transformador_use_case(
    repository=Depends(get_repository),
) -> ListarTelhadosPorTransformadorUseCase:
    """Dependency: List by transformer use case."""
    return ListarTelhadosPorTransformadorUseCase(repository)


def get_listar_por_confianca_use_case(
    repository=Depends(get_repository),
) -> ListarTelhadosPorConfiancaUseCase:
    """Dependency: List by confidence use case."""
    return ListarTelhadosPorConfiancaUseCase(repository)


def get_listar_por_area_use_case(
    repository=Depends(get_repository),
) -> ListarTelhadosPorAreaUseCase:
    """Dependency: List by area use case."""
    return ListarTelhadosPorAreaUseCase(repository)


def get_listar_por_orientacao_use_case(
    repository=Depends(get_repository),
) -> ListarTelhadosPorOrientacaoUseCase:
    """Dependency: List by orientation use case."""
    return ListarTelhadosPorOrientacaoUseCase(repository)


def get_estatisticas_use_case(
    repository=Depends(get_repository),
) -> GetTelhadoEstatisticasUseCase:
    """Dependency: Get statistics use case."""
    return GetTelhadoEstatisticasUseCase(repository)


def get_calcular_potencial_use_case(
    repository=Depends(get_repository),
) -> CalcularPotencialSolarUseCase:
    """Dependency: Calculate solar potential use case."""
    return CalcularPotencialSolarUseCase(repository)


# ============================================================================
# ERROR HANDLING
# ============================================================================


def handle_domain_error(error: TelhadoNotFoundError) -> HTTPException:
    """Convert domain errors to HTTP exceptions."""
    return HTTPException(status_code=404, detail=error.message)


# ============================================================================
# ENDPOINTS - Organized by specificity (specific routes first)
# ============================================================================

# DETECTION ENDPOINT (POST - specific action)
@router.post("/detectar", response_model=DeteccaoTelhadoResponse)
async def detectar_telhados_transformador(
    request: DeteccaoTelhadoRequest,
    background_tasks: BackgroundTasks = None
):
    """
    Detecta telhados usando imagens do Google Maps em grid para um transformador.
    
    Este endpoint:
    1. Busca as coordenadas do transformador no banco
    2. Gera um grid de URLs do Google Maps no zoom máximo
    3. Baixa cada imagem do grid
    4. Detecta telhados usando modelo YOLO treinado
    5. Salva os telhados detectados no banco de dados
    
    **Parâmetros:**
    - **transformador_id**: ID do transformador no banco
    - **confianca_minima**: Confiança mínima para detecção (0-1), default=0.5
    - **grid_size**: Tamanho do grid NxN (1-5), default=3 (grid 3x3 = 9 imagens)
    - **zoom**: Zoom do Google Maps (18-21), default=20 (máxima resolução ~0.6m/pixel)
    - **raio_metros**: Raio de cobertura em metros (50-1000), default=300
    - **salvar_debug**: Se True, salva imagens debug no disco
    
    **Retorna:**
    - Status da detecção
    - Número de telhados detectados
    - IDs dos telhados salvos no banco
    - Tempo de processamento
    - Erros e avisos
    """
    tempo_inicio = time.time()
    engine = get_engine()
    
    erros = []
    avisos = []
    telhados_salvos = []
    imagens_processadas = 0
    
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Iniciando detecção de telhados para transformador {request.transformador_id}")
        logger.info(f"Configuração: grid={request.grid_size}x{request.grid_size}, zoom={request.zoom}, confiança>={request.confianca_minima}")
        logger.info(f"{'='*80}\n")
        
        # ===================================================================
        # 1. BUSCAR DADOS DO TRANSFORMADOR
        # ===================================================================
        logger.info("[1/5] Buscando dados do transformador...")
        
        repository = TelhadoMultiFonteRepository(engine)
        transformador = repository.obter_transformador(request.transformador_id)
        
        if not transformador:
            raise HTTPException(
                status_code=404,
                detail=f"Transformador {request.transformador_id} não encontrado"
            )
        
        lat = transformador.get('latitude')
        lon = transformador.get('longitude')
        subestacao_codigo = transformador.get('subestacao_codigo')
        
        if not lat or not lon:
            raise HTTPException(
                status_code=400,
                detail=f"Transformador {request.transformador_id} não possui coordenadas válidas"
            )
        
        logger.info(f"✓ Transformador: {transformador.get('codigo')} ({lat}, {lon})")
        
        # Buscar subestacao_id
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM subestacoes_aneel WHERE codigo = :codigo LIMIT 1"),
                {"codigo": subestacao_codigo}
            )
            row = result.fetchone()
            subestacao_id = row[0] if row else None
        
        if not subestacao_id:
            avisos.append(f"Subestação {subestacao_codigo} não encontrada, usando ID padrão")
            subestacao_id = 1
        
        # ===================================================================
        # 2. GERAR GRID DE URLs DO GOOGLE MAPS
        # ===================================================================
        logger.info("[2/5] Gerando grid de URLs do Google Maps...")
        
        settings = get_settings()
        google_api_key = settings.google_maps_api_key
        if not google_api_key:
            avisos.append("GOOGLE_MAPS_API_KEY não configurada, URLs sem autenticação")
        
        grid_service = GoogleMapsGridService(api_key=google_api_key)
        grid_data = grid_service.gerar_grid_urls(
            lat_centro=lat,
            lon_centro=lon,
            grid_size=request.grid_size,
            zoom=request.zoom,
            raio_metros=request.raio_metros
        )
        
        logger.info(f"✓ Grid gerado: {len(grid_data)} pontos")
        
        cobertura = grid_service.estimar_cobertura_area(
            request.grid_size,
            request.zoom
        )
        logger.info(f"  Área estimada: {cobertura['area_total_coberta_m2']:.0f} m² ({cobertura['area_total_coberta_km2']:.3f} km²)")
        
        # ===================================================================
        # 3. INICIALIZAR SERVIÇO DE DETECÇÃO YOLO
        # ===================================================================
        logger.info("[3/5] Inicializando modelo YOLO...")
        
        # Usar modelo fine-tuned para detecção de telhados
        modelo_path = r"C:\Hackathon\Git\energy-netload-monitor\notebooks\runs\detect\runs\roof_detection\yolov8n_finetuned\weights\best.pt"
        detection_service = RoofDetectionService(model_path=modelo_path, use_gpu=True)
        
        logger.info(f"✓ Modelo carregado: {modelo_path}")
        
        # ===================================================================
        # 4. PROCESSAR CADA IMAGEM DO GRID
        # ===================================================================
        logger.info(f"[4/5] Processando {len(grid_data)} imagens do grid...")
        
        todos_telhados = []
        
        for idx, ponto in enumerate(grid_data):
            try:
                logger.info(f"\n  Imagem {idx+1}/{len(grid_data)} (row={ponto['row']}, col={ponto['col']})")
                logger.info(f"  Coords: ({ponto['lat']:.6f}, {ponto['lon']:.6f})")
                
                # Baixar imagem
                response = requests.get(ponto['url'], timeout=30)
                response.raise_for_status()
                
                # Converter para numpy array
                img_pil = Image.open(BytesIO(response.content))
                img_array = np.array(img_pil.convert('RGB'))
                
                # Detectar telhados
                deteccoes = detection_service.detectar_telhados(
                    img_array,
                    confianca_minima=request.confianca_minima
                )
                
                logger.info(f"  ✓ Detectados: {len(deteccoes)} telhados")
                
                # Converter coordenadas de pixel para lat/lon
                for det in deteccoes:
                    # Calcular posição relativa na imagem (0-1)
                    cx_norm = det['centroide']['x'] / 640.0
                    cy_norm = det['centroide']['y'] / 640.0
                    
                    # Calcular offset em metros (imagem é 640x640 pixels)
                    resolucao = ponto['resolucao_m_pixel']
                    largura_m = 640 * resolucao
                    
                    offset_x_m = (cx_norm - 0.5) * largura_m
                    offset_y_m = (0.5 - cy_norm) * largura_m  # Y invertido
                    
                    # Converter para lat/lon
                    telhado_lat = ponto['lat'] + (offset_y_m / 1000.0 / 111.0)
                    lon_per_km = 111.0 * np.cos(np.radians(ponto['lat']))
                    telhado_lon = ponto['lon'] + (offset_x_m / 1000.0 / lon_per_km)
                    
                    # Calcular área em m²
                    area_m2 = det['area_pixeis'] * (resolucao ** 2)
                    
                    todos_telhados.append({
                        'transformador_id': request.transformador_id,
                        'subestacao_id': subestacao_id,
                        'latitude': telhado_lat,
                        'longitude': telhado_lon,
                        'area_m2': area_m2,
                        'confianca': det['confianca'],
                        'bbox_json': det['bbox'],
                        'fonte_imagem': 'google_maps',
                        'resolucao_cm': resolucao * 100,
                        'url_imagem_origem': ponto['url']
                    })
                
                imagens_processadas += 1
                
            except Exception as e:
                erro_msg = f"Erro ao processar imagem {idx+1}: {str(e)}"
                logger.error(erro_msg)
                erros.append(erro_msg)
                continue
        
        logger.info(f"\n✓ Total de telhados detectados: {len(todos_telhados)}")
        
        # ===================================================================
        # 5. SALVAR TELHADOS NO BANCO DE DADOS
        # ===================================================================
        logger.info("[5/5] Salvando telhados no banco de dados...")
        
        if todos_telhados:
            with engine.begin() as conn:
                for telhado in todos_telhados:
                    # Converter bbox para JSON string
                    import json
                    bbox_json_str = json.dumps(telhado['bbox_json'])
                    
                    result = conn.execute(
                        text("""
                            INSERT INTO telhados_detectados_transformador 
                            (transformador_id, subestacao_id, latitude, longitude, area_m2, 
                             confianca, bbox_json, fonte_imagem, resolucao_cm, url_imagem_origem,
                             timestamp_deteccao)
                            VALUES 
                            (:transformador_id, :subestacao_id, :latitude, :longitude, :area_m2,
                             :confianca, CAST(:bbox_json AS jsonb), :fonte_imagem, :resolucao_cm, :url_imagem_origem,
                             NOW())
                            RETURNING id
                        """),
                        {
                            'transformador_id': telhado['transformador_id'],
                            'subestacao_id': telhado['subestacao_id'],
                            'latitude': telhado['latitude'],
                            'longitude': telhado['longitude'],
                            'area_m2': telhado['area_m2'],
                            'confianca': telhado['confianca'],
                            'bbox_json': bbox_json_str,
                            'fonte_imagem': telhado['fonte_imagem'],
                            'resolucao_cm': telhado['resolucao_cm'],
                            'url_imagem_origem': telhado['url_imagem_origem']
                        }
                    )
                    telhado_id = result.scalar()
                    telhados_salvos.append(telhado_id)
            
            logger.info(f"✓ {len(telhados_salvos)} telhados salvos no banco")
        else:
            avisos.append("Nenhum telhado detectado nas imagens processadas")
        
        # ===================================================================
        # FINALIZAR
        # ===================================================================
        tempo_total = time.time() - tempo_inicio
        
        logger.info(f"\n{'='*80}")
        logger.info("DETECÇÃO CONCLUÍDA")
        logger.info(f"Tempo total: {tempo_total:.2f}s")
        logger.info(f"Imagens processadas: {imagens_processadas}/{len(grid_data)}")
        logger.info(f"Telhados detectados: {len(telhados_salvos)}")
        logger.info(f"{'='*80}\n")
        
        return DeteccaoTelhadoResponse(
            sucesso=len(telhados_salvos) > 0,
            transformador_id=request.transformador_id,
            telhados_detectados=len(telhados_salvos),
            imagens_processadas=imagens_processadas,
            tempo_processamento_s=round(tempo_total, 2),
            telhados_salvos=telhados_salvos,
            erros=erros,
            avisos=avisos,
            detalhes={
                'transformador_codigo': transformador.get('codigo'),
                'grid_size': request.grid_size,
                'zoom': request.zoom,
                'area_coberta_m2': cobertura['area_total_coberta_m2'],
                'resolucao_m_pixel': cobertura['resolucao_m_pixel'],
                'coordenadas_centro': {'lat': lat, 'lon': lon}
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro crítico na detecção: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno ao processar detecção: {str(e)}"
        )


# SPECIFIC STAT ROUTES (must come before /{id})
@router.get("/stats", response_model=dict)
def get_stats(
    use_case: GetTelhadoEstatisticasUseCase = Depends(get_estatisticas_use_case),
):
    """
    Get statistics about all roofs.

    Returns:
    - **total**: Total count
    - **area_media_m2**: Average area in m²
    - **area_maxima_m2**: Maximum area in m²
    - **area_minima_m2**: Minimum area in m²
    - **confianca_media**: Average detection confidence
    - **alta_confianca_count**: Count of high confidence roofs
    - **com_transformador_count**: Roofs with transformer assigned
    """
    try:
        stats = use_case.execute()
        return {"status": "success", "data": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/confianca/alta", response_model=dict)
def list_high_confidence(
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTelhadosPorConfiancaUseCase = Depends(
        get_listar_por_confianca_use_case
    ),
):
    """
    List roofs with high confidence detection (>80%).

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    try:
        telhados, total = use_case.execute(
            min_confianca=0.8, limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [TelhadoMapper.to_list_response(t) for t in telhados],
                "total": total,
                "pagina": pagina,
                "limite": limite,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/area/buscar", response_model=dict)
def search_by_area(
    min_area: float = Query(..., ge=0, description="Minimum area in m²"),
    max_area: float = Query(..., ge=0, description="Maximum area in m²"),
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTelhadosPorAreaUseCase = Depends(get_listar_por_area_use_case),
):
    """
    Search roofs within area range.

    Query parameters:
    - **min_area**: Minimum area in m²
    - **max_area**: Maximum area in m²
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    if min_area > max_area:
        raise HTTPException(
            status_code=400, detail="min_area must be less than max_area"
        )

    try:
        telhados, total = use_case.execute(
            min_area=min_area, max_area=max_area, limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [TelhadoMapper.to_list_response(t) for t in telhados],
                "total": total,
                "area_range": {"min": min_area, "max": max_area},
                "pagina": pagina,
                "limite": limite,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/orientacao/{orientacao}", response_model=dict)
def list_by_orientation(
    orientacao: str,
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTelhadosPorOrientacaoUseCase = Depends(
        get_listar_por_orientacao_use_case
    ),
):
    """
    List roofs with specific orientation.

    Path parameters:
    - **orientacao**: Orientation (N, NE, E, SE, S, SW, W, NW)

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    valid_orientations = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
    if orientacao.upper() not in valid_orientations:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid orientation. Must be one of {valid_orientations}",
        )

    try:
        telhados, total = use_case.execute(
            orientacao=orientacao.upper(), limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [TelhadoMapper.to_list_response(t) for t in telhados],
                "total": total,
                "orientacao": orientacao.upper(),
                "pagina": pagina,
                "limite": limite,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/transformador/{transformador_id}", response_model=dict)
def list_by_transformer(
    transformador_id: int,
    limite: int = Query(100, ge=1, le=1000),
    use_case: ListarTelhadosPorTransformadorUseCase = Depends(
        get_listar_por_transformador_use_case
    ),
):
    """
    List roofs for a specific transformer.

    Path parameters:
    - **transformador_id**: Transformer ID

    Query parameters:
    - **limite**: Max results (1-1000, default: 100)
    """
    try:
        telhados = use_case.execute(transformador_id=transformador_id, limite=limite)

        return {
            "status": "success",
            "data": {
                "items": [TelhadoMapper.to_list_response(t) for t in telhados],
                "total": len(telhados),
                "transformador_id": transformador_id,
                "limite": limite,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ID-BASED ROUTES (must come AFTER prefix-based routes)
@router.get("/{id}", response_model=dict)
def get_telhado_detail(
    id: int,
    use_case: ObtenerTelhadoUseCase = Depends(get_obter_telhado_use_case),
):
    """
    Get detailed information about a roof.

    Returns:
    - **id**: Roof ID
    - **codigo**: Detection code
    - **latitude/longitude**: Coordinates
    - **area_m2**: Area in square meters
    - **inclinacao_graus**: Inclination angle
    - **orientacao**: Cardinal orientation
    - **confianca_deteccao**: Detection confidence (0-1)
    - **eh_alta_confianca**: Is high confidence (>80%)
    - **transformador_id**: Associated transformer ID (if any)
    """
    try:
        telhado = use_case.execute(id)
        return {
            "status": "success",
            "data": TelhadoMapper.to_detail_response(telhado),
        }
    except TelhadoNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/potencial-solar", response_model=dict)
def get_solar_potential(
    id: int,
    use_case: CalcularPotencialSolarUseCase = Depends(get_calcular_potencial_use_case),
):
    """
    Calculate solar power potential for a roof.

    Returns:
    - **telhado_id**: Roof ID
    - **area_m2**: Roof area
    - **orientacao**: Orientation
    - **inclinacao_graus**: Inclination
    - **potencia_estimada_kw**: Estimated power in kW
    - **confianca_deteccao**: Detection confidence
    """
    try:
        potencial = use_case.execute(id)
        return {"status": "success", "data": potencial}
    except TelhadoNotFoundError as e:
        raise handle_domain_error(e)


# GENERIC ROUTES (must come LAST)
@router.get("", response_model=dict)
def list_telhados(
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTelhadosUseCase = Depends(get_listar_telhados_use_case),
):
    """
    List all roofs with pagination.

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)

    Returns:
    - **items**: List of roofs
    - **total**: Total count
    - **pagina**: Current page
    - **limite**: Results per page
    """
    try:
        telhados, total = use_case.execute(limite=limite, pagina=pagina)

        return {
            "status": "success",
            "data": {
                "items": [TelhadoMapper.to_list_response(t) for t in telhados],
                "total": total,
                "pagina": pagina,
                "limite": limite,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
