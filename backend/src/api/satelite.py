"""
Endpoints para gerenciar dados de imagens de satélite de subestações.
Integra com INPE (CBERS-4A) e plataformas STAC para obter imagens de satélite.

MUDANÇA IMPORTANTE: Agora usa CBERS-4A do INPE como fonte principal!
- Resolução: 2 metros (muito melhor que Sentinel-2 com 10m)
- Gratuito e brasileiro
- Adequado para detecção de telhados
"""

import logging
import os
from typing import Optional, Annotated
from datetime import datetime, timedelta

from fastapi import APIRouter, Query, Depends

from ..core import DatabaseError, get_engine
from ..schemas import (
    BoundingBoxModel,
    ConsultaSateliteRequest,
    DadosSatelliteSubestacao,
    ListaImagensSatelite,
    RegistrarImagemRequest,
    RegistrarImagemResponse,
)
from ..services.inpe_satellite_service import INPESatelliteService
from ..services.cbers_service import CBERSService, ImagemCBERS
from .deps import EngineDepends, LimiteQuery

# Import STAC dependencies com tratamento de erro
try:
    from pystac_client import Client
    import planetary_computer
    HAS_STAC = True
except ImportError:
    HAS_STAC = False
    logger = logging.getLogger(__name__)
    logger.warning("pystac-client e planetary-computer não estão instalados")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/satelite", tags=["Satélite"])


@router.get(
    "/subestacao/{subestacao_id}/coordenadas",
    response_model=dict,
    summary="Obter coordenadas e área de subestação"
)
def get_coordenadas_subestacao(
    subestacao_id: int,
    engine: EngineDepends,
    raio_km: float = Query(
        default=5.0,
        ge=0.5,
        le=50.0,
        description="Área poligonal de cobertura em km (bounding box)"
    )
):
    """
    Retorna as coordenadas e bounding box de uma subestação.
    
    - **subestacao_id**: ID da subestação
    - **raio_km**: Área poligonal de cobertura em km (baseada em bbox, não circular)
    
    **Exemplo de resposta:**
    ```json
    {
        "subestacao": {
            "id": 1,
            "nome": "SE_DETECTADA_0",
            "latitude": -19.925,
            "longitude": -43.938,
            "distribuidora": "CEMIG DISTRIBUICAO S.A"
        },
        "bbox": {
            "min_lat": -19.970,
            "max_lat": -19.880,
            "min_lon": -43.983,
            "max_lon": -43.893,
            "center": {
                "latitude": -19.925,
                "longitude": -43.938
            },
            "dimensoes": {
                "largura_km": 10.0,
                "altura_km": 10.0
            }
        }
    }
    ```
    """
    try:
        service = INPESatelliteService(logger=logger)
        resultado = service.consultar_subestacao_satellite_data(
            engine=engine,
            subestacao_id=subestacao_id,
            raio_km=raio_km
        )
        
        if "erro" in resultado:
            raise DatabaseError(resultado["erro"])
        
        return resultado
    
    except Exception as exc:
        logger.error(
            f"Erro ao buscar coordenadas da subestação {subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            f"Falha ao buscar coordenadas da subestação"
        ) from exc


@router.get(
    "/subestacao/{subestacao_id}/imagens",
    response_model=ListaImagensSatelite,
    summary="Listar imagens de satélite de uma subestação"
)
def listar_imagens_subestacao(
    subestacao_id: int,
    engine: EngineDepends,
    limite: LimiteQuery = 50,
    ordenar_por: str = Query(
        default="data_aquisicao",
        description="Campo para ordenação (data_aquisicao, sensor, cobertura_nuvem_pct)"
    )
):
    """
    Lista todas as imagens de satélite registradas para uma subestação.
    
    - **subestacao_id**: ID da subestação
    - **limite**: Máximo de registros (default 50)
    - **ordenar_por**: Campo para ordenação (default data_aquisicao DESC)
    
    **Exemplo de resposta:**
    ```json
    {
        "subestacao_id": 1,
        "subestacao_nome": "SE_DETECTADA_0",
        "total_imagens": 3,
        "imagens": [
            {
                "id": "S2A_MSIL2A_20260101T131241_N0500_R031_T23KPA_20260101T134032",
                "sensor": "Sentinel-2",
                "data_aquisicao": "2026-01-01T13:12:41",
                "resolucao_m": 10,
                "cobertura_nuvem_pct": 12.5,
                "url": "https://...",
                "propriedades": {}
            }
        ]
    }
    ```
    """
    try:
        service = INPESatelliteService(logger=logger)
        imagens = service.listar_imagens_subestacao(
            engine=engine,
            subestacao_id=subestacao_id,
            limite=limite,
            ordenar_por=ordenar_por
        )
        
        return ListaImagensSatelite(
            subestacao_id=subestacao_id,
            total_imagens=len(imagens),
            imagens=imagens
        )
    
    except Exception as exc:
        logger.error(
            f"Erro ao listar imagens da subestação {subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            "Falha ao listar imagens de satélite"
        ) from exc


@router.post(
    "/subestacao/{subestacao_id}/registrar-imagem",
    response_model=RegistrarImagemResponse,
    summary="Registrar imagem de satélite"
)
def registrar_imagem_subestacao(
    subestacao_id: int,
    imagem: RegistrarImagemRequest,
    engine: EngineDepends
):
    """
    Registra metadados de uma imagem de satélite para uma subestação.
    
    - **subestacao_id**: ID da subestação
    - **imagem**: Dados da imagem
    
    **Exemplo de request:**
    ```json
    {
        "sensor": "Sentinel-2",
        "data_aquisicao": "2026-01-01T13:12:41",
        "resolucao_m": 10,
        "cobertura_nuvem_pct": 12.5,
        "url": "https://...",
        "propriedades": {
            "tile": "23KPA",
            "nivel_processamento": "L2A"
        }
    }
    ```
    """
    try:
        from datetime import datetime as dt
        from ..services.inpe_satellite_service import SatelliteMetadata, BoundingBox
        
        service = INPESatelliteService(logger=logger)
        
        # Criar metadata
        metadata = SatelliteMetadata(
            id=f"{imagem.sensor}_{imagem.data_aquisicao.timestamp()}",
            data_aquisicao=imagem.data_aquisicao,
            sensor=imagem.sensor,
            resolucao_m=imagem.resolucao_m,
            cobertura_nuvem_pct=imagem.cobertura_nuvem_pct,
            url=imagem.url,
            bounding_box=BoundingBox(0, 0, 0, 0),  # Será preenchido do banco
            propriedades=imagem.propriedades
        )
        
        sucesso = service.armazenar_metadata_imagem(
            engine=engine,
            subestacao_id=subestacao_id,
            metadata=metadata
        )
        
        if sucesso:
            return RegistrarImagemResponse(
                status="sucesso",
                mensagem="Imagem registrada com sucesso",
                imagem_id=metadata.id
            )
        else:
            return RegistrarImagemResponse(
                status="erro",
                mensagem="Falha ao registrar imagem",
                imagem_id=None
            )
    
    except Exception as exc:
        logger.error(
            f"Erro ao registrar imagem para subestação {subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            "Falha ao registrar imagem"
        ) from exc


@router.post(
    "/consultar-disponibilidade",
    response_model=DadosSatelliteSubestacao,
    summary="Consultar disponibilidade de imagens de satélite"
)
def consultar_disponibilidade_satelite(
    consulta: ConsultaSateliteRequest,
    engine: EngineDepends
):
    """
    Consulta a disponibilidade de imagens de satélite para uma subestação.
    Retorna URLs para consultar dados em diferentes plataformas (STAC, WMS).
    
    **Exemplo de request:**
    ```json
    {
        "subestacao_id": 1,
        "data_inicio": "2025-12-01T00:00:00",
        "data_fim": "2026-01-29T23:59:59",
        "raio_km": 5.0,
        "sensores": ["Sentinel-2", "Landsat"]
    }
    ```
    
    **Retorna:** URLs para consultar cada sensor, juntamente com bounding box e metadados.
    """
    try:
        service = INPESatelliteService(logger=logger)
        resultado = service.consultar_subestacao_satellite_data(
            engine=engine,
            subestacao_id=consulta.subestacao_id,
            data_inicio=consulta.data_inicio,
            data_fim=consulta.data_fim,
            raio_km=consulta.raio_km,
            sensores=consulta.sensores
        )
        
        if "erro" in resultado:
            raise DatabaseError(resultado["erro"])
        
        return DadosSatelliteSubestacao(**resultado)
    
    except Exception as exc:
        logger.error(
            f"Erro ao consultar disponibilidade para subestação "
            f"{consulta.subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            "Falha ao consultar disponibilidade de satélite"
        ) from exc


@router.get(
    "/bbox/{subestacao_id}",
    response_model=BoundingBoxModel,
    summary="Obter bounding box de uma subestação"
)
def get_bounding_box_subestacao(
    subestacao_id: int,
    engine: EngineDepends,
    raio_km: float = Query(
        default=5.0,
        ge=0.5,
        le=50.0,
        description="Área poligonal de cobertura em km (bounding box)"
    )
):
    """
    Retorna apenas a bounding box (retângulo geográfico) de uma subestação.
    Útil para recortes de imagens e consultas de APIs.
    
    - **subestacao_id**: ID da subestação
    - **raio_km**: Área poligonal de cobertura em km (baseada em bbox)
    
    **Exemplo de resposta:**
    ```json
    {
        "min_lat": -19.970,
        "max_lat": -19.880,
        "min_lon": -43.983,
        "max_lon": -43.893,
        "center": {
            "latitude": -19.925,
            "longitude": -43.938
        },
        "dimensoes": {
            "largura_km": 10.0,
            "altura_km": 10.0
        }
    }
    ```
    """
    try:
        from sqlalchemy import text
        
        # Buscar subestação
        query = text("""
            SELECT latitude, longitude
            FROM subestacoes_detectadas
            WHERE id = :id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"id": subestacao_id}).fetchone()
        
        if not result:
            raise DatabaseError("Subestação não encontrada")
        
        latitude, longitude = result
        
        service = INPESatelliteService(logger=logger)
        bbox = service.calcular_bbox_subestacao(latitude, longitude, raio_km)
        
        return BoundingBoxModel(
            min_lat=bbox.min_lat,
            max_lat=bbox.max_lat,
            min_lon=bbox.min_lon,
            max_lon=bbox.max_lon,
            center={
                "latitude": bbox.center_lat,
                "longitude": bbox.center_lon
            },
            dimensoes={
                "largura_km": bbox.width_km,
                "altura_km": bbox.height_km
            }
        )
    
    except Exception as exc:
        logger.error(
            f"Erro ao calcular bbox para subestação {subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            "Falha ao calcular bounding box"
        ) from exc


@router.post(
    "/subestacao/{subestacao_id}/consultar-e-registrar",
    summary="Consultar STAC e registrar imagens automaticamente",
    description="""
    Consulta STAC (Sentinel-2, Landsat) automaticamente e registra as imagens encontradas.
    
    Fluxo:
    1. Busca coordenadas da subestação
    2. Consulta Sentinel-2 e Landsat via STAC
    3. Registra as imagens no banco automaticamente
    4. Retorna URLs das imagens registradas
    """
)
def consultar_e_registrar_imagens(
    subestacao_id: int,
    data_inicio: str = Query("2025-01-01", description="Data início (YYYY-MM-DD)"),
    data_fim: str = Query("2025-12-31", description="Data fim (YYYY-MM-DD)"),
    raio_km: float = Query(5.0, ge=1, le=50, description="Área poligonal em km (bounding box)"),
    cobertura_nuvem_max: int = Query(30, ge=0, le=100, description="Cobertura máxima de nuvens %"),
    sensores: list = Query(["Sentinel-2", "Landsat"], description="Sensores a consultar"),
    engine: EngineDepends = None
):
    """
    Consulta STAC e registra imagens de satélite automaticamente.
    
    Args:
        subestacao_id: ID da subestação
        data_inicio: Data início (YYYY-MM-DD)
        data_fim: Data fim (YYYY-MM-DD)
        raio_km: Área poligonal de busca em km (baseada em bbox)
        cobertura_nuvem_max: Cobertura máxima de nuvens (%)
        sensores: Lista de sensores (Sentinel-2, Landsat)
        engine: Conexão com banco
        
    Returns:
        Dicionário com imagens registradas e URLs
    """
    try:
        import requests
        from datetime import datetime
        
        service = INPESatelliteService(logger=logger)
        
        # Buscar coordenadas da subestação
        resultado_coords = get_coordenadas_subestacao(
            subestacao_id=subestacao_id,
            engine=engine,
            raio_km=raio_km
        )
        
        if "erro" in resultado_coords:
            raise DatabaseError(resultado_coords["erro"])
        
        bbox = resultado_coords["bbox"]
        
        imagens_registradas = []
        erros = []
        
        # Consultar Sentinel-2
        if "Sentinel-2" in sensores:
            try:
                logger.info(f"Consultando Sentinel-2 para subestação {subestacao_id}...")
                
                payload_s2 = {
                    "bbox": [bbox["min_lon"], bbox["min_lat"], bbox["max_lon"], bbox["max_lat"]],
                    "datetime": f"{data_inicio}T00:00:00Z/{data_fim}T23:59:59Z",
                    "query": {"eo:cloud_cover": {"lte": cobertura_nuvem_max}},
                    "collections": ["sentinel-2-l2a"],
                    "limit": 5
                }
                
                resp = requests.post(
                    "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                    json=payload_s2,
                    timeout=30
                )
                resp.raise_for_status()
                
                items = resp.json().get("features", [])
                logger.info(f"Encontradas {len(items)} imagens Sentinel-2")
                
                for item in items:
                    try:
                        # Extrair URL - preferir visual (RGB) ou rendered_preview
                        assets = item.get("assets", {})
                        visual = assets.get("visual") or assets.get("rendered_preview")
                        
                        if not visual:
                            logger.warning(f"Nenhuma imagem RGB encontrada. Assets: {list(assets.keys())}")
                            continue
                        
                        url = visual.get("href")
                        if not url:
                            logger.warning("URL não encontrada no asset visual")
                            continue
                        
                        data_aquisicao = item.get("properties", {}).get("datetime", datetime.now().isoformat())
                        cloud_cover = item.get("properties", {}).get("eo:cloud_cover", cobertura_nuvem_max)
                        
                        logger.info(f"Processando Sentinel-2: {url[:80]}...")
                        
                        # Registrar no banco
                        from ..schemas import RegistrarImagemRequest
                        
                        img_request = RegistrarImagemRequest(
                            sensor="Sentinel-2",
                            data_aquisicao=data_aquisicao,
                            resolucao_m=10,
                            cobertura_nuvem_pct=cloud_cover,
                            url=url,
                            propriedades={
                                "tile": item.get("properties", {}).get("s2:tile_id"),
                                "processing_level": "L2A",
                                "asset_type": "visual"
                            }
                        )
                        
                        resultado_reg = registrar_imagem_subestacao(
                            subestacao_id=subestacao_id,
                            imagem=img_request,
                            engine=engine
                        )
                        
                        if resultado_reg.status == "sucesso":
                            imagens_registradas.append({
                                "sensor": "Sentinel-2",
                                "url": url,
                                "data": data_aquisicao,
                                "cobertura_nuvem": cloud_cover
                            })
                            logger.info(f"✓ Sentinel-2 registrada: ID {resultado_reg.imagem_id}")
                        else:
                            logger.warning(f"Falha ao registrar: {resultado_reg.mensagem}")
                            erros.append(f"Falha ao registrar Sentinel-2: {resultado_reg.mensagem}")
                        
                    except Exception as e:
                        logger.error(f"Erro ao processar item Sentinel-2: {e}", exc_info=True)
                        erros.append(f"Sentinel-2: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Erro ao consultar Sentinel-2: {e}")
                erros.append(f"Sentinel-2: {str(e)}")
        
        # Consultar Landsat
        if "Landsat" in sensores:
            try:
                logger.info(f"Consultando Landsat para subestação {subestacao_id}...")
                
                payload_ls = {
                    "bbox": [bbox["min_lon"], bbox["min_lat"], bbox["max_lon"], bbox["max_lat"]],
                    "datetime": f"{data_inicio}T00:00:00Z/{data_fim}T23:59:59Z",
                    "limit": 5
                }
                
                resp = requests.post(
                    "https://rstac.cr.usgs.gov/collections/landsat-c2-l2/items",
                    json=payload_ls,
                    timeout=30
                )
                resp.raise_for_status()
                
                items = resp.json().get("features", [])
                logger.info(f"Encontradas {len(items)} imagens Landsat")
                
                for item in items:
                    try:
                        # Extrair URL do Landsat
                        assets = item.get("assets", {})
                        rgb = assets.get("rendered_browse")
                        
                        if not rgb:
                            continue
                        
                        url = rgb.get("href")
                        data_aquisicao = item.get("properties", {}).get("datetime", datetime.now().isoformat())
                        cloud_cover = item.get("properties", {}).get("landsat:cloud_cover", 50)
                        
                        # Registrar no banco
                        from ..schemas import RegistrarImagemRequest
                        
                        img_request = RegistrarImagemRequest(
                            sensor="Landsat",
                            data_aquisicao=data_aquisicao,
                            resolucao_m=30,
                            cobertura_nuvem_pct=cloud_cover,
                            url=url,
                            propriedades={
                                "processing_level": "L2"
                            }
                        )
                        
                        registrar_imagem_subestacao(
                            subestacao_id=subestacao_id,
                            imagem=img_request,
                            engine=engine
                        )
                        
                        imagens_registradas.append({
                            "sensor": "Landsat",
                            "url": url,
                            "data": data_aquisicao,
                            "cobertura_nuvem": cloud_cover
                        })
                        
                        logger.info(f"✓ Landsat registrada: {url}")
                        
                    except Exception as e:
                        logger.warning(f"Erro ao processar item Landsat: {e}")
                        erros.append(str(e))
                        
            except Exception as e:
                logger.error(f"Erro ao consultar Landsat: {e}")
                erros.append(f"Landsat: {str(e)}")
        
        return {
            "subestacao_id": subestacao_id,
            "imagens_registradas": len(imagens_registradas),
            "imagens": imagens_registradas,
            "erros": erros,
            "mensagem": f"{len(imagens_registradas)} imagens registradas com sucesso"
        }
        
    except Exception as exc:
        logger.error(
            f"Erro ao consultar e registrar imagens para subestação {subestacao_id}: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            f"Falha ao consultar STAC: {str(exc)}"
        ) from exc


@router.post(
    "/planetary-computer/{subestacao_id}",
    response_model=dict,
    summary="Consultar Planetary Computer com assinatura automática"
)
def consultar_planetary_computer(
    subestacao_id: int,
    engine: EngineDepends,
    data_inicio: str = Query(default=None, description="Data início (YYYY-MM-DD)"),
    data_fim: str = Query(default=None, description="Data fim (YYYY-MM-DD)"),
    raio_km: float = Query(default=5.0, ge=0.5, le=50.0),
    cobertura_nuvem_max: int = Query(default=30, ge=0, le=100),
):
    """
    Consulta Planetary Computer (STAC) para Sentinel-2 com assinatura automática.
    
    URLs retornadas já possuem assinatura válida do Planetary Computer.
    
    - **subestacao_id**: ID da subestação
    - **data_inicio**: Data no formato YYYY-MM-DD
    - **data_fim**: Data no formato YYYY-MM-DD
    - **raio_km**: Raio de busca em km
    - **cobertura_nuvem_max**: Cobertura máxima de nuvens (0-100)
    """
    if not HAS_STAC:
        raise DatabaseError(
            "Dependências não instaladas. Execute: pip install pystac-client planetary-computer"
        )
    
    try:
        # Obter coordenadas da subestação
        service = INPESatelliteService(logger=logger)
        dados = service.consultar_subestacao_satellite_data(
            engine=engine,
            subestacao_id=subestacao_id,
            raio_km=raio_km
        )
        
        if "erro" in dados:
            raise DatabaseError(dados["erro"])
        
        subestacao = dados["subestacao"]
        latitude = subestacao["latitude"]
        longitude = subestacao["longitude"]
        
        # Datas padrão
        if not data_fim:
            data_fim = datetime.utcnow().date().isoformat()
        if not data_inicio:
            data_inicio = (datetime.utcnow() - timedelta(days=90)).date().isoformat()
        
        logger.info(f"Consultando Planetary Computer: ({latitude}, {longitude})")
        logger.info(f"Período: {data_inicio} a {data_fim}, nuvens: {cobertura_nuvem_max}%")
        
        # Conectar ao STAC do Planetary Computer
        client = Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        
        # Definir bbox
        buffer_graus = raio_km / 111
        bbox = [
            longitude - buffer_graus,
            latitude - buffer_graus,
            longitude + buffer_graus,
            latitude + buffer_graus,
        ]
        
        # Buscar Sentinel-2
        search = client.search(
            collections=["sentinel-2-l2a"],
            bbox=bbox,
            datetime=f"{data_inicio}T00:00:00Z/{data_fim}T23:59:59Z",
            query={"eo:cloud_cover": {"lt": cobertura_nuvem_max}},
            max_items=20,
        )
        
        # Processar resultados
        imagens = []
        
        for item in search.items():
            props = item.properties
            assets = item.assets
            
            # URL do asset visual
            visual_url = None
            if "visual" in assets:
                visual_url = assets["visual"].href
            elif "TCI_10m" in assets:
                visual_url = assets["TCI_10m"].href
            
            # Aplicar assinatura
            if visual_url and visual_url.startswith("https://"):
                visual_url = planetary_computer.sign_url(visual_url)
            
            imagem_info = {
                "id": item.id,
                "data": item.datetime.isoformat() if item.datetime else props.get("datetime"),
                "sensor": "Sentinel-2",
                "cobertura_nuvem": props.get("eo:cloud_cover", 0),
                "url": visual_url,
                "bbox": item.bbox,
            }
            
            imagens.append(imagem_info)
            logger.debug(f"Imagem: {item.id}, nuvens: {imagem_info['cobertura_nuvem']:.1f}%")
        
        logger.info(f"Total: {len(imagens)} imagens encontradas")
        
        return {
            "subestacao_id": subestacao_id,
            "subestacao": subestacao,
            "imagens_encontradas": len(imagens),
            "imagens": imagens,
            "periodo": {
                "data_inicio": data_inicio,
                "data_fim": data_fim,
            },
            "filtros": {
                "raio_km": raio_km,
                "cobertura_nuvem_max": cobertura_nuvem_max,
            },
            "nota": "URLs já possuem assinatura automática do Planetary Computer"
        }
        
    except Exception as exc:
        logger.error(
            f"Erro ao consultar Planetary Computer: {exc}",
            exc_info=True
        )
        raise DatabaseError(
            f"Falha ao consultar Planetary Computer: {str(exc)}"
        ) from exc


# ==========================================
# NOVOS ENDPOINTS CBERS-4A (INPE) - RESOLUÇÃO 2m
# ==========================================

@router.get(
    "/cbers/{subestacao_id}/buscar",
    summary="Buscar imagens CBERS-4A para subestação",
    description="""
    🆕 Endpoint para buscar imagens CBERS-4A do INPE (resolução 2m).
    
    **VANTAGENS:**
    - Resolução 2 metros (5x melhor que Sentinel-2)
    - Totalmente gratuito
    - Dados brasileiros (INPE)
    - Adequado para detectar telhados grandes
    
    **Retorna:** Lista de imagens CBERS-4A disponíveis com metadados.
    """
)
async def buscar_cbers_subestacao(
    subestacao_id: int,
    engine: EngineDepends,
    data_inicio: Optional[str] = Query(None, description="Data início (YYYY-MM-DD)"),
    data_fim: Optional[str] = Query(None, description="Data fim (YYYY-MM-DD)"),
    raio_km: float = Query(5.0, ge=1.0, le=50.0, description="Raio de busca em km"),
    cobertura_nuvem_max: float = Query(30.0, ge=0, le=100, description="Cobertura máxima de nuvens %")
):
    """
    Busca imagens CBERS-4A do INPE para uma subestação.
    
    Args:
        subestacao_id: ID da subestação
        data_inicio: Data início (YYYY-MM-DD), padrão últimos 6 meses
        data_fim: Data fim (YYYY-MM-DD), padrão hoje
        raio_km: Raio de busca em km
        cobertura_nuvem_max: Cobertura máxima de nuvens (%)
        
    Returns:
        Lista de imagens CBERS-4A encontradas
    """
    try:
        from datetime import datetime, timedelta
        from sqlalchemy import text
        
        # Buscar coordenadas da subestação
        query = text("""
            SELECT id, nome, latitude, longitude, distribuidora
            FROM subestacoes_detectadas
            WHERE id = :id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"id": subestacao_id}).fetchone()
        
        if not result:
            raise DatabaseError("Subestação não encontrada")
        
        id_sub, nome, latitude, longitude, distribuidora = result
        
        # Datas padrão se não fornecidas
        if not data_fim:
            data_fim = datetime.now().strftime("%Y-%m-%d")
        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        
        logger.info(
            f"Buscando CBERS-4A para {nome} (lat={latitude}, lon={longitude}) "
            f"de {data_inicio} até {data_fim}"
        )
        
        # Criar serviço CBERS
        cbers_service = CBERSService()
        
        # Buscar imagens
        imagens = cbers_service.buscar_imagens(
            latitude=latitude,
            longitude=longitude,
            raio_km=raio_km,
            data_inicio=data_inicio,
            data_fim=data_fim,
            cobertura_nuvem_max=cobertura_nuvem_max,
            colecao="CBERS-4A-WPM-L4-SR"
        )
        
        logger.info(f"Encontradas {len(imagens)} imagens CBERS-4A")
        
        # Converter para formato de resposta
        imagens_response = []
        for img in imagens:
            imagens_response.append({
                "id": img.id,
                "data_aquisicao": img.data.isoformat() if img.data else None,
                "sensor": img.sensor,
                "resolucao_m": img.resolucao,
                "cobertura_nuvem_pct": img.cobertura_nuvem,
                "urls": img.urls,
                "bbox": {
                    "min_lon": img.bbox[0],
                    "min_lat": img.bbox[1],
                    "max_lon": img.bbox[2],
                    "max_lat": img.bbox[3]
                }
            })
        
        return {
            "subestacao": {
                "id": id_sub,
                "nome": nome,
                "latitude": latitude,
                "longitude": longitude,
                "distribuidora": distribuidora
            },
            "parametros_busca": {
                "data_inicio": data_inicio,
                "data_fim": data_fim,
                "raio_km": raio_km,
                "cobertura_nuvem_max": cobertura_nuvem_max
            },
            "total_imagens": len(imagens),
            "imagens": imagens_response
        }
        
    except Exception as exc:
        logger.error(f"Erro ao buscar CBERS para subestação {subestacao_id}: {exc}", exc_info=True)
        raise DatabaseError(f"Falha ao buscar imagens CBERS: {str(exc)}") from exc


@router.get(
    "/cbers/download-banda/{image_id}",
    summary="Download de banda CBERS-4A",
    description="Baixa uma banda específica de uma imagem CBERS-4A (pan, red, green, blue, nir)"
)
async def download_banda_cbers(
    image_id: str,
    banda: str = Query(..., description="Nome da banda: pan, red, green, blue, nir"),
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat")
):
    """
    Baixa uma banda específica de imagem CBERS-4A.
    
    Args:
        image_id: ID da imagem CBERS
        banda: Nome da banda (pan, red, green, blue, nir)
        bbox: Opcional - recorte (min_lon,min_lat,max_lon,max_lat)
        
    Returns:
        Array numpy com dados da banda
    """
    try:
        import numpy as np
        
        cbers_service = CBERSService()
        
        # Parse bbox se fornecido
        bbox_tuple = None
        if bbox:
            bbox_parts = [float(x) for x in bbox.split(",")]
            if len(bbox_parts) == 4:
                bbox_tuple = tuple(bbox_parts)
        
        # Download da banda
        dados_banda = cbers_service.download_banda(
            image_id=image_id,
            banda=banda,
            bbox=bbox_tuple
        )
        
        if dados_banda is None:
            raise DatabaseError(f"Banda {banda} não encontrada na imagem {image_id}")
        
        return {
            "image_id": image_id,
            "banda": banda,
            "shape": dados_banda.shape,
            "dtype": str(dados_banda.dtype),
            "min": float(np.min(dados_banda)),
            "max": float(np.max(dados_banda)),
            "mean": float(np.mean(dados_banda))
        }
        
    except Exception as exc:
        logger.error(f"Erro ao baixar banda {banda}: {exc}", exc_info=True)
        raise DatabaseError(f"Falha ao baixar banda: {str(exc)}") from exc


@router.get(
    "/cbers/composicao-rgb/{image_id}",
    summary="Criar composição RGB de imagem CBERS-4A",
    description="Cria uma composição RGB (true color) de uma imagem CBERS-4A"
)
async def criar_composicao_rgb_cbers(
    image_id: str,
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat"),
    salvar_caminho: Optional[str] = Query(None, description="Caminho para salvar a imagem")
):
    """
    Cria composição RGB de imagem CBERS-4A.
    
    Args:
        image_id: ID da imagem CBERS
        bbox: Opcional - recorte (min_lon,min_lat,max_lon,max_lat)
        salvar_caminho: Opcional - caminho para salvar a imagem PNG
        
    Returns:
        Informações sobre a composição RGB criada
    """
    try:
        from PIL import Image
        import numpy as np
        
        cbers_service = CBERSService()
        
        # Parse bbox se fornecido
        bbox_tuple = None
        if bbox:
            bbox_parts = [float(x) for x in bbox.split(",")]
            if len(bbox_parts) == 4:
                bbox_tuple = tuple(bbox_parts)
        
        # Criar composição RGB
        rgb_image = cbers_service.criar_composicao_rgb(
            image_id=image_id,
            bbox=bbox_tuple,
            salvar_caminho=salvar_caminho
        )
        
        if rgb_image is None:
            raise DatabaseError(f"Falha ao criar composição RGB para imagem {image_id}")
        
        # Converter PIL Image para numpy array para estatísticas
        rgb_array = np.array(rgb_image)
        
        return {
            "image_id": image_id,
            "composicao": "RGB (True Color)",
            "shape": rgb_array.shape,
            "bandas": ["Red", "Green", "Blue"],
            "salvo_em": salvar_caminho if salvar_caminho else None,
            "estatisticas": {
                "min": int(rgb_array.min()),
                "max": int(rgb_array.max()),
                "mean": float(rgb_array.mean())
            }
        }
        
    except Exception as exc:
        logger.error(f"Erro ao criar composição RGB: {exc}", exc_info=True)
        raise DatabaseError(f"Falha ao criar composição RGB: {str(exc)}") from exc


# ============================================================================
# ESTRATÉGIA HÍBRIDA - CBERS + Google Maps Fallback
# ============================================================================

@router.post(
    "/hibrido/buscar",
    response_model=dict,
    summary="Buscar imagem com fallback automático (CBERS → Google Maps)",
    description="""
    Busca imagem de satélite usando estratégia híbrida com fallback automático:
    
    1. **CBERS-4A** (2m, grátis) - Primeira escolha
    2. **Google Maps** (0.3m, 25k grátis/mês) - Fallback automático
    3. **Sentinel-2** (10m, grátis) - Última opção (desabilitado)
    
    **Estratégias disponíveis:**
    - `auto`: Escolha inteligente baseada em resolução preferida
    - `alta_resolucao`: Prioriza Google Maps (melhor qualidade)
    - `custo_zero`: Apenas fontes gratuitas (CBERS + Sentinel)
    - `rapido`: Prioriza cache e fontes rápidas
    """
)
async def buscar_imagem_hibrida(
    latitude: Annotated[float, Query(description="Latitude da localização", ge=-90, le=90)],
    longitude: Annotated[float, Query(description="Longitude da localização", ge=-180, le=180)],
    raio_km: Annotated[float, Query(description="Raio de busca em km", ge=1, le=100)] = 10.0,
    estrategia: Annotated[str, Query(description="Estratégia de busca")] = "auto",
    usar_cache: Annotated[bool, Query(description="Usar cache de imagens")] = True,
    preferencia_resolucao: Annotated[float, Query(description="Resolução preferida em m/pixel")] = 2.0
):
    """
    Busca imagem com fallback automático entre CBERS, Google Maps e Sentinel-2
    """
    try:
        from ..services.imagem_strategy_service import ImagemStrategyService
        
        logger.info(f"Buscando imagem híbrida: ({latitude}, {longitude})")
        logger.info(f"  Estratégia: {estrategia}")
        logger.info(f"  Raio: {raio_km} km")
        
        # Validar estratégia
        estrategias_validas = ["auto", "alta_resolucao", "custo_zero", "rapido"]
        if estrategia not in estrategias_validas:
            raise DatabaseError(
                f"Estratégia inválida: {estrategia}. "
                f"Válidas: {', '.join(estrategias_validas)}"
            )
        
        # Inicializar serviço
        strategy_service = ImagemStrategyService(
            preferencia_resolucao=preferencia_resolucao
        )
        
        # Buscar imagem
        resultado = strategy_service.buscar_imagem_automatica(
            latitude=latitude,
            longitude=longitude,
            raio_km=raio_km,
            usar_cache=usar_cache,
            estrategia=estrategia
        )
        
        if resultado is None:
            return {
                "sucesso": False,
                "mensagem": "Nenhuma fonte de imagem disponível",
                "fonte": None,
                "estrategia_usada": estrategia,
                "estatisticas": strategy_service.get_estatisticas()
            }
        
        # Sucesso
        return {
            "sucesso": True,
            "fonte": resultado.fonte,
            "resolucao_m": resultado.resolucao_m,
            "shape": list(resultado.imagem.shape),
            "latitude": resultado.latitude,
            "longitude": resultado.longitude,
            "timestamp": resultado.timestamp.isoformat(),
            "metadata": resultado.metadata,
            "estrategia_usada": estrategia,
            "mensagem": f"Imagem obtida de {resultado.fonte.upper()} com resolução {resultado.resolucao_m}m/pixel"
        }
        
    except Exception as exc:
        logger.error(f"Erro ao buscar imagem híbrida: {exc}", exc_info=True)
        raise DatabaseError(f"Falha na busca híbrida: {str(exc)}") from exc


@router.get(
    "/hibrido/estatisticas",
    response_model=dict,
    summary="Estatísticas de uso da estratégia híbrida",
    description="Retorna estatísticas de tentativas e sucessos por fonte de imagem"
)
async def get_estatisticas_hibridas():
    """
    Retorna estatísticas agregadas de uso das fontes de imagem
    """
    try:
        from ..services.imagem_strategy_service import ImagemStrategyService
        
        # Criar instância temporária para pegar estatísticas
        strategy_service = ImagemStrategyService()
        stats = strategy_service.get_estatisticas()
        
        return {
            "sucesso": True,
            "estatisticas": stats
        }
        
    except Exception as exc:
        logger.error(f"Erro ao obter estatísticas: {exc}", exc_info=True)
        raise DatabaseError(f"Falha ao obter estatísticas: {str(exc)}") from exc


@router.get(
    "/hibrido/custo-estimado",
    response_model=dict,
    summary="Estimar custo do Google Maps",
    description="Calcula custo estimado para uso do Google Maps Static API"
)
async def estimar_custo_google_maps(
    num_imagens: Annotated[int, Query(description="Número de imagens", ge=1, le=1000000)] = 1000
):
    """
    Estima custo para usar Google Maps (25k grátis, depois $0.002/imagem)
    """
    try:
        from ..services.google_maps_service import GoogleMapsService
        
        google_service = GoogleMapsService()
        estimativa = google_service.estimar_custo(num_imagens)
        
        return {
            "sucesso": True,
            "estimativa": estimativa,
            "google_maps_disponivel": google_service.esta_disponivel()
        }
        
    except Exception as exc:
        logger.error(f"Erro ao estimar custo: {exc}", exc_info=True)
        raise DatabaseError(f"Falha ao estimar custo: {str(exc)}") from exc


@router.post(
    "/hibrido/processar-lote",
    response_model=dict,
    summary="Processar múltiplas localizações em lote",
    description="Busca imagens para várias localizações usando estratégia híbrida"
)
async def processar_lote_hibrido(
    localizacoes: list[dict],
    estrategia: Annotated[str, Query(description="Estratégia de busca")] = "auto",
    preferencia_resolucao: Annotated[float, Query(description="Resolução preferida")] = 2.0
):
    """
    Processa múltiplas localizações em lote
    
    Exemplo de payload:
    ```json
    [
        {"id": 1, "latitude": -15.7939, "longitude": -47.8828},
        {"id": 2, "latitude": -23.5505, "longitude": -46.6333}
    ]
    ```
    """
    try:
        from ..services.imagem_strategy_service import ImagemStrategyService
        
        logger.info(f"Processando lote de {len(localizacoes)} localizações")
        
        # Validar entrada
        if not localizacoes:
            raise DatabaseError("Lista de localizações vazia")
        
        if len(localizacoes) > 100:
            raise DatabaseError("Máximo de 100 localizações por lote")
        
        # Inicializar serviço
        strategy_service = ImagemStrategyService(
            preferencia_resolucao=preferencia_resolucao
        )
        
        # Processar lote
        resultados = strategy_service.processar_lista_subestacoes(
            subestacoes=localizacoes,
            estrategia=estrategia
        )
        
        # Contar sucessos
        sucessos = sum(1 for r in resultados if r.get('sucesso'))
        
        return {
            "sucesso": True,
            "total_processado": len(resultados),
            "sucessos": sucessos,
            "falhas": len(resultados) - sucessos,
            "taxa_sucesso": (sucessos / len(resultados) * 100) if resultados else 0,
            "resultados": resultados,
            "estatisticas": strategy_service.get_estatisticas()
        }
        
    except Exception as exc:
        logger.error(f"Erro ao processar lote: {exc}", exc_info=True)
        raise DatabaseError(f"Falha no processamento em lote: {str(exc)}") from exc
