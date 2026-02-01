"""
Endpoints para gerenciar dados de imagens de satélite de subestações.
Integra com INPE (CBERS-4A) e plataformas STAC para obter imagens de satélite.

MUDANÇA IMPORTANTE: Agora usa CBERS-4A do INPE como fonte principal!
- Resolução: 2 metros (muito melhor que Sentinel-2 com 10m)
- Gratuito e brasileiro
- Adequado para detecção de telhados
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Path
from pydantic import BaseModel

from ..core import DatabaseError
from ..schemas import (
    ConsultaSateliteRequest,
    DadosSatelliteSubestacao,
    ListaImagensSatelite,
    RegistrarImagemRequest,
    RegistrarImagemResponse,
)
from ..services.inpe_satellite_service import INPESatelliteService
from ..services.cbers_service import CBERSService
from ..services.satellite_service_v2 import SatelliteServiceV2
from ..services.inpe_service_v2 import INPEServiceV2
from ..services.google_maps_service_v2 import GoogleMapsServiceV2
from .deps import EngineDepends, LimiteQuery

# Import STAC dependencies com tratamento de erro
try:
    HAS_STAC = True
except ImportError:
    HAS_STAC = False
    logger = logging.getLogger(__name__)
    logger.warning("pystac-client e planetary-computer não estão instalados")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/satelite", tags=["Satélite"])

class FonteDecisaoResponse(BaseModel):
    """Resposta de decisão de fonte para satélite"""
    fonte: str
    pode_usar: bool
    motivo: str
    resolucao_metros: float
    cobertura: str
    quota_disponivel: Optional[int] = None
    percentual_uso: Optional[float] = None


class ImagemSateliteResponse(BaseModel):
    """Resposta com imagem de satélite"""
    id: str
    data: str
    cobertura_nuvem_percent: float
    resolucao_metros: float
    sensor: str
    fonte: str = "CBERS-4A"  # CBERS-4A ou GOOGLE_MAPS
    tipo: Optional[str] = None  # Para Google Maps: satellite, hybrid
    url: Optional[str] = None  # URL direta da imagem (Google Maps)
    banda_pan: Optional[str] = None
    banda_red: Optional[str] = None
    banda_green: Optional[str] = None
    banda_blue: Optional[str] = None


class BuscaTransformadorResponse(BaseModel):
    """Resposta de busca por transformador"""
    fonte: str
    transformador_id: int
    imagens_encontradas: int
    imagens: List[ImagemSateliteResponse]
    imagens_google_maps: Optional[List[ImagemSateliteResponse]] = []  # Imagens do Google Maps
    bbox: tuple
    area_poligonal_km: float
    periodo: str
    resolucao_metros: float
    status: str


class ImagemSalvaResponse(BaseModel):
    """Resposta com imagem salva no banco"""
    id: int
    imagem_id: str
    url_download: Optional[str] = None
    data_imagem: Optional[str] = None
    cobertura_nuvem_percentual: Optional[float] = None
    resolucao_metros: Optional[float] = None
    status: str
    data_requisicao: str


class ListarImagensTransformadorResponse(BaseModel):
    """Resposta com lista de imagens do transformador"""
    transformador_id: int
    total_requisicoes: int
    total_sucesso: int
    imagens: List[ImagemSalvaResponse]


class QuotaGoogleMapsResponse(BaseModel):
    """Resposta com quota Google Maps"""
    pode_usar: bool
    usada: int
    disponivel: int
    percentual_uso: float
    limite: int
    mes: str


class RegistrarImagemTransformadorRequest(BaseModel):
    """Request para registrar imagem de transformador"""
    transformador_id: int
    subestacao_id: int
    data_aquisicao: str
    resolucao_m: float = 2.0
    cobertura_nuvem_pct: float
    urls_bandas: Optional[Dict[str, str]] = None  # {blue, green, red, nir, swir} - opcional para CBERS-4A
    url_google_maps_satellite: Optional[str] = None  # URL da imagem satellite do Google Maps
    url_google_maps_hybrid: Optional[str] = None  # URL da imagem hybrid do Google Maps


class RegistrarImagemTransformadorResponse(BaseModel):
    """Response ao registrar imagem de transformador"""
    sucesso: bool
    imagem_id: Optional[int] = None
    bandas_registradas: int = 0
    mensagem: str

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


# ==========================================
# ENDPOINTS CBERS-4A (INPE) - RESOLUÇÃO 2m
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

# ============================================================================
# ENDPOINTS - TRANSFORMADOR
# ============================================================================

@router.get(
    "/transformador/{transformador_id}/imagens/historico",
    response_model=ListarImagensTransformadorResponse,
    summary="Listar imagens salvas do transformador no banco"
)
def listar_imagens_historico_transformador(
    engine: EngineDepends,
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    limit: int = Query(50, ge=1, le=100, description="Máximo de registros a retornar"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
    apenas_sucesso: bool = Query(True, description="Retornar apenas requisições bem-sucedidas")
):
    """
    Lista imagens salvas no banco de dados para um transformador.
    
    Retorna histórico de requisições CBERS-4A com:
    - **ID da requisição** - identificador único
    - **imagem_id** - ID CBERS-4A (ex: CBERS_4A_WFI_20260130_210_132_L4)
    - **url_download** - URL para download da imagem
    - **data_imagem** - Data de aquisição
    - **cobertura_nuvem_percentual** - % de cobertura de nuvens
    - **resolucao_metros** - Resolução em metros
    - **status** - sucesso / sem_cobertura / erro
    - **data_requisicao** - Quando a requisição foi feita
    
    **Parâmetros:**
    - `transformador_id`: ID do transformador
    - `limit`: Máximo de registros (padrão: 50)
    - `offset`: Página (padrão: 0)
    - `apenas_sucesso`: Filtrar por sucesso (padrão: true)
    
    **Exemplo:**
    ```
    GET /satelite/v2/transformador/1/imagens/historico?limit=10&apenas_sucesso=true
    ```
    """
    try:
        from sqlalchemy import text
        
        # Query SQL para buscar imagens
        where_clause = "WHERE transformador_id = :transformador_id"
        if apenas_sucesso:
            where_clause += " AND status = 'sucesso'"
        
        query = f"""
            SELECT 
                id,
                imagem_id,
                url_download,
                data_imagem,
                cobertura_nuvem_percentual,
                resolucao_metros,
                status,
                data_requisicao
            FROM requisicoes_satelite_cbers4a
            {where_clause}
            ORDER BY data_requisicao DESC
            LIMIT :limit OFFSET :offset
        """
        
        # Query para contar total
        count_query = f"""
            SELECT COUNT(*) as total
            FROM requisicoes_satelite_cbers4a
            {where_clause}
        """
        
        with engine.begin() as conn:
            # Executar query de contagem
            result_count = conn.execute(
                text(count_query),
                {'transformador_id': transformador_id}
            )
            total_sucesso = result_count.scalar() or 0
            
            # Executar query de dados
            result = conn.execute(
                text(query),
                {
                    'transformador_id': transformador_id,
                    'limit': limit,
                    'offset': offset
                }
            )
            
            imagens = []
            for row in result:
                imagens.append(ImagemSalvaResponse(
                    id=row[0],
                    imagem_id=row[1],
                    url_download=row[2],
                    data_imagem=str(row[3]) if row[3] else None,
                    cobertura_nuvem_percentual=float(row[4]) if row[4] is not None else None,
                    resolucao_metros=float(row[5]) if row[5] is not None else None,
                    status=row[6],
                    data_requisicao=str(row[7]) if row[7] else None
                ))
        
        # Contar total geral (sem limite)
        with engine.begin() as conn:
            result_total = conn.execute(
                text(count_query),
                {'transformador_id': transformador_id}
            )
            total_requisicoes = result_total.scalar() or 0
        
        return ListarImagensTransformadorResponse(
            transformador_id=transformador_id,
            total_requisicoes=total_requisicoes,
            total_sucesso=total_sucesso,
            imagens=imagens
        )
        
    except Exception as e:
        logger.error(f"❌ Erro ao listar imagens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - SUBESTAÇÃO
# ============================================================================

@router.get(
    "/subestacao/{subestacao_id}/imagens",
    response_model=Dict,
    summary="Buscar imagens de satélite para subestação"
)
def buscar_imagens_subestacao(
    engine: EngineDepends,
    subestacao_id: int = Path(..., gt=0, description="ID da subestação"),
    cobertura_nuvem_max: int = Query(
        30,
        ge=0,
        le=100,
        description="Máximo % de cobertura de nuvens"
    ),
    dias_passados: int = Query(
        90,
        ge=1,
        le=365,
        description="Buscar nos últimos N dias"
    )
):
    """
    Busca imagens CBERS-4A para toda a área de uma subestação (polígono).
    
    **Retorna:**
    - Imagens ordenadas por qualidade (menos nuvens primeiro)
    - Bounding box da subestação
    - Período da busca
    - Status da operação
    """
    try:
        data_fim = datetime.now().strftime('%Y-%m-%d')
        data_inicio = (
            datetime.now() -
            __import__('datetime').timedelta(days=dias_passados)
        ).strftime('%Y-%m-%d')
        
        sat_service = SatelliteServiceV2(engine)
        inpe_service = INPEServiceV2(engine, sat_service)
        
        resultado = inpe_service.buscar_imagens_cbers4a_poligono(
            subestacao_id=subestacao_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
            cobertura_nuvem_max=cobertura_nuvem_max
        )
        
        return resultado
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar imagens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/google-maps/info",
    response_model=Dict,
    summary="Quota e estatísticas de uso do Google Maps"
)
def obter_info_google_maps(
    engine: EngineDepends
):
    """
    Retorna quota e estatísticas detalhadas de uso do Google Maps.
    
    **Resposta inclui:**
    - Quota do mês atual (limite, usado, disponível, percentual, custo estimado)
    - Transformadores únicos buscados
    - Total de requisições históricas
    - Histórico dos últimos 30 dias
    - Última requisição
    
    **Exemplo:**
    ```json
    {
        "quota": {
            "limite_mensal": 25000,
            "usada_mes_atual": 1250,
            "disponivel": 23750,
            "percentual_uso": 5.0,
            "custo_estimado_usd": 8.75,
            "ultima_requisicao": "2024-01-15T10:30:00"
        },
        "estatisticas": {
            "total_requisicoes": 5420,
            "transformadores_unicos": 42,
            "historico_30_dias": [...],
            "quota_mes_atual": {...}
        },
        "mes_ano": "2024-01",
        "total_requisicoes_mes": 1250,
        "requisicoes_sucesso_mes": 1200,
        "requisicoes_erro_mes": 50
    }
    ```
    """
    try:
        import os
        from ..services.google_maps_quota_service import GoogleMapsQuotaService
        
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        quota = service.obter_quota_google_maps_mes_atual()
        stats = service.obter_estatisticas_google_maps()
        
        # Migração de dados úteis de /telhados/quota-mes
        quota_service = GoogleMapsQuotaService(engine)
        quota_mes_detalhada = quota_service.obter_quota_mes()
        
        resultado = {
            "quota": quota,
            "estatisticas": stats
        }
        
        # Adicionar informações de quota do mês
        if quota_mes_detalhada.get('sucesso'):
            resultado.update({
                "mes_ano": quota_mes_detalhada.get('mes_ano'),
                "total_requisicoes_mes": quota_mes_detalhada.get('quota_usada_requests'),
                "quota_total": quota_mes_detalhada.get('quota_total'),
                "percentual_uso_mensal": quota_mes_detalhada.get('percentual_uso'),
                "custo_estimado_usd": quota_mes_detalhada.get('custo_estimado_usd')
            })
            
            # Atualizar quota com custo
            if "quota" in resultado:
                resultado["quota"]["custo_estimado_usd"] = quota_mes_detalhada.get('custo_estimado_usd')
        
        return resultado
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter informações do Google Maps: {e}")
        raise HTTPException(status_code=500, detail=str(e))
