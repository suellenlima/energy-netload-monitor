"""
API Endpoints para Transformadores e Áreas de Cobertura
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import Engine

from core import create_db_engine, load_settings
from schemas import TransformadorSchema, AreaCoberturaSchema, SubestacaoSchema
from services.area_service import AreaService

router = APIRouter(prefix="/api/v1/transformadores", tags=["transformadores"])


def get_area_service() -> AreaService:
    """Dependência para obter serviço de áreas"""
    settings = load_settings()
    engine = create_db_engine(settings.database.url)
    return AreaService(engine)


# ============================================================================
# TRANSFORMADORES - DETALHES E ÁREAS
# ============================================================================

@router.get("/{id}")
def get_transformador_detalhes(
    id: int,
    service: AreaService = Depends(get_area_service)
):
    """
    Retorna detalhes de um transformador incluindo sua área de cobertura
    
    - **id**: ID do transformador
    """
    trans = service.obter_area_transformador(id)
    
    if not trans:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    return {
        "status": "success",
        "data": trans
    }


@router.get("/{id}/area")
def get_area_transformador(
    id: int,
    formato: str = Query("geojson", enum=["geojson", "wkt", "json"]),
    service: AreaService = Depends(get_area_service)
):
    """
    Retorna a área de cobertura de um transformador em diferentes formatos
    
    - **formato**: geojson, wkt ou json
    """
    trans = service.obter_area_transformador(id)
    
    if not trans:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    if formato == "geojson":
        return {
            "type": "Feature",
            "geometry": trans.get("geojson_area"),
            "properties": {
                "id": trans["id"],
                "nome": trans["nome"],
                "area_km2": trans["area_km2"],
                "raio_m": trans["raio_m"],
                "potencia_kva": trans["potencia_kva"]
            }
        }
    
    elif formato == "wkt":
        return {
            "wkt": trans.get("wkt_area"),
            "area_km2": trans["area_km2"]
        }
    
    else:  # json
        return {
            "id": trans["id"],
            "nome": trans["nome"],
            "area_km2": trans["area_km2"],
            "raio_m": trans["raio_m"],
            "latitude": trans["latitude"],
            "longitude": trans["longitude"]
        }


@router.get("/{id}/bbox")
def get_bbox_transformador(
    id: int,
    service: AreaService = Depends(get_area_service)
):
    """
    Retorna bounding box de um transformador para busca de imagens de satélite
    
    Útil para:
    - Baixar imagens Sentinel-2
    - Buscar no Planetary Computer
    - Download de dados de satélite
    """
    bbox = service.obter_bbox_transformador(id)
    
    if not bbox:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    return {
        "status": "success",
        "data": bbox
    }


# ============================================================================
# TRANSFORMADORES - LISTAGEM
# ============================================================================

@router.get("/subestacao/{subestacao_id}")
def listar_transformadores_subestacao(
    subestacao_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: AreaService = Depends(get_area_service)
):
    """
    Lista todos os transformadores de uma subestação com suas áreas
    
    - **subestacao_id**: ID da subestação
    - **skip**: Offset para paginação
    - **limit**: Limite de resultados (máx 1000)
    """
    df = service.listar_transformadores_subestacao(subestacao_id)
    
    if df.empty:
        return {
            "status": "success",
            "data": [],
            "total": 0
        }
    
    transformadores = df.iloc[skip:skip + limit].to_dict('records')
    
    return {
        "status": "success",
        "data": transformadores,
        "total": len(df),
        "skip": skip,
        "limit": limit
    }


@router.get("")
def listar_transformadores(
    subestacao_id: Optional[int] = Query(None),
    min_lat: Optional[float] = Query(None),
    min_lon: Optional[float] = Query(None),
    max_lat: Optional[float] = Query(None),
    max_lon: Optional[float] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: AreaService = Depends(get_area_service)
):
    """
    Lista transformadores com filtros e paginação
    
    Filtros disponíveis:
    - **subestacao_id**: Filtrar por subestação
    - **min_lat, min_lon, max_lat, max_lon**: Busca por bounding box (região)
    """
    
    if subestacao_id:
        df = service.listar_transformadores_subestacao(subestacao_id)
    
    elif min_lat and min_lon and max_lat and max_lon:
        df = service.buscar_transformadores_por_regiao(min_lat, min_lon, max_lat, max_lon)
    
    else:
        df = service.listar_todas_transformadores()
    
    if df.empty:
        return {
            "status": "success",
            "data": [],
            "total": 0
        }
    
    transformadores = df.iloc[skip:skip + limit].to_dict('records')
    
    return {
        "status": "success",
        "data": transformadores,
        "total": len(df),
        "skip": skip,
        "limit": limit
    }


# ============================================================================
# TRANSFORMADORES - EXPORT
# ============================================================================

@router.get("/export/{formato}")
def exportar_transformadores(
    formato: str = Query(..., enum=["csv", "geojson", "json"]),
    service: AreaService = Depends(get_area_service)
):
    """
    Exporta todos os transformadores com suas áreas
    
    Formatos disponíveis:
    - **csv**: Arquivo CSV para Excel/Google Sheets
    - **geojson**: GeoJSON para QGIS, Mapbox, ArcGIS
    - **json**: JSON simples
    """
    
    try:
        resultado = service.exportar_transformadores(formato=formato)
        
        if not resultado:
            raise HTTPException(
                status_code=400, 
                detail=f"Falha ao exportar em formato {formato}"
            )
        
        return {
            "status": "success",
            "formato": formato,
            "data": resultado if formato == "json" else "Use endpoint POST para download"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ESTATÍSTICAS
# ============================================================================

@router.get("/stats/areas")
def obter_estatisticas_areas(
    service: AreaService = Depends(get_area_service)
):
    """
    Retorna estatísticas gerais de áreas de transformadores
    
    Informações:
    - Total de transformadores
    - Quantos têm área calculada
    - Área média, mínima e máxima
    - Área total coberta
    """
    
    stats = service.obter_estatisticas_areas()
    
    return {
        "status": "success",
        "data": stats
    }


# ============================================================================
# BUSCA ESPACIAL
# ============================================================================

@router.get("/regiao/buscar")
def buscar_transformadores_regiao(
    min_lat: float = Query(...),
    min_lon: float = Query(...),
    max_lat: float = Query(...),
    max_lon: float = Query(...),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: AreaService = Depends(get_area_service)
):
    """
    Busca transformadores dentro de um bounding box (região)
    
    Parâmetros:
    - **min_lat, min_lon, max_lat, max_lon**: Coordenadas do retângulo
    
    Exemplo: São Paulo
    - min_lat: -23.7
    - min_lon: -46.8
    - max_lat: -23.4
    - max_lon: -46.4
    """
    
    try:
        df = service.buscar_transformadores_por_regiao(min_lat, min_lon, max_lat, max_lon)
        
        if df.empty:
            return {
                "status": "success",
                "data": [],
                "total": 0
            }
        
        transformadores = df.iloc[skip:skip + limit].to_dict('records')
        
        return {
            "status": "success",
            "data": transformadores,
            "total": len(df),
            "skip": skip,
            "limit": limit,
            "bbox": {
                "min_lat": min_lat,
                "min_lon": min_lon,
                "max_lat": max_lat,
                "max_lon": max_lon
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
