"""
API Endpoints para Transformadores e Áreas de Cobertura
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Path

from ..core import get_engine
from ..services.transformador_service import TransformadorService

router = APIRouter(prefix="/api/v1/transformadores", tags=["transformadores"])


def get_transformador_service() -> TransformadorService:
    """Dependência para obter serviço de transformadores"""
    engine = get_engine()
    return TransformadorService(engine)


# ============================================================================
# TRANSFORMADORES - DETALHES E ÁREAS
# ============================================================================

@router.get("/{id}")
def get_transformador_detalhes(
    id: int,
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna detalhes de um transformador incluindo sua área de cobertura
    
    - **id**: ID do transformador
    """
    trans = service.obter_detalhes(id)
    
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
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna a área de cobertura de um transformador em diferentes formatos
    
    - **formato**: geojson, wkt ou json
    """
    area = service.obter_area_cobertura_geojson(id, formato=formato)
    
    if area is None:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    return {
        "status": "success",
        "data": area
    }


@router.get("/{id}/bbox")
def get_bbox_transformador(
    id: int,
    margem_km: float = Query(2.0, gt=0.1, le=50),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna bounding box de um transformador para busca de imagens de satélite
    
    Parâmetros:
    - **margem_km**: Margem em km ao redor do transformador (padrão: 2 km)
    
    Útil para:
    - Baixar imagens Sentinel-2
    - Buscar no Planetary Computer
    - Download de dados de satélite
    """
    bbox = service.obter_bbox_para_satelite(id, margem_km=margem_km)
    
    if not bbox:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    return {
        "status": "success",
        "data": bbox
    }


# ============================================================================
# TRANSFORMADORES - LISTAGEM
# ============================================================================

@router.get("/subestacao/{subestacao_codigo}")
def listar_transformadores_subestacao(
    subestacao_codigo: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista todos os transformadores de uma subestação com suas áreas
    
    - **subestacao_codigo**: Código da subestação
    - **skip**: Offset para paginação
    - **limit**: Limite de resultados (máx 1000)
    """
    resultado = service.listar_por_subestacao(subestacao_codigo, skip=skip, limit=limit)
    
    return {
        "status": "success",
        **resultado
    }


@router.get("/distribuidora/{distribuidora}")
def listar_transformadores_distribuidora(
    distribuidora: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista transformadores de uma distribuidora
    
    - **distribuidora**: Nome ou parte do nome da distribuidora
    - **skip**: Offset para paginação
    - **limit**: Limite de resultados (máx 1000)
    """
    resultado = service.listar_por_distribuidora(distribuidora, skip=skip, limit=limit)
    
    return {
        "status": "success",
        **resultado
    }


@router.get("/tipo-tensao/{tipo_tensao}")
def listar_transformadores_por_tipo_tensao(
    tipo_tensao: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista transformadores por tipo de tensão
    
    - **tipo_tensao**: "BT" (Baixa), "MT" (Média) ou "AT" (Alta)
    - **skip**: Offset para paginação
    - **limit**: Limite de resultados
    """
    try:
        resultado = service.listar_por_tipo_tensao(tipo_tensao, skip=skip, limit=limit)
        return {
            "status": "success",
            **resultado
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("")
def listar_transformadores(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista todos os transformadores com paginação
    
    - **skip**: Offset para paginação
    - **limit**: Limite de resultados (máx 1000)
    """
    resultado = service.listar_todos(skip=skip, limit=limit)
    
    return {
        "status": "success",
        **resultado
    }


# ============================================================================
# TRANSFORMADORES - EXPORT
# ============================================================================

@router.get("/export/{formato}")
def exportar_transformadores(
    formato: str = Path(..., enum=["csv", "geojson", "json"]),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Exporta todos os transformadores
    
    Formatos disponíveis:
    - **csv**: Arquivo CSV para Excel/Google Sheets
    - **geojson**: GeoJSON para QGIS, Mapbox, ArcGIS
    - **json**: JSON simples
    """
    
    try:
        resultado = service.exportar(formato=formato)
        
        if not resultado:
            raise HTTPException(
                status_code=400, 
                detail=f"Falha ao exportar em formato {formato}"
            )
        
        return {
            "status": "success",
            "formato": formato,
            "data": resultado if isinstance(resultado, dict) else resultado[:500]  # Preview CSV
        }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ESTATÍSTICAS
# ============================================================================

@router.get("/stats/geral")
def obter_estatisticas_gerais(
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna estatísticas gerais de transformadores
    
    Informações:
    - Total por tipo de tensão (BT, MT, AT)
    - Potência média, mínima e máxima
    - Quantidade de subestações e distribuidoras
    """
    
    stats = service.obter_estatisticas_gerais()
    
    return {
        "status": "success",
        "data": stats
    }


@router.get("/stats/areas")
def obter_estatisticas_areas(
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna estatísticas de áreas de transformadores
    
    Informações:
    - Total de áreas calculadas
    - Métodos usados (ConvexHull vs Buffer)
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
    service: TransformadorService = Depends(get_transformador_service)
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
        resultado = service.buscar_por_regiao(min_lat, min_lon, max_lat, max_lon, skip=skip, limit=limit)
        
        return {
            "status": "success",
            **resultado
        }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# CONSUMIDORES ASSOCIADOS
# ============================================================================

@router.get("/{id}/consumidores/resumo")
def obter_resumo_consumidores(
    id: int,
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Retorna contagem de consumidores BT/MT/AT associados a um transformador
    
    - **id**: ID do transformador
    """
    transformador = service.obter_detalhes(id)
    if not transformador:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    consumidores = service.obter_consumidores_associados(transformador['codigo'])
    
    return {
        "status": "success",
        "transformador_id": id,
        "transformador_codigo": transformador['codigo'],
        "data": consumidores
    }


@router.get("/{id}/consumidores/bt")
def listar_consumidores_bt(
    id: int,
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista consumidores de Baixa Tensão (BT) de um transformador
    
    - **id**: ID do transformador
    - **limit**: Limite de resultados
    """
    transformador = service.obter_detalhes(id)
    if not transformador:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    resultado = service.listar_consumidores_bt_do_transformador(transformador['codigo'], limit=limit)
    
    return {
        "status": "success",
        **resultado
    }


@router.get("/{id}/consumidores/mt")
def listar_consumidores_mt(
    id: int,
    limit: int = Query(100, ge=1, le=1000),
    service: TransformadorService = Depends(get_transformador_service)
):
    """
    Lista consumidores de Média Tensão (MT) de um transformador
    
    - **id**: ID do transformador
    - **limit**: Limite de resultados
    """
    transformador = service.obter_detalhes(id)
    if not transformador:
        raise HTTPException(status_code=404, detail="Transformador não encontrado")
    
    resultado = service.listar_consumidores_mt_do_transformador(transformador['codigo'], limit=limit)
    
    return {
        "status": "success",
        **resultado
    }
