"""Transformador API endpoints (DDD Architecture).

This module defines the HTTP API endpoints for transformador operations.
It uses dependency injection to provide use cases and handles error conversion.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..application.transformador import (
    BuscarRegiaoUseCase,
    ExportarTransformadoresUseCase,
    GetBboxUseCase,
    GetEstatisticasAreasUseCase,
    GetEstatisticasGeraisUseCase,
    GetResumoConsumidoresUseCase,
    ListarConsumidoresATUseCase,
    ListarConsumidoresBTUseCase,
    ListarConsumidoresMTUseCase,
    ListarPorTipoTensaoUseCase,
    ListarTransformadoresPorDistribuidoraUseCase,
    ListarTransformadoresPorSubestacaoUseCase,
    ListarTransformadoresUseCase,
    ObtenerAreaCoberturaUseCase,
    ObtenerTransformadorUseCase,
)
from ..core import get_engine
from ..domain.transformador import TransformadorNotFoundError
from ..infrastructure.mappers.transformador_mapper import TransformadorMapper
from ..infrastructure.persistence import SQLAlchemyTransformadorRepository
from ..schemas.transformador import TransformadorDetailResponse, TransformadorListResponse

router = APIRouter(prefix="/api/v1/transformadores", tags=["transformadores"])


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================


def get_repository():
    """Dependency: Transformador repository."""
    engine = get_engine()
    return SQLAlchemyTransformadorRepository(engine)


def get_obter_transformador_use_case(
    repository=Depends(get_repository),
) -> ObtenerTransformadorUseCase:
    """Dependency: Use case for obtaining a transformador."""
    return ObtenerTransformadorUseCase(repository)


def get_listar_transformadores_use_case(
    repository=Depends(get_repository),
) -> ListarTransformadoresUseCase:
    """Dependency: Use case for listing transformadores."""
    return ListarTransformadoresUseCase(repository)


def get_listar_por_subestacao_use_case(
    repository=Depends(get_repository),
) -> ListarTransformadoresPorSubestacaoUseCase:
    """Dependency: Use case for listing transformadores by substation."""
    return ListarTransformadoresPorSubestacaoUseCase(repository)


def get_listar_por_distribuidora_use_case(
    repository=Depends(get_repository),
) -> ListarTransformadoresPorDistribuidoraUseCase:
    """Dependency: Use case for listing transformadores by distributor."""
    return ListarTransformadoresPorDistribuidoraUseCase(repository)


def get_obter_area_cobertura_use_case(
    repository=Depends(get_repository),
) -> ObtenerAreaCoberturaUseCase:
    """Dependency: Use case for obtaining coverage area."""
    return ObtenerAreaCoberturaUseCase(repository)


def get_bbox_use_case(
    repository=Depends(get_repository),
) -> GetBboxUseCase:
    """Dependency: Use case for obtaining bbox."""
    return GetBboxUseCase(repository)


def get_listar_por_tipo_tensao_use_case(
    repository=Depends(get_repository),
) -> ListarPorTipoTensaoUseCase:
    """Dependency: Use case for listing by voltage type."""
    return ListarPorTipoTensaoUseCase(repository)


def get_estatisticas_gerais_use_case(
    repository=Depends(get_repository),
) -> GetEstatisticasGeraisUseCase:
    """Dependency: Use case for general statistics."""
    return GetEstatisticasGeraisUseCase(repository)


def get_estatisticas_areas_use_case(
    repository=Depends(get_repository),
) -> GetEstatisticasAreasUseCase:
    """Dependency: Use case for area statistics."""
    return GetEstatisticasAreasUseCase(repository)


def get_buscar_regiao_use_case(
    repository=Depends(get_repository),
) -> BuscarRegiaoUseCase:
    """Dependency: Use case for region search."""
    return BuscarRegiaoUseCase(repository)


def get_resumo_consumidores_use_case(
    repository=Depends(get_repository),
) -> GetResumoConsumidoresUseCase:
    """Dependency: Use case for consumer summary."""
    return GetResumoConsumidoresUseCase(repository)


def get_listar_consumidores_bt_use_case(
    repository=Depends(get_repository),
) -> ListarConsumidoresBTUseCase:
    """Dependency: Use case for listing BT consumers."""
    return ListarConsumidoresBTUseCase(repository)


def get_listar_consumidores_mt_use_case(
    repository=Depends(get_repository),
) -> ListarConsumidoresMTUseCase:
    """Dependency: Use case for listing MT consumers."""
    return ListarConsumidoresMTUseCase(repository)


def get_listar_consumidores_at_use_case(
    repository=Depends(get_repository),
) -> ListarConsumidoresATUseCase:
    """Dependency: Use case for listing AT consumers."""
    return ListarConsumidoresATUseCase(repository)


def get_exportar_use_case(
    repository=Depends(get_repository),
) -> ExportarTransformadoresUseCase:
    """Dependency: Use case for export."""
    return ExportarTransformadoresUseCase(repository)


# ============================================================================
# ERROR HANDLERS (Convert domain exceptions to HTTP exceptions)
# ============================================================================


def handle_domain_error(error: TransformadorNotFoundError) -> HTTPException:
    """Convert domain error to HTTP exception."""
    return HTTPException(status_code=404, detail=error.message)


# ============================================================================
# ENDPOINTS
# ============================================================================

# SPECIFIC STAT ROUTES (must come before /{id})
@router.get("/stats/geral", response_model=dict)
def get_stats_geral(
    use_case: GetEstatisticasGeraisUseCase = Depends(
        get_estatisticas_gerais_use_case
    ),
):
    """
    Get general statistics about all transformadores.

    Returns:
    - **total**: Total count
    - **potencia_total_kva**: Total power in kVA
    - **potencia_media_kva**: Average power in kVA
    - **potencia_maxima_kva**: Maximum power in kVA
    - **potencia_minima_kva**: Minimum power in kVA
    - **quantidade_bt**: Count of BT transformadores
    - **quantidade_mt**: Count of MT transformadores
    - **quantidade_at**: Count of AT transformadores
    """
    try:
        stats = use_case.execute()

        return {"status": "success", "data": stats}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/areas", response_model=dict)
def get_stats_areas(
    use_case: GetEstatisticasAreasUseCase = Depends(
        get_estatisticas_areas_use_case
    ),
):
    """
    Get statistics grouped by distribution area.

    Returns statistics per distributor with quantity and total power.
    """
    try:
        stats = use_case.execute()

        return {"status": "success", "data": stats}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/regiao/buscar", response_model=dict)
def search_region(
    min_lat: float = Query(..., description="Minimum latitude"),
    min_lon: float = Query(..., description="Minimum longitude"),
    max_lat: float = Query(..., description="Maximum latitude"),
    max_lon: float = Query(..., description="Maximum longitude"),
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: BuscarRegiaoUseCase = Depends(get_buscar_regiao_use_case),
):
    """
    Search transformadores within a geographic region.

    Query parameters:
    - **min_lat, min_lon, max_lat, max_lon**: Bounding box coordinates
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    try:
        transformadores, total = use_case.execute(
            min_lat=min_lat,
            min_lon=min_lon,
            max_lat=max_lat,
            max_lon=max_lon,
            limite=limite,
            pagina=pagina,
        )

        return {
            "status": "success",
            "data": {
                "items": [
                    TransformadorMapper.to_list_response(t)
                    for t in transformadores
                ],
                "total": total,
                "bbox": {
                    "min_lat": min_lat,
                    "min_lon": min_lon,
                    "max_lat": max_lat,
                    "max_lon": max_lon,
                },
                "pagina": pagina,
                "limite": limite,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# PREFIXED ROUTES (must come before /{id})
@router.get("/tipo-tensao/{tipo_tensao}", response_model=dict)
def list_by_tipo_tensao(
    tipo_tensao: str,
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarPorTipoTensaoUseCase = Depends(
        get_listar_por_tipo_tensao_use_case
    ),
):
    """
    List transformadores filtered by voltage type.

    Path parameters:
    - **tipo_tensao**: Voltage type (BT, MT, AT)

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    try:
        transformadores, total = use_case.execute(
            tipo_tensao=tipo_tensao, limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [
                    TransformadorMapper.to_list_response(t)
                    for t in transformadores
                ],
                "total": total,
                "tipo_tensao": tipo_tensao,
                "pagina": pagina,
                "limite": limite,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/subestacao/{subestacao_codigo}", response_model=dict)
def list_by_subestacao(
    subestacao_codigo: str,
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTransformadoresPorSubestacaoUseCase = Depends(
        get_listar_por_subestacao_use_case
    ),
):
    """
    List all transformadores for a specific substation.

    Path parameters:
    - **subestacao_codigo**: Substation code

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    try:
        transformadores, total = use_case.execute(
            subestacao_codigo=subestacao_codigo, limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [
                    TransformadorMapper.to_list_response(t)
                    for t in transformadores
                ],
                "total": total,
                "subestacao_codigo": subestacao_codigo,
                "pagina": pagina,
                "limite": limite,
            },
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/distribuidora/{distribuidora}", response_model=dict)
def list_by_distribuidora(
    distribuidora: str,
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTransformadoresPorDistribuidoraUseCase = Depends(
        get_listar_por_distribuidora_use_case
    ),
):
    """
    List all transformadores for a specific distribution company.

    Path parameters:
    - **distribuidora**: Distribution company name

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)
    """
    try:
        transformadores = use_case.execute(
            distribuidora=distribuidora, limite=limite, pagina=pagina
        )

        return {
            "status": "success",
            "data": {
                "items": [
                    TransformadorMapper.to_list_response(t)
                    for t in transformadores
                ],
                "distribuidora": distribuidora,
                "pagina": pagina,
                "limite": limite,
            },
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/export/{formato}", response_model=dict)
def export_transformadores(
    formato: str,
    use_case: ExportarTransformadoresUseCase = Depends(get_exportar_use_case),
):
    """
    Export all transformadores in specified format.

    Path parameters:
    - **formato**: Export format (json, csv, geojson)

    Returns exported data as string in the requested format.
    """
    try:
        data = use_case.execute(formato)

        if not data:
            raise HTTPException(
                status_code=500, detail="Failed to export data"
            )

        return {
            "status": "success",
            "data": data,
            "formato": formato,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ID-BASED ROUTES (must come AFTER prefix-based routes)
@router.get("/{id}", response_model=TransformadorDetailResponse)
def get_transformador_detail(
    id: int,
    use_case: ObtenerTransformadorUseCase = Depends(
        get_obter_transformador_use_case
    ),
    area_use_case: ObtenerAreaCoberturaUseCase = Depends(
        get_obter_area_cobertura_use_case
    ),
):
    """
    Get detailed information about a transformador.

    Returns:
    - **id**: Transformador ID
    - **codigo**: ANEEL code
    - **nome**: Name
    - **latitude**: Geographic latitude
    - **longitude**: Geographic longitude
    - **potencia_kva**: Power in kVA
    - **potencia_mva**: Power in MVA
    - **potencia_w**: Power in Watts
    - **tipo_tensao**: Voltage type
    - **subestacao_codigo**: Associated substation
    - **distribuidora**: Distribution company
    - **ativo**: Is active
    - **area_cobertura_geojson**: Coverage area in GeoJSON
    """
    try:
        # Execute use case
        transformador = use_case.execute(id)

        # Get coverage area
        area_geojson = None
        try:
            area_geojson = area_use_case.execute(id)
        except TransformadorNotFoundError:
            pass

        # Map to response
        return TransformadorMapper.to_detail_response(transformador, area_geojson)

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/area", response_model=dict)
def get_area_cobertura(
    id: int,
    use_case: ObtenerAreaCoberturaUseCase = Depends(
        get_obter_area_cobertura_use_case
    ),
):
    """
    Get coverage area of a transformador in GeoJSON format.

    Path parameters:
    - **id**: Transformador ID

    Returns coverage area as GeoJSON geometry.
    """
    try:
        area_geojson = use_case.execute(id)

        if area_geojson is None:
            raise HTTPException(
                status_code=404,
                detail="Coverage area not found for this transformador",
            )

        return {"status": "success", "data": {"area_geojson": area_geojson}}

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/bbox", response_model=dict)
def get_bbox(
    id: int,
    margem_km: float = Query(2.0, ge=0.1, le=50.0),
    use_case: GetBboxUseCase = Depends(get_bbox_use_case),
):
    """
    Get bounding box for satellite imagery.

    Path parameters:
    - **id**: Transformador ID

    Query parameters:
    - **margem_km**: Margin in kilometers (0.1-50.0, default: 2.0)

    Returns bounding box with {min_lat, min_lon, max_lat, max_lon}.
    """
    try:
        bbox = use_case.execute(id, margem_km)

        if not bbox:
            raise HTTPException(
                status_code=404, detail="Transformador not found"
            )

        return {"status": "success", "data": bbox}

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/consumidores/resumo", response_model=dict)
def get_consumer_summary(
    id: int,
    use_case: GetResumoConsumidoresUseCase = Depends(
        get_resumo_consumidores_use_case
    ),
):
    """
    Get summary of consumers (BT/MT/AT count) for a transformador.

    Path parameters:
    - **id**: Transformador ID

    Returns counts by consumer type.
    """
    try:
        summary = use_case.execute(id)

        if summary is None:
            return {
                "status": "success",
                "data": {"bt_count": 0, "mt_count": 0, "at_count": 0},
            }

        return {"status": "success", "data": summary}

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)
    except Exception as e:
        # Return empty counts on any error
        return {
            "status": "success",
            "data": {"bt_count": 0, "mt_count": 0, "at_count": 0},
        }


@router.get("/{id}/consumidores/bt", response_model=dict)
def list_bt_consumers(
    id: int,
    limite: int = Query(100, ge=1, le=1000),
    use_case: ListarConsumidoresBTUseCase = Depends(
        get_listar_consumidores_bt_use_case
    ),
):
    """
    List BT (low voltage) consumers for a transformador.

    Path parameters:
    - **id**: Transformador ID

    Query parameters:
    - **limite**: Max results per request (1-1000, default: 100)
    """
    try:
        consumers = use_case.execute(id, limite)

        if consumers is None:
            consumers = []

        return {
            "status": "success",
            "data": {"items": consumers, "total": len(consumers), "tipo": "BT"},
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/consumidores/mt", response_model=dict)
def list_mt_consumers(
    id: int,
    limite: int = Query(100, ge=1, le=1000),
    use_case: ListarConsumidoresMTUseCase = Depends(
        get_listar_consumidores_mt_use_case
    ),
):
    """
    List MT (medium voltage) consumers for a transformador.

    Path parameters:
    - **id**: Transformador ID

    Query parameters:
    - **limite**: Max results per request (1-1000, default: 100)
    """
    try:
        consumers = use_case.execute(id, limite)

        if consumers is None:
            consumers = []

        return {
            "status": "success",
            "data": {"items": consumers, "total": len(consumers), "tipo": "MT"},
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)


@router.get("/{id}/consumidores/at", response_model=dict)
def list_at_consumers(
    id: int,
    limite: int = Query(100, ge=1, le=1000),
    use_case: ListarConsumidoresATUseCase = Depends(
        get_listar_consumidores_at_use_case
    ),
):
    """
    List AT (high voltage) consumers for a transformador.

    Path parameters:
    - **id**: Transformador ID

    Query parameters:
    - **limite**: Max results per request (1-1000, default: 100)
    """
    try:
        consumers = use_case.execute(id, limite)

        if consumers is None:
            consumers = []

        return {
            "status": "success",
            "data": {"items": consumers, "total": len(consumers), "tipo": "AT"},
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)
    except Exception as e:
        # Return empty list on any database error
        return {
            "status": "success",
            "data": {"items": [], "total": 0, "tipo": "AT"},
        }


# GENERIC ROUTES (must come LAST)
@router.get("", response_model=dict)
def list_transformadores(
    limite: int = Query(100, ge=1, le=1000),
    pagina: int = Query(0, ge=0),
    use_case: ListarTransformadoresUseCase = Depends(
        get_listar_transformadores_use_case
    ),
):
    """
    List all transformadores with pagination.

    Query parameters:
    - **limite**: Results per page (1-1000, default: 100)
    - **pagina**: Page number (0-indexed)

    Returns:
    - **items**: List of transformadores
    - **total**: Total count
    - **pagina**: Current page
    - **limite**: Results per page
    """
    try:
        transformadores, total = use_case.execute(limite=limite, pagina=pagina)

        return {
            "status": "success",
            "data": {
                "items": [
                    TransformadorMapper.to_list_response(t)
                    for t in transformadores
                ],
                "total": total,
                "pagina": pagina,
                "limite": limite,
            },
        }

    except TransformadorNotFoundError as e:
        raise handle_domain_error(e)
