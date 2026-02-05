"""Telhado API endpoints (DDD Architecture).

This module defines the HTTP API endpoints for roof detection operations.
It uses dependency injection to provide use cases and handles error conversion.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

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
from src.core import get_engine
from src.domain.telhado import TelhadoNotFoundError
from src.infrastructure.mappers.telhado_mapper import TelhadoMapper
from src.infrastructure.persistence.telhado import SQLAlchemyTelhadoRepository

router = APIRouter(prefix="/api/v1/telhados", tags=["telhados"])


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
