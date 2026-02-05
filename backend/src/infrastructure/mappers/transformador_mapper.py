"""Transformador mapper - converts between domain entities and DTOs.

The mapper is responsible for converting domain entities to response schemas
(DTOs) and vice versa. This keeps the domain and presentation layers decoupled.
"""

from typing import Optional

from ...domain.transformador import Transformador
from ...schemas.transformador import (
    TransformadorDetailResponse,
    TransformadorListResponse,
)


class TransformadorMapper:
    """Maps Transformador domain entity to API response schemas."""

    @staticmethod
    def to_list_response(transformador: Transformador) -> TransformadorListResponse:
        """Convert domain entity to list response DTO."""
        return TransformadorListResponse(
            id=transformador.id,
            codigo=str(transformador.codigo),
            nome=str(transformador.nome),
            latitude=transformador.localizacao.latitude,
            longitude=transformador.localizacao.longitude,
            potencia_kva=transformador.potencia.kva,
            tipo_tensao=str(transformador.tipo_tensao),
            subestacao_codigo=transformador.subestacao_codigo,
            distribuidora=transformador.distribuidora,
            ativo=transformador.ativo,
        )

    @staticmethod
    def to_detail_response(
        transformador: Transformador,
        area_cobertura_geojson: Optional[str] = None,
    ) -> TransformadorDetailResponse:
        """Convert domain entity to detail response DTO."""
        return TransformadorDetailResponse(
            id=transformador.id,
            codigo=str(transformador.codigo),
            nome=str(transformador.nome),
            latitude=transformador.localizacao.latitude,
            longitude=transformador.localizacao.longitude,
            potencia_kva=transformador.potencia.kva,
            potencia_mva=transformador.potencia.mva,
            potencia_w=transformador.potencia.w,
            tipo_tensao=str(transformador.tipo_tensao),
            subestacao_codigo=transformador.subestacao_codigo,
            distribuidora=transformador.distribuidora,
            ativo=transformador.ativo,
            criado_em=transformador.criado_em,
            atualizado_em=transformador.atualizado_em,
            area_cobertura_geojson=area_cobertura_geojson,
        )
