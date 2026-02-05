"""Mappers for Satelite Infrastructure."""

from ....domain.satelite import (
    AreaCobertura,
    Coordenadas,
    QuotaMensal,
    RequisicaoSatelite,
    TransformadorSatelite,
)


def to_domain_transformador_satelite(row) -> TransformadorSatelite:
    """Convert DB row to TransformadorSatelite domain object."""
    coordenadas = None
    if row.latitude and row.longitude:
        try:
            coordenadas = Coordenadas(
                latitude=row.latitude,
                longitude=row.longitude
            )
            coordenadas.validar()
        except ValueError:
            pass

    return TransformadorSatelite(
        transformador_id=row.id,
        transformador_codigo=row.codigo,
        transformador_nome=row.nome,
        distribuidora=row.distribuidora,
        coordenadas=coordenadas,
        tipo_tensao=row.tipo_tensao,
        area_cobertura=None,
    )


def to_domain_coordenadas(row) -> Coordenadas:
    """Convert DB row to Coordenadas domain object."""
    return Coordenadas(
        latitude=row.latitude,
        longitude=row.longitude
    )


def to_domain_area_cobertura(row) -> AreaCobertura:
    """Convert DB row to AreaCobertura domain object."""
    return AreaCobertura(
        area_m2=row.area_m2,
        area_km2=row.area_km2,
        metodo_calculo=row.metodo_calculo,
        num_consumidores=row.num_consumidores,
        num_vertices=row.num_vertices
    )


def to_domain_requisicao_satelite(row) -> RequisicaoSatelite:
    """Convert DB row to RequisicaoSatelite domain object."""
    return RequisicaoSatelite(
        transformador_id=row.transformador_id,
        subestacao_id=row.subestacao_id,
        fonte_satelite=row.fonte_satelite,
        status=row.status,
        imagem_id=row.imagem_id,
        url_download=row.url_download,
        cobertura_nuvem_percentual=row.cobertura_nuvem_percentual,
        resolucao_metros=row.resolucao_metros,
        tempo_requisicao_ms=row.tempo_requisicao_ms,
        custo_usd_estimado=row.custo_usd_estimado,
        data_requisicao=str(row.data_requisicao) if row.data_requisicao else None,
        data_imagem=str(row.data_imagem) if row.data_imagem else None,
    )


def to_domain_quota_mensal(requisicoes_mes: int, limite_mensal: int = 25000) -> QuotaMensal:
    """Convert data to QuotaMensal domain object."""
    return QuotaMensal(
        requisicoes_mes=requisicoes_mes,
        limite_mensal=limite_mensal
    )


def requisicao_to_response(requisicao: RequisicaoSatelite) -> dict:
    """Convert RequisicaoSatelite domain to API response."""
    return {
        "transformador_id": requisicao.transformador_id,
        "subestacao_id": requisicao.subestacao_id,
        "fonte_satelite": requisicao.fonte_satelite,
        "status": requisicao.status,
        "imagem_id": requisicao.imagem_id,
        "url_download": requisicao.url_download,
        "cobertura_nuvem_percentual": requisicao.cobertura_nuvem_percentual,
        "resolucao_metros": requisicao.resolucao_metros,
        "tempo_requisicao_ms": requisicao.tempo_requisicao_ms,
        "custo_usd_estimado": requisicao.custo_usd_estimado,
        "data_requisicao": requisicao.data_requisicao,
        "data_imagem": requisicao.data_imagem,
    }


def transformador_to_response(trafo: TransformadorSatelite) -> dict:
    """Convert TransformadorSatelite domain to API response."""
    return {
        "transformador_id": trafo.transformador_id,
        "transformador_codigo": trafo.transformador_codigo,
        "transformador_nome": trafo.transformador_nome,
        "distribuidora": trafo.distribuidora,
        "tipo_tensao": trafo.tipo_tensao,
        "coordenadas": {
            "latitude": trafo.coordenadas.latitude,
            "longitude": trafo.coordenadas.longitude,
        } if trafo.coordenadas else None,
        "area_cobertura": {
            "area_m2": trafo.area_cobertura.area_m2,
            "area_km2": trafo.area_cobertura.area_km2,
            "metodo_calculo": trafo.area_cobertura.metodo_calculo,
            "num_consumidores": trafo.area_cobertura.num_consumidores,
            "num_vertices": trafo.area_cobertura.num_vertices,
        } if trafo.area_cobertura else None,
    }
