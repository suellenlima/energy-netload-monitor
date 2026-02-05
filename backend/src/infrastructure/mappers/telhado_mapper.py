"""Telhado mapper for DTO conversion."""

from src.domain.telhado import Telhado
from src.domain.telhado.value_objects import (
    AreaTelhado,
    CodigoTelhado,
    InclinacaoTelhado,
    Orientacao,
)
from src.domain.comum.value_objects import Localizacao


class TelhadoMapper:
    """Map Telhado entity to/from DTO."""

    @staticmethod
    def to_detail_response(telhado: Telhado) -> dict:
        """Map entity to detailed response."""
        return {
            "id": telhado.id,
            "codigo": str(telhado.codigo),
            "latitude": telhado.localizacao.latitude,
            "longitude": telhado.localizacao.longitude,
            "area_m2": telhado.area.valor,
            "inclinacao_graus": telhado.inclinacao.valor,
            "orientacao": str(telhado.orientacao),
            "confianca_deteccao": telhado.confianca_deteccao,
            "eh_alta_confianca": telhado.eh_alta_confianca(),
            "transformador_id": telhado.transformador_id,
            "consumidor_id": telhado.consumidor_id,
            "criado_em": telhado.criado_em.isoformat() if telhado.criado_em else None,
            "atualizado_em": telhado.atualizado_em.isoformat()
            if telhado.atualizado_em
            else None,
        }

    @staticmethod
    def to_list_response(telhado: Telhado) -> dict:
        """Map entity to list item response."""
        return {
            "id": telhado.id,
            "codigo": str(telhado.codigo),
            "latitude": telhado.localizacao.latitude,
            "longitude": telhado.localizacao.longitude,
            "area_m2": telhado.area.valor,
            "orientacao": str(telhado.orientacao),
            "confianca_deteccao": telhado.confianca_deteccao,
            "eh_alta_confianca": telhado.eh_alta_confianca(),
        }
