"""SQLAlchemy implementation of Telhado repository."""

import logging
from typing import List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.domain.telhado import (
    ITelhadoRepository,
    Telhado,
)
from src.domain.telhado.value_objects import (
    AreaTelhado,
    CodigoTelhado,
    InclinacaoTelhado,
    Orientacao,
)
from src.domain.comum.value_objects import Localizacao


class SQLAlchemyTelhadoRepository(ITelhadoRepository):
    """SQLAlchemy implementation of roof repository."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.logger = logging.getLogger(__name__)

    def obter_por_id(self, telhado_id: int) -> Optional[Telhado]:
        """Get a roof by ID."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    id, transformador_id, latitude, longitude, area_m2, 
                    confianca, timestamp_criacao, timestamp_atualizacao
                FROM telhados_detectados_transformador
                WHERE id = :telhado_id
            """),
                {"telhado_id": telhado_id},
            )
            row = result.fetchone()
            if not row:
                return None
            return self._map_row_to_entity(row)

    def listar_todos(
        self, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List all roofs with pagination."""
        with self.engine.begin() as conn:
            # Get total count
            count_result = conn.execute(text("SELECT COUNT(*) FROM telhados_detectados_transformador"))
            total = count_result.scalar() or 0

            # Get paginated results
            offset = pagina * limite
            result = conn.execute(
                text("""
                SELECT 
                    id, transformador_id, latitude, longitude, area_m2, 
                    confianca, timestamp_criacao, timestamp_atualizacao
                FROM telhados_detectados_transformador
                ORDER BY id
                LIMIT :limite OFFSET :offset
            """),
                {"limite": limite, "offset": offset},
            )

            roofs = [self._map_row_to_entity(row) for row in result.fetchall()]
            return roofs, total

    def listar_por_transformador(
        self, transformador_id: int, limite: int = 100
    ) -> List[Telhado]:
        """List roofs for a specific transformer."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    id, transformador_id, latitude, longitude, area_m2, 
                    confianca, timestamp_criacao, timestamp_atualizacao
                FROM telhados_detectados_transformador
                WHERE transformador_id = :transformador_id
                LIMIT :limite
            """),
                {"transformador_id": transformador_id, "limite": limite},
            )
            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def listar_por_confianca(
        self, min_confianca: float = 0.8, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs with minimum confidence level."""
        with self.engine.begin() as conn:
            # Get total count
            count_result = conn.execute(
                text("SELECT COUNT(*) FROM telhados_detectados_transformador WHERE confianca >= :min_confianca"),
                {"min_confianca": min_confianca},
            )
            total = count_result.scalar() or 0

            # Get paginated results
            offset = pagina * limite
            result = conn.execute(
                text("""
                SELECT 
                    id, transformador_id, latitude, longitude, area_m2, 
                    confianca, timestamp_criacao, timestamp_atualizacao
                FROM telhados_detectados_transformador
                WHERE confianca >= :min_confianca
                ORDER BY confianca DESC
                LIMIT :limite OFFSET :offset
            """),
                {
                    "min_confianca": min_confianca,
                    "limite": limite,
                    "offset": offset,
                },
            )

            roofs = [self._map_row_to_entity(row) for row in result.fetchall()]
            return roofs, total

    def listar_por_area(
        self, min_area: float, max_area: float, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs within area range."""
        with self.engine.begin() as conn:
            # Get total count
            count_result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM telhados_detectados_transformador WHERE area_m2 BETWEEN :min_area AND :max_area"
                ),
                {"min_area": min_area, "max_area": max_area},
            )
            total = count_result.scalar() or 0

            # Get paginated results
            offset = pagina * limite
            result = conn.execute(
                text("""
                SELECT 
                    id, transformador_id, latitude, longitude, area_m2, 
                    confianca, timestamp_criacao, timestamp_atualizacao
                FROM telhados_detectados_transformador
                WHERE area_m2 BETWEEN :min_area AND :max_area
                ORDER BY area_m2 DESC
                LIMIT :limite OFFSET :offset
            """),
                {
                    "min_area": min_area,
                    "max_area": max_area,
                    "limite": limite,
                    "offset": offset,
                },
            )

            roofs = [self._map_row_to_entity(row) for row in result.fetchall()]
            return roofs, total

    def listar_por_orientacao(
        self, orientacao: str, limite: int = 100, pagina: int = 0
    ) -> Tuple[List[Telhado], int]:
        """List roofs with specific orientation (simplified - returns all)."""
        # Note: Since the table doesn't have orientation column,
        # we return all with reasonable confidence for demo purposes
        return self.listar_por_confianca(min_confianca=0.0, limite=limite, pagina=pagina)

    def obter_estatisticas(self) -> dict:
        """Get statistics about roofs."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    COUNT(*) as total,
                    AVG(area_m2) as area_media,
                    MAX(area_m2) as area_maxima,
                    MIN(area_m2) as area_minima,
                    AVG(confianca) as confianca_media,
                    SUM(CASE WHEN confianca >= 0.8 THEN 1 ELSE 0 END) as alta_confianca,
                    SUM(CASE WHEN transformador_id IS NOT NULL THEN 1 ELSE 0 END) as com_transformador
                FROM telhados_detectados_transformador
            """)
            )
            row = result.fetchone()

            return {
                "total": row[0] or 0,
                "area_media_m2": float(row[1]) if row[1] else 0,
                "area_maxima_m2": float(row[2]) if row[2] else 0,
                "area_minima_m2": float(row[3]) if row[3] else 0,
                "confianca_media": float(row[4]) if row[4] else 0,
                "alta_confianca_count": row[5] or 0,
                "com_transformador_count": row[6] or 0,
            }

    def _map_row_to_entity(self, row: tuple) -> Telhado:
        """Map database row to Telhado entity."""
        (
            id_,
            transformador_id,
            latitude,
            longitude,
            area_m2,
            confianca,
            timestamp_criacao,
            timestamp_atualizacao,
        ) = row

        try:
            # Generate a simple codigo from ID
            codigo_vo = CodigoTelhado(f"TELHADO_{id_}")
            area_vo = AreaTelhado(area_m2 or 0)
            inclinacao_vo = InclinacaoTelhado(0)  # Not available in table
            orientacao_vo = Orientacao("N")  # Default orientation since not in table
            localizacao_vo = Localizacao(
                latitude=float(latitude or 0), longitude=float(longitude or 0)
            )

            return Telhado(
                id=int(id_),
                codigo=codigo_vo,
                localizacao=localizacao_vo,
                area=area_vo,
                inclinacao=inclinacao_vo,
                orientacao=orientacao_vo,
                confianca_deteccao=float(confianca or 0),
                transformador_id=transformador_id,
                consumidor_id=None,  # Not available in table
                criado_em=timestamp_criacao,
                atualizado_em=timestamp_atualizacao,
            )
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error mapping row to entity: {e}, row: {row}")
            return None
