"""Transformador SQLAlchemy Repository Implementation - Infrastructure Layer.

This is an infrastructure layer implementation of the ITransformadorRepository
interface using SQLAlchemy as the ORM.
"""

import csv
import json
import logging
from io import StringIO
from typing import List, Optional

from sqlalchemy import text

from ....domain.comum.value_objects import Localizacao, Potencia
from ....domain.transformador import (
    AreaCobertura,
    CodigoTransformador,
    ITransformadorRepository,
    NomeTransformador,
    TensaoTipo,
    Transformador,
    TransformadorNotFoundError,
)
from ..shared import BaseRepository


class SQLAlchemyTransformadorRepository(BaseRepository, ITransformadorRepository):
    """
    SQLAlchemy implementation of ITransformadorRepository.

    Responsible for: SELECT queries, database access, mapping raw SQL results
    to Transformador domain entities.
    """

    def __init__(self, engine):
        """Initialize repository with database engine."""
        super().__init__(engine)
        self.logger = logging.getLogger(__name__)

    def obter_por_id(self, transformador_id: int) -> Optional[Transformador]:
        """
        Retrieve a transformador by its ID.

        Returns:
            Transformador domain entity or None if not found
        """
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.id = :trans_id
            """),
                {"trans_id": transformador_id},
            )

            row = result.fetchone()
            if not row:
                self.logger.debug(f"Transformador {transformador_id} not found")
                return None

            return self._map_row_to_entity(row)

    def obter_por_codigo(self, codigo: str) -> Optional[Transformador]:
        """Retrieve a transformador by its code."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.codigo = :codigo
            """),
                {"codigo": codigo},
            )

            row = result.fetchone()
            if not row:
                self.logger.debug(f"Transformador with code {codigo} not found")
                return None

            return self._map_row_to_entity(row)

    def listar_todos(
        self, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """List all transformadores with pagination."""
        offset = pagina * limite

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.ativo = true
                ORDER BY t.id DESC
                LIMIT :limit OFFSET :offset
            """),
                {"limit": limite, "offset": offset},
            )

            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def listar_por_subestacao(
        self, subestacao_codigo: str, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """List all transformadores for a specific substation."""
        offset = pagina * limite

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.subestacao_codigo = :sub_codigo AND t.ativo = true
                ORDER BY t.potencia_kva DESC
                LIMIT :limit OFFSET :offset
            """),
                {
                    "sub_codigo": subestacao_codigo,
                    "limit": limite,
                    "offset": offset,
                },
            )

            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def listar_por_distribuidora(
        self, distribuidora: str, limite: int = 100, pagina: int = 0
    ) -> List[Transformador]:
        """List all transformadores for a specific distribution company."""
        offset = pagina * limite

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.distribuidora ILIKE :dist AND t.ativo = true
                ORDER BY t.codigo
                LIMIT :limit OFFSET :offset
            """),
                {
                    "dist": f"%{distribuidora}%",
                    "limit": limite,
                    "offset": offset,
                },
            )

            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def contar_total(self) -> int:
        """Count total number of transformadores."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM transformadores_aneel WHERE ativo = true")
            )
            return result.scalar() or 0

    def contar_por_subestacao(self, subestacao_codigo: str) -> int:
        """Count transformadores for a specific substation."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT COUNT(*) FROM transformadores_aneel 
                WHERE subestacao_codigo = :sub_codigo AND ativo = true
            """),
                {"sub_codigo": subestacao_codigo},
            )
            return result.scalar() or 0

    def obter_area_cobertura(self, transformador_id: int) -> Optional[AreaCobertura]:
        """Retrieve coverage area for a transformador."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    ST_AsGeoJSON(tac.geom)::text as geojson_area,
                    ST_AsText(tac.geom) as wkt_area
                FROM transformador_area_cobertura tac
                WHERE tac.transformador_codigo = (
                    SELECT codigo FROM transformadores_aneel WHERE id = :trans_id
                ) AND tac.ativo = true
            """),
                {"trans_id": transformador_id},
            )

            row = result.fetchone()
            if not row:
                return None

            return AreaCobertura(geojson=row[0], wkt=row[1])

    def _map_row_to_entity(self, row: tuple) -> Transformador:
        """Map database row to Transformador domain entity."""
        (
            id_,
            codigo,
            nome,
            latitude,
            longitude,
            potencia_kva,
            subestacao_codigo,
            tipo_tensao,
            distribuidora,
            ativo,
            criado_em,
            atualizado_em,
            geojson_ponto,
        ) = row

        try:
            # Create value objects
            codigo_vo = CodigoTransformador(codigo or "UNKNOWN")
            nome_vo = NomeTransformador(nome or "")
            potencia_vo = Potencia(potencia_kva or 0)
            localizacao_vo = Localizacao(
                latitude=float(latitude or 0), longitude=float(longitude or 0)
            )
            tipo_tensao_vo = TensaoTipo(tipo_tensao or "UNKNOWN")

            # Create and return domain entity
            return Transformador(
                id=int(id_),
                codigo=codigo_vo,
                nome=nome_vo,
                potencia=potencia_vo,
                localizacao=localizacao_vo,
                tipo_tensao=tipo_tensao_vo,
                subestacao_codigo=subestacao_codigo,
                distribuidora=distribuidora,
                ativo=bool(ativo),
                criado_em=criado_em,
                atualizado_em=atualizado_em,
            )
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error mapping row to entity: {e}, row: {row}")
            return None

    def obter_bbox_para_satelite(
        self, transformador_id: int, margem_km: float = 2.0
    ) -> Optional[dict]:
        """Get bounding box for satellite imagery with margin."""
        with self.engine.begin() as conn:
            margin_degrees = margem_km / 111.0

            result = conn.execute(
                text("""
                SELECT 
                    t.latitude - :margin as min_lat,
                    t.longitude - :margin as min_lon,
                    t.latitude + :margin as max_lat,
                    t.longitude + :margin as max_lon
                FROM transformadores_aneel t
                WHERE t.id = :trans_id AND t.ativo = true
            """),
                {"trans_id": transformador_id, "margin": margin_degrees},
            )

            row = result.fetchone()
            if not row:
                return None

            return {
                "min_lat": float(row[0]),
                "min_lon": float(row[1]),
                "max_lat": float(row[2]),
                "max_lon": float(row[3]),
            }

    def listar_por_tipo_tensao(
        self, tipo_tensao: str, limite: int = 100, pagina: int = 0
    ) -> list:
        """List transformadores filtered by voltage type."""
        offset = pagina * limite

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.tipo_tensao = :tipo_tensao AND t.ativo = true
                ORDER BY t.codigo
                LIMIT :limit OFFSET :offset
            """),
                {
                    "tipo_tensao": tipo_tensao,
                    "limit": limite,
                    "offset": offset,
                },
            )

            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def contar_por_tipo_tensao(self, tipo_tensao: str) -> int:
        """Count transformadores by voltage type."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT COUNT(*) FROM transformadores_aneel 
                WHERE tipo_tensao = :tipo_tensao AND ativo = true
            """),
                {"tipo_tensao": tipo_tensao},
            )
            return result.scalar() or 0

    def obter_estatisticas_gerais(self) -> dict:
        """Get general statistics about transformadores."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    COUNT(*) as total,
                    COALESCE(SUM(potencia_kva), 0) as potencia_total_kva,
                    COALESCE(AVG(potencia_kva), 0) as potencia_media_kva,
                    COALESCE(MAX(potencia_kva), 0) as potencia_maxima_kva,
                    COALESCE(MIN(potencia_kva), 0) as potencia_minima_kva,
                    SUM(CASE WHEN tipo_tensao = 'BT' THEN 1 ELSE 0 END) as quantidade_bt,
                    SUM(CASE WHEN tipo_tensao = 'MT' THEN 1 ELSE 0 END) as quantidade_mt,
                    SUM(CASE WHEN tipo_tensao = 'AT' THEN 1 ELSE 0 END) as quantidade_at
                FROM transformadores_aneel 
                WHERE ativo = true
            """)
            )

            row = result.fetchone()
            if not row:
                return {}

            return {
                "total": int(row[0]),
                "potencia_total_kva": float(row[1]),
                "potencia_media_kva": float(row[2]),
                "potencia_maxima_kva": float(row[3]),
                "potencia_minima_kva": float(row[4]),
                "quantidade_bt": int(row[5] or 0),
                "quantidade_mt": int(row[6] or 0),
                "quantidade_at": int(row[7] or 0),
            }

    def obter_estatisticas_areas(self) -> dict:
        """Get area-based statistics."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.distribuidora,
                    COUNT(*) as quantidade,
                    COALESCE(SUM(t.potencia_kva), 0) as potencia_total_kva,
                    COALESCE(AVG(t.potencia_kva), 0) as potencia_media_kva
                FROM transformadores_aneel t
                WHERE t.ativo = true
                GROUP BY t.distribuidora
                ORDER BY potencia_total_kva DESC
            """)
            )

            stats_by_area = {}
            for row in result.fetchall():
                distribuidora, quantidade, potencia_total, potencia_media = row
                stats_by_area[str(distribuidora)] = {
                    "quantidade": int(quantidade),
                    "potencia_total_kva": float(potencia_total),
                    "potencia_media_kva": float(potencia_media),
                }

            return {"por_distribuidora": stats_by_area}

    def buscar_por_regiao(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        limite: int = 100,
        pagina: int = 0,
    ) -> list:
        """Search transformadores in geographic region."""
        offset = pagina * limite

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    t.data_criacao,
                    t.data_atualizacao,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.ativo = true
                    AND t.latitude >= :min_lat
                    AND t.latitude <= :max_lat
                    AND t.longitude >= :min_lon
                    AND t.longitude <= :max_lon
                ORDER BY t.id DESC
                LIMIT :limit OFFSET :offset
            """),
                {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                    "limit": limite,
                    "offset": offset,
                },
            )

            return [self._map_row_to_entity(row) for row in result.fetchall()]

    def contar_por_regiao(
        self, min_lat: float, min_lon: float, max_lat: float, max_lon: float
    ) -> int:
        """Count transformadores in geographic region."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT COUNT(*) FROM transformadores_aneel 
                WHERE ativo = true
                    AND latitude >= :min_lat
                    AND latitude <= :max_lat
                    AND longitude >= :min_lon
                    AND longitude <= :max_lon
            """),
                {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                },
            )
            return result.scalar() or 0

    def obter_resumo_consumidores(
        self, transformador_codigo: str
    ) -> Optional[dict]:
        """Get consumer summary (BT/MT/AT counts)."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    SUM(CASE WHEN tipo_tensao = 'BT' THEN 1 ELSE 0 END) as bt_count,
                    SUM(CASE WHEN tipo_tensao = 'MT' THEN 1 ELSE 0 END) as mt_count,
                    SUM(CASE WHEN tipo_tensao = 'AT' THEN 1 ELSE 0 END) as at_count
                FROM consumidor c
                WHERE c.transformador_codigo = :trans_codigo
            """),
                {"trans_codigo": transformador_codigo},
            )

            row = result.fetchone()
            if not row:
                return {"bt_count": 0, "mt_count": 0, "at_count": 0}

            return {
                "bt_count": int(row[0] or 0),
                "mt_count": int(row[1] or 0),
                "at_count": int(row[2] or 0),
            }

    def listar_consumidores_bt(self, transformador_codigo: str, limite: int) -> list:
        """List BT (low voltage) consumers."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("""
                    SELECT 
                        c.id,
                        c.codigo,
                        c.nome,
                        c.tipo_tensao
                    FROM consumidor c
                    WHERE c.transformador_codigo = :trans_codigo 
                        AND c.tipo_tensao = 'BT'
                    LIMIT :limit
                """),
                    {"trans_codigo": transformador_codigo, "limit": limite},
                )

                consumers = []
                for row in result.fetchall():
                    consumers.append(
                        {
                            "id": row[0],
                            "codigo": row[1],
                            "nome": row[2],
                            "tipo_tensao": row[3],
                        }
                    )
                return consumers
        except Exception as e:
            self.logger.warning(f"Error listing BT consumers: {str(e)}")
            return []

    def listar_consumidores_mt(self, transformador_codigo: str, limite: int) -> list:
        """List MT (medium voltage) consumers."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("""
                    SELECT 
                        c.id,
                        c.codigo,
                        c.nome,
                        c.tipo_tensao
                    FROM consumidor c
                    WHERE c.transformador_codigo = :trans_codigo 
                        AND c.tipo_tensao = 'MT'
                    LIMIT :limit
                """),
                    {"trans_codigo": transformador_codigo, "limit": limite},
                )

                consumers = []
                for row in result.fetchall():
                    consumers.append(
                        {
                            "id": row[0],
                            "codigo": row[1],
                            "nome": row[2],
                            "tipo_tensao": row[3],
                        }
                    )
                return consumers
        except Exception as e:
            self.logger.warning(f"Error listing MT consumers: {str(e)}")
            return []

    def listar_consumidores_at(self, transformador_codigo: str, limite: int) -> list:
        """List AT (high voltage) consumers."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("""
                    SELECT 
                        c.id,
                        c.codigo,
                        c.nome,
                        c.tipo_tensao
                    FROM consumidor c
                    WHERE c.transformador_codigo = :trans_codigo 
                        AND c.tipo_tensao = 'AT'
                    LIMIT :limit
                """),
                    {"trans_codigo": transformador_codigo, "limit": limite},
                )

                consumers = []
                for row in result.fetchall():
                    consumers.append(
                        {
                            "id": row[0],
                            "codigo": row[1],
                            "nome": row[2],
                            "tipo_tensao": row[3],
                        }
                    )
                return consumers
        except Exception as e:
            self.logger.warning(f"Error listing AT consumers: {str(e)}")
            return []

    def exportar(self, formato: str = "json") -> Optional[str]:
        """Export all transformadores in specified format."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_codigo,
                    t.tipo_tensao,
                    t.distribuidora,
                    t.ativo,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.ativo = true
                ORDER BY t.id
            """)
            )

            rows = result.fetchall()

            if formato == "json":
                data = []
                for row in rows:
                    data.append(
                        {
                            "id": row[0],
                            "codigo": row[1],
                            "nome": row[2],
                            "latitude": float(row[3]),
                            "longitude": float(row[4]),
                            "potencia_kva": float(row[5]),
                            "subestacao_codigo": row[6],
                            "tipo_tensao": row[7],
                            "distribuidora": row[8],
                            "ativo": row[9],
                        }
                    )
                return json.dumps(data, indent=2, ensure_ascii=False)

            elif formato == "csv":
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(
                    [
                        "id",
                        "codigo",
                        "nome",
                        "latitude",
                        "longitude",
                        "potencia_kva",
                        "subestacao_codigo",
                        "tipo_tensao",
                        "distribuidora",
                        "ativo",
                    ]
                )
                for row in rows:
                    writer.writerow(
                        [
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                        ]
                    )
                return output.getvalue()

            elif formato == "geojson":
                features = []
                for row in rows:
                    features.append(
                        {
                            "type": "Feature",
                            "properties": {
                                "id": row[0],
                                "codigo": row[1],
                                "nome": row[2],
                                "potencia_kva": float(row[5]),
                                "subestacao_codigo": row[6],
                                "tipo_tensao": row[7],
                                "distribuidora": row[8],
                            },
                            "geometry": json.loads(row[10]),
                        }
                    )

                return json.dumps(
                    {"type": "FeatureCollection", "features": features},
                    indent=2,
                    ensure_ascii=False,
                )

            return None
