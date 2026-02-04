"""
Repository para operações de banco de dados de Transformadores
"""

import logging
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from .base import BaseRepository


class TransformadorRepository(BaseRepository):
    """
    Repositório para acesso aos dados de transformadores no banco de dados.
    Responsável por: SELECT, filtros, consultas SQL.
    """

    def obter_por_id(self, transformador_id: int) -> Optional[Dict]:
        """Busca transformador por ID com dados de área"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
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
                    ST_AsText(t.localizacao) as wkt_ponto,
                    ST_AsGeoJSON(t.localizacao)::text as geojson_ponto
                FROM transformadores_aneel t
                WHERE t.id = :trans_id AND t.ativo = true
            """), {'trans_id': transformador_id})
            
            row = result.fetchone()
            if not row:
                self.logger.debug(f"Transformador {transformador_id} não encontrado")
                return None
            
            return {
                'id': row[0],
                'codigo': row[1],
                'nome': row[2],
                'latitude': float(row[3]) if row[3] else None,
                'longitude': float(row[4]) if row[4] else None,
                'potencia_kva': float(row[5]) if row[5] else None,
                'subestacao_codigo': row[6],
                'tipo_tensao': row[7],
                'distribuidora': row[8],
                'wkt_ponto': row[9],
                'geojson_ponto': row[10]
            }

    def obter_area_cobertura(self, transformador_id: int) -> Optional[Dict]:
        """Busca a área de cobertura de um transformador"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    tac.id,
                    tac.transformador_codigo,
                    tac.tipo_tensao,
                    tac.metodo_calculo,
                    tac.area_km2,
                    tac.area_m2,
                    tac.num_consumidores,
                    tac.num_vertices,
                    ST_AsText(tac.geom) as wkt_area,
                    ST_AsGeoJSON(tac.geom)::text as geojson_area
                FROM transformador_area_cobertura tac
                WHERE tac.transformador_codigo = (
                    SELECT codigo FROM transformadores_aneel WHERE id = :trans_id
                ) AND tac.ativo = true
            """), {'trans_id': transformador_id})
            
            row = result.fetchone()
            if not row:
                return None
            
            return {
                'id': row[0],
                'transformador_codigo': row[1],
                'tipo_tensao': row[2],
                'metodo_calculo': row[3],
                'area_km2': float(row[4]) if row[4] else None,
                'area_m2': float(row[5]) if row[5] else None,
                'num_consumidores': row[6],
                'num_vertices': row[7],
                'wkt_area': row[8],
                'geojson_area': row[9]
            }

    def listar_todos(self, skip: int = 0, limit: int = 100) -> pd.DataFrame:
        """Lista todos os transformadores ativos com paginação"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    id, codigo, nome, latitude, longitude, potencia_kva,
                    tipo_tensao, subestacao_codigo, distribuidora, ativo
                FROM transformadores_aneel
                WHERE ativo = true
                ORDER BY id DESC
                LIMIT :limit OFFSET :skip
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'skip': skip, 'limit': limit}
            )

    def listar_por_subestacao(self, subestacao_codigo: str, skip: int = 0, limit: int = 100) -> pd.DataFrame:
        """Lista transformadores por subestação"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    id, codigo, nome, latitude, longitude, potencia_kva,
                    tipo_tensao, subestacao_codigo, distribuidora, ativo
                FROM transformadores_aneel
                WHERE subestacao_codigo = :sub_codigo AND ativo = true
                ORDER BY potencia_kva DESC
                LIMIT :limit OFFSET :skip
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'sub_codigo': subestacao_codigo, 'skip': skip, 'limit': limit}
            )

    def listar_por_distribuidora(self, distribuidora: str, skip: int = 0, limit: int = 100) -> pd.DataFrame:
        """Lista transformadores por distribuidora"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    id, codigo, nome, latitude, longitude, potencia_kva,
                    tipo_tensao, subestacao_codigo, distribuidora, ativo
                FROM transformadores_aneel
                WHERE distribuidora ILIKE :dist AND ativo = true
                ORDER BY codigo
                LIMIT :limit OFFSET :skip
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'dist': f'%{distribuidora}%', 'skip': skip, 'limit': limit}
            )

    def buscar_por_regiao(self, min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> pd.DataFrame:
        """Busca transformadores dentro de um bounding box"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    id, codigo, nome, latitude, longitude, potencia_kva,
                    tipo_tensao, subestacao_codigo, distribuidora, ativo
                FROM transformadores_aneel
                WHERE latitude BETWEEN :min_lat AND :max_lat
                  AND longitude BETWEEN :min_lon AND :max_lon
                  AND ativo = true
                ORDER BY potencia_kva DESC
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={
                    'min_lat': min_lat,
                    'max_lat': max_lat,
                    'min_lon': min_lon,
                    'max_lon': max_lon
                }
            )

    def buscar_por_tipo_tensao(self, tipo_tensao: str, skip: int = 0, limit: int = 100) -> pd.DataFrame:
        """Lista transformadores por tipo de tensão (BT, MT, AT)"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    id, codigo, nome, latitude, longitude, potencia_kva,
                    tipo_tensao, subestacao_codigo, distribuidora, ativo
                FROM transformadores_aneel
                WHERE tipo_tensao = :tipo_tensao AND ativo = true
                ORDER BY potencia_kva DESC
                LIMIT :limit OFFSET :skip
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'tipo_tensao': tipo_tensao, 'skip': skip, 'limit': limit}
            )

    def contar_total(self) -> int:
        """Retorna total de transformadores ativos"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM transformadores_aneel WHERE ativo = true
            """))
            return result.scalar()

    def contar_por_subestacao(self, subestacao_codigo: str) -> int:
        """Retorna total de transformadores em uma subestação"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM transformadores_aneel 
                WHERE subestacao_codigo = :sub_codigo AND ativo = true
            """), {'sub_codigo': subestacao_codigo})
            return result.scalar()

    def contar_por_distribuidora(self, distribuidora: str) -> int:
        """Retorna total de transformadores em uma distribuidora"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM transformadores_aneel 
                WHERE distribuidora ILIKE :dist AND ativo = true
            """), {'dist': f'%{distribuidora}%'})
            return result.scalar()

    def obter_estadisticas_gerais(self) -> Dict:
        """Retorna estatísticas gerais de transformadores"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN tipo_tensao = 'BT' THEN 1 END) as total_bt,
                    COUNT(CASE WHEN tipo_tensao = 'MT' THEN 1 END) as total_mt,
                    COUNT(CASE WHEN tipo_tensao = 'AT' THEN 1 END) as total_at,
                    COUNT(DISTINCT subestacao_codigo) as total_subestacoes,
                    COUNT(DISTINCT distribuidora) as total_distribuidoras,
                    ROUND(AVG(potencia_kva)::numeric, 2) as potencia_media_kva,
                    MIN(potencia_kva) as potencia_minima_kva,
                    MAX(potencia_kva) as potencia_maxima_kva,
                    SUM(potencia_kva) as potencia_total_kva
                FROM transformadores_aneel 
                WHERE ativo = true
            """))
            
            row = result.fetchone()
            return {
                'total': row[0],
                'total_bt': row[1],
                'total_mt': row[2],
                'total_at': row[3],
                'total_subestacoes': row[4],
                'total_distribuidoras': row[5],
                'potencia_media_kva': float(row[6]) if row[6] else 0,
                'potencia_minima_kva': float(row[7]) if row[7] else 0,
                'potencia_maxima_kva': float(row[8]) if row[8] else 0,
                'potencia_total_kva': float(row[9]) if row[9] else 0
            }

    def obter_estatisticas_areas(self) -> Dict:
        """Retorna estatísticas de áreas de cobertura"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_areas,
                    COUNT(CASE WHEN metodo_calculo = 'convex_hull' THEN 1 END) as areas_convex_hull,
                    COUNT(CASE WHEN metodo_calculo LIKE 'buffer_%' THEN 1 END) as areas_buffer,
                    ROUND(AVG(area_km2)::numeric, 4) as area_media_km2,
                    MIN(area_km2) as area_minima_km2,
                    MAX(area_km2) as area_maxima_km2,
                    ROUND(SUM(area_km2)::numeric, 2) as area_total_km2
                FROM transformador_area_cobertura 
                WHERE ativo = true
            """))
            
            row = result.fetchone()
            return {
                'total_areas': row[0] or 0,
                'areas_convex_hull': row[1] or 0,
                'areas_buffer': row[2] or 0,
                'area_media_km2': float(row[3]) if row[3] else 0,
                'area_minima_km2': float(row[4]) if row[4] else 0,
                'area_maxima_km2': float(row[5]) if row[5] else 0,
                'area_total_km2': float(row[6]) if row[6] else 0
            }

    def exportar_como_dataframe(self) -> pd.DataFrame:
        """Exporta todos os transformadores como DataFrame"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    t.id, t.codigo, t.nome, t.latitude, t.longitude,
                    t.potencia_kva, t.tipo_tensao, t.subestacao_codigo, 
                    t.distribuidora, t.data_criacao, t.data_atualizacao,
                    tac.area_km2, tac.metodo_calculo, tac.num_consumidores
                FROM transformadores_aneel t
                LEFT JOIN transformador_area_cobertura tac ON tac.transformador_codigo = t.codigo
                WHERE t.ativo = true
                ORDER BY t.id
            """
            return pd.read_sql_query(text(query), conn)

    def contar_consumidores_por_transformador(self, transformador_codigo: str) -> Dict:
        """Conta consumidores BT/MT/AT associados a um transformador"""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    (SELECT COUNT(*) FROM consumidores_bt_aneel 
                     WHERE transformador_mt_codigo = :codigo AND ativo = true) as consumidores_bt,
                    (SELECT COUNT(*) FROM consumidores_mt_aneel 
                     WHERE circuito_mt_codigo = :codigo AND ativo = true) as consumidores_mt,
                    (SELECT COUNT(*) FROM consumidores_at_aneel 
                     WHERE circuito_at_codigo = :codigo AND ativo = true) as consumidores_at
            """), {'codigo': transformador_codigo})
            
            row = result.fetchone()
            return {
                'consumidores_bt': row[0] or 0,
                'consumidores_mt': row[1] or 0,
                'consumidores_at': row[2] or 0,
                'total_consumidores': (row[0] or 0) + (row[1] or 0) + (row[2] or 0)
            }

    def obter_consumidores_bt_por_transformador(self, transformador_codigo: str, limit: int = 100) -> pd.DataFrame:
        """Lista consumidores BT associados a um transformador"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    codigo, municipio_codigo, carga_instalada_kw, 
                    classe_subclasse_codigo, situacao_ativacao_codigo,
                    latitude, longitude, data_conexao
                FROM consumidores_bt_aneel
                WHERE transformador_mt_codigo = :codigo AND ativo = true
                ORDER BY carga_instalada_kw DESC
                LIMIT :limit
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'codigo': transformador_codigo, 'limit': limit}
            )

    def obter_consumidores_mt_por_transformador(self, transformador_codigo: str, limit: int = 100) -> pd.DataFrame:
        """Lista consumidores MT associados a um transformador"""
        with self.engine.begin() as conn:
            query = """
                SELECT 
                    codigo, municipio_codigo, carga_instalada_kw, demanda_contratada_kw,
                    classe_subclasse_codigo, situacao_ativacao_codigo,
                    latitude, longitude, data_conexao
                FROM consumidores_mt_aneel
                WHERE circuito_mt_codigo = :codigo AND ativo = true
                ORDER BY demanda_contratada_kw DESC
                LIMIT :limit
            """
            return pd.read_sql_query(
                text(query),
                conn,
                params={'codigo': transformador_codigo, 'limit': limit}
            )

