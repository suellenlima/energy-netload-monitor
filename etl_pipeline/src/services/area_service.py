"""
Serviço de Áreas de Cobertura
Funções reutilizáveis para consultar e exportar áreas de transformadores e subestações
"""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


class AreaService:
    """Serviço para consultar e exportar áreas de transformadores/subestações"""
    
    def __init__(self, engine):
        self.engine = engine
    
    # ========================================================================
    # TRANSFORMADORES
    # ========================================================================
    
    def obter_area_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca a área de cobertura de um transformador"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_id,
                    COALESCE(tac.area_km2, 0.25) as area_km2,
                    COALESCE(tac.raio_aproximado_m, 500) as raio_m,
                    ST_AsText(COALESCE(tac.area_cobertura, 
                        ST_Buffer(t.localizacao::geography, 500)::geometry)) as wkt_area,
                    ST_AsGeoJSON(COALESCE(tac.area_cobertura, 
                        ST_Buffer(t.localizacao::geography, 500)::geometry)) as geojson_area,
                    (SELECT COUNT(*) FROM consumidores 
                     WHERE transformador_id = :trans_id AND status = 'ativo') as consumidores
                FROM transformadores t
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                WHERE t.id = :trans_id AND t.status = 'ativo'
            """), {'trans_id': transformador_id})
            
            row = result.fetchone()
            if not row:
                logger.error(f"❌ Transformador {transformador_id} não encontrado")
                return None
            
            return {
                'id': row[0],
                'codigo': row[1],
                'nome': row[2],
                'latitude': float(row[3]),
                'longitude': float(row[4]),
                'potencia_kva': float(row[5]),
                'subestacao_id': row[6],
                'area_km2': float(row[7]),
                'raio_m': float(row[8]),
                'wkt_area': row[9],
                'geojson_area': row[10],
                'consumidores': row[11]
            }
    
    def obter_bbox_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Retorna bounding box para busca de satélite"""
        
        trans_data = self.obter_area_transformador(transformador_id)
        if not trans_data:
            return None
        
        # Calcular delta baseado no raio
        raio_km = trans_data['raio_m'] / 1000
        delta = raio_km / 111.0  # 1 grau ≈ 111 km
        
        lat = trans_data['latitude']
        lon = trans_data['longitude']
        
        return {
            'min_lat': lat - delta,
            'min_lon': lon - delta,
            'max_lat': lat + delta,
            'max_lon': lon + delta,
            'transformador_id': trans_data['id'],
            'transformador_nome': trans_data['nome']
        }
    
    def listar_transformadores_subestacao(self, subestacao_id: int) -> pd.DataFrame:
        """Lista todos os transformadores de uma subestação com suas áreas"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    COALESCE(tac.area_km2, 0.25) as area_km2,
                    COALESCE(tac.raio_aproximado_m, 500) as raio_m,
                    COALESCE(tac.metodo_definicao, 'raio_fixo') as metodo,
                    (SELECT COUNT(*) FROM consumidores 
                     WHERE transformador_id = t.id AND status = 'ativo') as consumidores
                FROM transformadores t
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                WHERE t.subestacao_id = :sub_id AND t.status = 'ativo'
                ORDER BY t.nome
            """), {'sub_id': subestacao_id})
            
            df = pd.DataFrame(result.fetchall(), columns=[
                'id', 'codigo', 'nome', 'latitude', 'longitude', 'potencia_kva',
                'area_km2', 'raio_m', 'metodo', 'consumidores'
            ])
            
            return df
    
    def listar_todas_transformadores(self) -> pd.DataFrame:
        """Lista todos os transformadores com suas áreas"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    t.subestacao_id,
                    COALESCE(tac.area_km2, 0.25) as area_km2,
                    COALESCE(tac.raio_aproximado_m, 500) as raio_m,
                    COALESCE(tac.metodo_definicao, 'raio_fixo') as metodo,
                    se.nome as subestacao,
                    se.subsistema,
                    (SELECT COUNT(*) FROM consumidores 
                     WHERE transformador_id = t.id AND status = 'ativo') as consumidores
                FROM transformadores t
                JOIN subestacoes_detectadas se ON se.id = t.subestacao_id
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                WHERE t.status = 'ativo'
                ORDER BY t.subestacao_id, t.nome
            """))
            
            df = pd.DataFrame(result.fetchall(), columns=[
                'id', 'codigo', 'nome', 'latitude', 'longitude', 'potencia_kva',
                'subestacao_id', 'area_km2', 'raio_m', 'metodo', 'subestacao',
                'subsistema', 'consumidores'
            ])
            
            return df
    
    # ========================================================================
    # SUBESTAÇÕES
    # ========================================================================
    
    def obter_area_subestacao(self, subestacao_id: int) -> Optional[Dict]:
        """Busca a área de cobertura de uma subestação"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    se.id,
                    se.nome,
                    se.latitude,
                    se.longitude,
                    se.subsistema,
                    COALESCE(sac.area_km2, 0) as area_km2,
                    sac.metodo_definicao,
                    ST_AsText(sac.area_cobertura) as wkt_area,
                    ST_AsGeoJSON(sac.area_cobertura) as geojson_area,
                    (SELECT COUNT(*) FROM transformadores 
                     WHERE subestacao_id = :sub_id AND status = 'ativo') as total_transformadores
                FROM subestacoes_detectadas se
                LEFT JOIN subestacoes_area_cobertura sac ON sac.subestacao_id = se.id
                WHERE se.id = :sub_id
            """), {'sub_id': subestacao_id})
            
            row = result.fetchone()
            if not row:
                logger.error(f"❌ Subestação {subestacao_id} não encontrada")
                return None
            
            return {
                'id': row[0],
                'nome': row[1],
                'latitude': float(row[2]) if row[2] else None,
                'longitude': float(row[3]) if row[3] else None,
                'subsistema': row[4],
                'area_km2': float(row[5]),
                'metodo_definicao': row[6],
                'wkt_area': row[7],
                'geojson_area': row[8],
                'total_transformadores': row[9]
            }
    
    def listar_subestacoes_com_areas(self) -> pd.DataFrame:
        """Lista subestações com estatísticas de suas áreas"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    se.id,
                    se.nome,
                    se.fonte_dados,
                    se.subsistema,
                    se.latitude,
                    se.longitude,
                    sac.area_km2,
                    sac.metodo_definicao,
                    COUNT(DISTINCT t.id) as total_transformadores,
                    COUNT(DISTINCT tac.id) as transformadores_com_area,
                    ROUND(AVG(tac.area_km2)::numeric, 2) as area_media_transformador_km2,
                    ROUND(SUM(tac.area_km2)::numeric, 2) as area_total_transformadores_km2
                FROM subestacoes_detectadas se
                LEFT JOIN subestacoes_area_cobertura sac ON sac.subestacao_id = se.id
                LEFT JOIN transformadores t ON t.subestacao_id = se.id AND t.status = 'ativo'
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                GROUP BY se.id, se.nome, se.fonte_dados, se.subsistema, se.latitude, 
                         se.longitude, sac.area_km2, sac.metodo_definicao
                ORDER BY se.nome
            """))
            
            df = pd.DataFrame(result.fetchall(), columns=[
                'id', 'nome', 'fonte_dados', 'subsistema', 'latitude', 'longitude',
                'area_km2', 'metodo_definicao', 'total_transformadores', 
                'transformadores_com_area', 'area_media_transformador_km2',
                'area_total_transformadores_km2'
            ])
            
            return df
    
    # ========================================================================
    # EXPORTAÇÃO
    # ========================================================================
    
    def exportar_transformadores(self, formato: str = 'csv', output_file: str = None) -> Optional[str]:
        """
        Exporta transformadores com suas áreas.
        Formatos: csv, geojson, json
        """
        
        df = self.listar_todas_transformadores()
        
        if formato == 'csv':
            if output_file:
                df.to_csv(output_file, index=False)
            return output_file or df.to_csv(index=False)
        
        elif formato == 'geojson':
            try:
                import geopandas as gpd
                geometry = gpd.points_from_xy(df.longitude, df.latitude)
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
                if output_file:
                    gdf.to_file(output_file, driver='GeoJSON')
                return output_file or gdf.to_json()
            except ImportError:
                logger.error("GeoPandas não instalado para GeoJSON")
                return None
        
        elif formato == 'json':
            import json
            return json.dumps(df.to_dict('records'), default=str)
        
        else:
            logger.error(f"Formato desconhecido: {formato}")
            return None
    
    def exportar_subestacoes(self, formato: str = 'csv', output_file: str = None) -> Optional[str]:
        """Exporta subestações com estatísticas de áreas"""
        
        df = self.listar_subestacoes_com_areas()
        
        if formato == 'csv':
            if output_file:
                df.to_csv(output_file, index=False)
            return output_file or df.to_csv(index=False)
        
        elif formato == 'json':
            import json
            return json.dumps(df.to_dict('records'), default=str)
        
        else:
            logger.error(f"Formato desconhecido: {formato}")
            return None
    
    # ========================================================================
    # BUSCAS ESPACIAIS
    # ========================================================================
    
    def buscar_transformadores_por_regiao(
        self, 
        min_lat: float, 
        min_lon: float, 
        max_lat: float, 
        max_lon: float
    ) -> pd.DataFrame:
        """Buscar transformadores dentro de um bounding box"""
        
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    t.id,
                    t.codigo,
                    t.nome,
                    t.latitude,
                    t.longitude,
                    t.potencia_kva,
                    COALESCE(tac.area_km2, 0.25) as area_km2,
                    COALESCE(tac.raio_aproximado_m, 500) as raio_m
                FROM transformadores t
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                WHERE t.latitude BETWEEN :min_lat AND :max_lat
                  AND t.longitude BETWEEN :min_lon AND :max_lon
                  AND t.status = 'ativo'
                ORDER BY t.nome
            """), {
                'min_lat': min_lat, 'min_lon': min_lon,
                'max_lat': max_lat, 'max_lon': max_lon
            })
            
            df = pd.DataFrame(result.fetchall(), columns=[
                'id', 'codigo', 'nome', 'latitude', 'longitude', 
                'potencia_kva', 'area_km2', 'raio_m'
            ])
            
            return df
    
    # ========================================================================
    # ESTATÍSTICAS
    # ========================================================================
    
    def obter_estatisticas_areas(self) -> Dict:
        """Retorna estatísticas gerais de áreas"""
        
        with self.engine.begin() as conn:
            # Estatísticas de transformadores
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_transformadores,
                    COUNT(DISTINCT CASE WHEN tac.id IS NOT NULL THEN t.id END) as com_area,
                    ROUND(AVG(tac.area_km2)::numeric, 2) as area_media_km2,
                    ROUND(MIN(tac.area_km2)::numeric, 2) as area_minima_km2,
                    ROUND(MAX(tac.area_km2)::numeric, 2) as area_maxima_km2,
                    ROUND(SUM(tac.area_km2)::numeric, 2) as area_total_km2
                FROM transformadores t
                LEFT JOIN transformadores_area_cobertura tac ON tac.transformador_id = t.id
                WHERE t.status = 'ativo'
            """))
            
            row = result.fetchone()
            
            return {
                'total_transformadores': row[0],
                'com_area_calculada': row[1],
                'percentual_com_area': round(100.0 * (row[1] or 0) / (row[0] or 1), 2),
                'area_media_km2': float(row[2]) if row[2] else 0,
                'area_minima_km2': float(row[3]) if row[3] else 0,
                'area_maxima_km2': float(row[4]) if row[4] else 0,
                'area_total_km2': float(row[5]) if row[5] else 0
            }
