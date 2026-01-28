"""
Serviço de detecção automática de subestações via clustering geoespacial.

Usa DBSCAN para agrupar pontos de geração distribuída (GD) e identificar
padrões que indicam subestações implícitas na rede.
"""

import logging
import sys
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
from sklearn.cluster import DBSCAN
from sqlalchemy import text
from sqlalchemy.engine import Engine

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import delete_all_rows, table_exists
from core.database import get_engine


def detect_subestacoes_by_clustering(
    engine: Engine,
    distribuidora: str | None = None,
    eps_km: float = 5.0,
    min_samples: int = 3,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Detecta subestações implícitas agrupando pontos de GD por proximidade.
    
    Parâmetros:
    - distribuidora: Filtrar por distribuidora específica (opcional)
    - eps_km: Raio de busca em km (padrão 5 km)
    - min_samples: Mínimo de pontos para formar um cluster (padrão 3)
    - logger: Logger para debug
    
    Retorna:
    - DataFrame com subestações detectadas
    """
    logger = logger or logging.getLogger("geospatial.clustering")
    
    try:
        # Buscar dados de GD com localização geográfica
        gdf = _fetch_gd_locations(engine, distribuidora, logger)
        
        if gdf.empty or len(gdf) < min_samples:
            logger.warning("Insuficientes pontos de GD para clustering.")
            return pd.DataFrame()
        
        # Converter para coordenadas e fazer clustering
        clusters = _run_dbscan_clustering(gdf, eps_km, min_samples)
        
        # Gerar informações de subestações detectadas
        subestacoes_df = _generate_subestacao_records(gdf, clusters, logger)
        
        return subestacoes_df
        
    except Exception as e:
        logger.error(f"Erro ao detectar subestações: {e}", exc_info=True)
        return pd.DataFrame()


def _fetch_gd_locations(
    engine: Engine, 
    distribuidora: str | None, 
    logger: logging.Logger
) -> gpd.GeoDataFrame:
    """
    Busca pontos de geração distribuída com localização.
    """
    where_clause = ""
    params = {}
    
    if distribuidora:
        where_clause = "WHERE distribuidora ILIKE :dist"
        params = {"dist": f"%{distribuidora}%"}
    
    query = text(f"""
        SELECT us.nome, us.potencia_kw, us.latitude, us.longitude, us.fonte,
               gd.distribuidora, gd.classe, gd.sigla_uf
        FROM usinas_siga us
        LEFT JOIN gd_detalhada gd 
            ON UPPER(us.nome) LIKE UPPER(CONCAT('%', gd.distribuidora, '%'))
        {where_clause}
        WHERE us.latitude IS NOT NULL AND us.longitude IS NOT NULL
    """)
    
    try:
        df = pd.read_sql(query, engine, params=params)
        
        if df.empty:
            logger.warning("Nenhum dado de GD encontrado.")
            return gpd.GeoDataFrame()
        
        # Preencher distribuidora se vazia
        df["distribuidora"] = df["distribuidora"].fillna("Desconhecida")
        
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326"
        )
        
        logger.info(f"Carregados {len(gdf)} pontos de GD.")
        return gdf
        
    except Exception as e:
        logger.error(f"Erro ao buscar localizações de GD: {e}")
        return gpd.GeoDataFrame()


def _run_dbscan_clustering(
    gdf: gpd.GeoDataFrame,
    eps_km: float,
    min_samples: int
) -> np.ndarray:
    """
    Executa clustering DBSCAN em coordenadas convertidas para km.
    """
    # Converter para Web Mercator para distâncias em km
    gdf_projected = gdf.to_crs("EPSG:3857")
    
    # Extrair coordenadas
    coords = np.array([
        [geom.x, geom.y] for geom in gdf_projected.geometry
    ])
    
    # Converter eps de km para metros
    eps_meters = eps_km * 1000
    
    # Executar DBSCAN
    clustering = DBSCAN(eps=eps_meters, min_samples=min_samples).fit(coords)
    
    return clustering.labels_


def _generate_subestacao_records(
    gdf: gpd.GeoDataFrame,
    clusters: np.ndarray,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Gera registros de subestações detectadas a partir dos clusters.
    """
    gdf["cluster_id"] = clusters
    
    # Filtrar ruído (cluster_id == -1)
    gdf_clustered = gdf[gdf["cluster_id"] != -1].copy()
    
    if gdf_clustered.empty:
        logger.warning("Nenhum cluster válido encontrado.")
        return pd.DataFrame()
    
    # Agrupar por cluster
    subestacoes = []
    
    for cluster_id in gdf_clustered["cluster_id"].unique():
        cluster_data = gdf_clustered[gdf_clustered["cluster_id"] == cluster_id]
        
        # Calcular centróide
        centroid_lat = cluster_data["latitude"].mean()
        centroid_lon = cluster_data["longitude"].mean()
        
        # Calcular raio máximo do cluster
        center_point = (centroid_lon, centroid_lat)
        max_distance_km = _calculate_max_distance(cluster_data, center_point)
        
        # Agregar informações
        primeiro_registro = cluster_data.iloc[0]
        distribuidora = primeiro_registro.get("distribuidora", "Desconhecida")
        
        # Determinar subsistema (mockado por enquanto)
        subsistema = _infer_subsistema(centroid_lat)
        
        subestacoes.append({
            "cluster_id": int(cluster_id),
            "nome": f"SE_DETECTADA_{cluster_id}",
            "latitude": centroid_lat,
            "longitude": centroid_lon,
            "distribuidora": distribuidora,
            "subsistema": subsistema,
            "quantidade_gd": len(cluster_data),
            "potencia_total_mw": cluster_data["potencia_kw"].sum() / 1000,
            "raio_deteccao_km": round(max_distance_km, 2)
        })
    
    logger.info(f"Detectadas {len(subestacoes)} potenciais subestações.")
    return pd.DataFrame(subestacoes)


def _calculate_max_distance(gdf: gpd.GeoDataFrame, center: Tuple[float, float]) -> float:
    """
    Calcula distância máxima entre centróide e pontos do cluster.
    Usa operações vetorizadas NumPy para melhor performance.
    """
    if gdf.empty:
        return 0.0

    center_lon, center_lat = center

    # Converter para radianos de forma vetorizada
    lon1 = np.radians(center_lon)
    lat1 = np.radians(center_lat)
    lon2 = np.radians(gdf["longitude"].values)
    lat2 = np.radians(gdf["latitude"].values)

    # Fórmula de Haversine vetorizada
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c

    return float(np.max(km)) if len(km) > 0 else 0.0


def _infer_subsistema(latitude: float) -> str:
    """
    Inferir subsistema aproximado por latitude (simplificado).
    """
    if latitude > -15:
        return "NORTE"
    elif latitude > -18:
        return "NORDESTE"
    elif latitude > -28:
        return "SUDESTE"
    else:
        return "SUL"


def load_detected_subestacoes(df: pd.DataFrame, engine: Engine, logger: logging.Logger) -> int:
    """
    Carrega subestações detectadas no banco de dados.
    """
    if df.empty:
        logger.info("Nenhuma subestação detectada para carregar.")
        return 0
    
    try:
        # Criar GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326"
        )
        
        # Limpar dados anteriores
        if table_exists("subestacoes_detectadas", engine):
            delete_all_rows("subestacoes_detectadas", engine)
        
        # Carregar dados
        gdf.to_postgis("subestacoes_detectadas", engine, if_exists="append", index=False)
        
        logger.info(f"Carregadas {len(gdf)} subestações detectadas.")
        return int(len(gdf))
        
    except Exception as e:
        logger.error(f"Erro ao carregar subestações detectadas: {e}")
        return 0
