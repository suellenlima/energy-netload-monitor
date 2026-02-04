"""
Serviço de detecção automática de subestações via clustering geoespacial.

Usa DBSCAN para agrupar pontos de geração distribuída (GD) e identificar
padrões que indicam subestações implícitas na rede.
"""

import logging
import sys
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core import delete_all_rows, table_exists


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


def associate_ucs_to_nearest_subestacao(
    engine: Engine,
    raio_km: float = 10.0,
    origem: str = "detectadas",
    logger: logging.Logger | None = None
) -> dict:
    """
    Associa cada UC (gd_granular) à subestação mais próxima.

    Args:
        engine: Engine do SQLAlchemy
        raio_km: Raio máximo de busca em km
        origem: 'detectadas', 'ons' ou 'ambas'
        logger: Logger para mensagens

    Returns:
        Dict com estatísticas da associação
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"Associando UCs à subestação mais próxima (raio: {raio_km}km)")

    try:
        # Determinar tabela de subestações
        tabela_subestacao = {
            "detectadas": "subestacoes_detectadas",
            "ons": "subestacoes_ons",
            "ambas": "subestacoes_detectadas"  # Prioriza detectadas
        }.get(origem, "subestacoes_detectadas")

        # Query para associação espacial
        query = text(f"""
            WITH distancias AS (
                SELECT
                    g.id as uc_id,
                    s.id as subestacao_id,
                    ST_Distance(g.geom::geography, s.geom::geography) / 1000.0 as distancia_km,
                    ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY ST_Distance(g.geom::geography, s.geom::geography)) as rn
                FROM gd_granular g
                CROSS JOIN {tabela_subestacao} s
                WHERE g.geom IS NOT NULL
                  AND s.geom IS NOT NULL
                  AND ST_DWithin(g.geom::geography, s.geom::geography, :raio_metros)
            )
            UPDATE gd_granular
            SET subestacao_id = d.subestacao_id
            FROM distancias d
            WHERE gd_granular.id = d.uc_id
              AND d.rn = 1
            RETURNING gd_granular.id;
        """)

        with engine.begin() as conn:
            result = conn.execute(query, {"raio_metros": raio_km * 1000})
            ucs_associadas = result.rowcount

        # Buscar estatísticas
        stats_query = text("""
            SELECT
                COUNT(*) FILTER (WHERE subestacao_id IS NOT NULL) as ucs_associadas,
                COUNT(*) FILTER (WHERE subestacao_id IS NULL AND geom IS NOT NULL) as ucs_sem_subestacao,
                COUNT(*) FILTER (WHERE geom IS NULL) as ucs_sem_coordenadas,
                COUNT(*) as total_ucs
            FROM gd_granular
        """)

        with engine.connect() as conn:
            stats = conn.execute(stats_query).fetchone()

        resultado = {
            "ucs_associadas": int(stats.ucs_associadas or 0),
            "ucs_sem_subestacao": int(stats.ucs_sem_subestacao or 0),
            "ucs_sem_coordenadas": int(stats.ucs_sem_coordenadas or 0),
            "total_ucs": int(stats.total_ucs or 0),
            "percentual_associado": round(
                (stats.ucs_associadas / stats.total_ucs * 100) if stats.total_ucs > 0 else 0,
                2
            ),
            "raio_km": raio_km,
            "origem": origem
        }

        logger.info(
            f"Associação concluída: {resultado['ucs_associadas']}/{resultado['total_ucs']} UCs "
            f"({resultado['percentual_associado']}%)"
        )

        return resultado

    except Exception as exc:
        logger.error(f"Erro ao associar UCs: {exc}", exc_info=True)
        return {
            "ucs_associadas": 0,
            "error": str(exc)
        }


def get_uc_mix_by_subestacao(
    engine: Engine,
    subestacao_id: int,
    logger: logging.Logger | None = None
) -> dict:
    """
    Retorna o mix de unidades consumidoras por subestação.

    Args:
        engine: Engine do SQLAlchemy
        subestacao_id: ID da subestação
        logger: Logger

    Returns:
        Dict com mix por classe e tipo de estabelecimento
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"Buscando mix de UCs para subestação {subestacao_id}")

    query = text("""
        SELECT
            classe_consumo,
            tipo_estabelecimento,
            COUNT(*) as qtd_instalacoes,
            SUM(qtd_unidades) as qtd_unidades_consumidoras,
            SUM(potencia_kw) / 1000.0 as potencia_total_mw
        FROM gd_granular
        WHERE subestacao_id = :subestacao_id
        GROUP BY classe_consumo, tipo_estabelecimento
        ORDER BY potencia_total_mw DESC
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"subestacao_id": subestacao_id})
            rows = result.fetchall()

        if not rows:
            return {
                "subestacao_id": subestacao_id,
                "mix": {},
                "totais": {
                    "qtd_instalacoes": 0,
                    "qtd_unidades_consumidoras": 0,
                    "potencia_total_mw": 0.0
                }
            }

        # Agrupar por classe
        mix_por_classe = {}
        totais = {
            "qtd_instalacoes": 0,
            "qtd_unidades_consumidoras": 0,
            "potencia_total_mw": 0.0
        }

        for row in rows:
            classe = row.classe_consumo
            if classe not in mix_por_classe:
                mix_por_classe[classe] = {
                    "qtd_instalacoes": 0,
                    "qtd_unidades_consumidoras": 0,
                    "potencia_total_mw": 0.0,
                    "por_tipo": {}
                }

            tipo = row.tipo_estabelecimento
            mix_por_classe[classe]["por_tipo"][tipo] = {
                "qtd_instalacoes": int(row.qtd_instalacoes),
                "qtd_unidades_consumidoras": int(row.qtd_unidades_consumidoras),
                "potencia_total_mw": round(float(row.potencia_total_mw), 3)
            }

            # Acumular totais por classe
            mix_por_classe[classe]["qtd_instalacoes"] += int(row.qtd_instalacoes)
            mix_por_classe[classe]["qtd_unidades_consumidoras"] += int(row.qtd_unidades_consumidoras)
            mix_por_classe[classe]["potencia_total_mw"] += float(row.potencia_total_mw)

            # Acumular totais gerais
            totais["qtd_instalacoes"] += int(row.qtd_instalacoes)
            totais["qtd_unidades_consumidoras"] += int(row.qtd_unidades_consumidoras)
            totais["potencia_total_mw"] += float(row.potencia_total_mw)

        # Arredondar totais por classe
        for classe in mix_por_classe:
            mix_por_classe[classe]["potencia_total_mw"] = round(
                mix_por_classe[classe]["potencia_total_mw"], 3
            )

        totais["potencia_total_mw"] = round(totais["potencia_total_mw"], 3)

        return {
            "subestacao_id": subestacao_id,
            "mix": mix_por_classe,
            "totais": totais
        }

    except Exception as exc:
        logger.error(f"Erro ao buscar mix de UCs: {exc}", exc_info=True)
        return {
            "subestacao_id": subestacao_id,
            "error": str(exc)
        }
