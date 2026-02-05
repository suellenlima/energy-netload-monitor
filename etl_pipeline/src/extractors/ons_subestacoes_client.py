import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import (
    create_db_engine,
    create_session,
    delete_all_rows,
    load_settings,
    request,
    table_exists,
)


# URL correta do dataset público de subestações ONS (AWS S3)
SUBESTACOES_ONS_URL = (
    "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/subestacao/SUBESTACAO.csv"
)

def extract_subestacoes_data(session, settings, logger: logging.Logger) -> pd.DataFrame:
    """
    Extrai dados de subestações do ONS via API AWS S3.
    Retorna DataFrame com as subestações da Rede de Operação.
    """
    logger.info("Extraindo dados de subestações do ONS...")
    try:
        response = request(session, "GET", SUBESTACOES_ONS_URL, 
                          settings=settings.http, logger=logger, stream=True)
        response.raise_for_status()
        # Dados do ONS vêm em UTF-8 com separador ponto-e-vírgula
        df = pd.read_csv(response.raw, encoding="UTF-8", sep=";")
        logger.info(f"✅ Carregadas {len(df)} subestações do ONS")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair dados reais do ONS: {e}")
        logger.warning("Continuando com dados mock para demonstração...")
        return pd.DataFrame()


def transform_subestacoes_data(df: pd.DataFrame, logger: logging.Logger) -> gpd.GeoDataFrame:
    """
    Transforma dados de subestações em GeoDataFrame.
    Mapeia as colunas reais do ONS para o schema esperado.
    """
    if df.empty:
        logger.warning("DataFrame de subestações vazio.")
        return gpd.GeoDataFrame()

    # Mapear colunas do ONS para nosso schema
    df_clean = df.copy()
    
    # Renomear colunas do ONS para nosso schema
    column_mapping = {
        'nom_subestacao': 'nome',
        'id_subestacao': 'sigla_se',
        'val_niveltensao': 'tensao_kv',
        'nom_subsistema': 'subsistema',
        'nom_agente_principal': 'distribuidora',
        'val_latitude': 'latitude',
        'val_longitude': 'longitude',
    }
    
    # Manter apenas colunas que existem no DataFrame
    available_mapping = {old: new for old, new in column_mapping.items() if old in df_clean.columns}
    df_clean = df_clean.rename(columns=available_mapping)
    
    if 'latitude' not in df_clean.columns or 'longitude' not in df_clean.columns:
        logger.error("Colunas de latitude/longitude não encontradas no arquivo do ONS")
        return gpd.GeoDataFrame()
    
    # Pré-filtrar para remover valores nulos
    df_clean = df_clean.dropna(subset=['latitude', 'longitude'])
    
    # Converter para numéricas
    df_clean["latitude"] = pd.to_numeric(df_clean["latitude"], errors="coerce")
    df_clean["longitude"] = pd.to_numeric(df_clean["longitude"], errors="coerce")
    
    # Remover linhas onde conversão falhou
    df_clean = df_clean.dropna(subset=['latitude', 'longitude'])
    
    if 'tensao_kv' in df_clean.columns:
        df_clean["tensao_kv"] = pd.to_numeric(df_clean["tensao_kv"], errors="coerce")
    
    # Adicionar coluna de fonte se não existir
    if 'fonte_dados' not in df_clean.columns:
        df_clean['fonte_dados'] = 'ONS'

    # Criar GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df_clean,
        geometry=gpd.points_from_xy(df_clean.longitude, df_clean.latitude),
        crs="EPSG:4326",
    )
    logger.info(f"✅ Transformadas {len(gdf)} subestações para GeoDataFrame")
    return gdf


def load_subestacoes_data(gdf: gpd.GeoDataFrame, engine, logger: logging.Logger) -> int:
    """
    Carrega dados de subestações no banco de dados.
    """
    if gdf.empty:
        logger.info("Sem subestações para carregar.")
        return 0

    try:
        # Sempre usar if_exists="replace" para garantir que geometria seja criada corretamente
        # Isso evita problemas com GEOMETRY_COLUMNS não estar registrado
        gdf.to_postgis("subestacoes_ons", engine, if_exists="replace", index=False)
        logger.info("Tabela subestacoes_ons criada/atualizada com metadata PostGIS.")
        logger.info("Carregadas %s subestações em subestacoes_ons.", len(gdf))
        return int(len(gdf))
    except Exception as e:
        logger.error(f"Erro ao carregar subestações: {e}")
        return 0


def run_extraction(session=None, engine=None, settings=None, logger=None) -> int:
    """
    Executa o pipeline de extração de subestações.
    """
    logger = logger or logging.getLogger("etl.subestacoes")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL nao esta configurada.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(engine)

    try:
        df = extract_subestacoes_data(session, settings, logger)
        if df is None or df.empty:
            logger.warning("Nenhum dado de subestações extraído.")
            return 0

        gdf = transform_subestacoes_data(df, logger)
        if gdf.empty:
            logger.warning("Nenhuma subestação transformada.")
            return 0

        rows_loaded = load_subestacoes_data(gdf, engine, logger)
        return rows_loaded
    except Exception as e:
        logger.error(f"Erro na pipeline de subestações: {e}", exc_info=True)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    """Ponto de entrada para execução direta."""
    import logging
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    logger = logging.getLogger("etl.subestacoes")
    
    try:
        settings = load_settings()
        if not settings.database.url:
            raise ValueError("DATABASE_URL não configurada")
        
        engine = create_db_engine(settings.database.url)
        session = create_session(settings.http, logger=logger)
        
        logger.info("🚀 Iniciando extração de subestações...")
        rows = run_extraction(session=session, engine=engine, settings=settings, logger=logger)
        
        if rows > 0:
            logger.info(f"✅ Pipeline concluída com sucesso: {rows} subestações carregadas")
        else:
            logger.warning("⚠️ Pipeline concluída mas nenhuma subestação foi carregada")
    
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}", exc_info=True)
        import sys
        sys.exit(1)
