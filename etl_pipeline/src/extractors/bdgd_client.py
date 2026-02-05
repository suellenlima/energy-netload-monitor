"""
Extrator de dados da BDGD (Base de Dados Geográfica da Distribuidora).

Fonte: ANEEL - https://dadosabertos.aneel.gov.br/dataset/bdgd

A BDGD contém informações das distribuidoras sobre:
- Unidades consumidoras (UCs)
- Transformadores
- Redes de distribuição (MT/BT)
- Subestações

Focamos nas UCs para obter mix real de consumidores por região.
"""

import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
from geoalchemy2 import Geometry
from shapely.geometry import Point
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    create_engine,
    func,
)
from sqlalchemy.orm import declarative_base, sessionmaker

import sys
if '/app/src' not in sys.path:
    sys.path.insert(0, '/app/src')

try:
    from ..core.config import load_settings
    from ..core.db import create_db_engine
except (ImportError, ValueError):
    from core.config import load_settings
    from core.db import create_db_engine

logger = logging.getLogger(__name__)
settings = load_settings()

# Obter URL do banco
db_url = os.getenv("DATABASE_URL") or settings.database.url
if not db_url:
    db_url = "postgresql://admin:admin123@db:5432/energy_monitor"

Base = declarative_base()


class BDGDUnidadeConsumidora(Base):
    """Modelo de Unidade Consumidora da BDGD."""

    __tablename__ = "bdgd_unidades_consumidoras"

    id = Column(BigInteger, primary_key=True)
    cod_id = Column(String(50))
    distribuidora = Column(String(100), nullable=False)
    classe_consumo = Column(String(50), nullable=False)
    subclasse = Column(String(50))
    energia_kwh_mes = Column(Numeric)
    tensao_fornecimento = Column(String(20))
    municipio = Column(String(100))
    bairro = Column(String(100))
    geom = Column(Geometry("POINT", srid=4326))
    data_referencia = Column(Date)
    created_at = Column(DateTime, server_default=func.now())


# URLs conhecidas de BDGD (exemplos - variam por distribuidora)
BDGD_URLS = {
    "ENEL-SP": "https://dadosabertos.aneel.gov.br/dataset/.../bdgd_enel_sp.zip",
    "CPFL": "https://dadosabertos.aneel.gov.br/dataset/.../bdgd_cpfl.zip",
    # Adicionar mais conforme disponibilidade
}


def extract_bdgd_csv(
    distribuidora: str,
    arquivo_csv: Optional[Path] = None,
    url: Optional[str] = None
) -> pd.DataFrame:
    """
    Extrai dados da BDGD de arquivo CSV ou URL.

    Args:
        distribuidora: Nome da distribuidora
        arquivo_csv: Caminho local para arquivo CSV (prioritário)
        url: URL para download do CSV/ZIP

    Returns:
        DataFrame com dados das UCs

    Nota:
        A BDGD pode ter formatos diferentes por distribuidora.
        Este código assume um formato padrão ANEEL.
    """
    logger.info(f"Extraindo BDGD para {distribuidora}")

    # Tentar arquivo local primeiro
    if arquivo_csv and Path(arquivo_csv).exists():
        logger.info(f"Lendo arquivo local: {arquivo_csv}")
        df = pd.read_csv(
            arquivo_csv,
            encoding="latin1",  # BDGD geralmente usa latin1
            sep=";",            # Algumas usam ;
            decimal=",",        # Decimal brasileiro
            low_memory=False
        )
    elif url:
        logger.info(f"Baixando de URL: {url}")
        # Implementar download + descompactação se for ZIP
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        # Processar conforme formato
        df = pd.read_csv(
            response.content,
            encoding="latin1",
            sep=";",
            decimal=",",
            low_memory=False
        )
    else:
        logger.warning(f"BDGD não disponível para {distribuidora}. Usando dados mock.")
        return _generate_mock_bdgd(distribuidora)

    logger.info(f"BDGD extraída: {len(df)} registros")
    return df


def transform_bdgd_csv(df: pd.DataFrame, distribuidora: str) -> gpd.GeoDataFrame:
    """
    Transforma CSV da BDGD em GeoDataFrame padronizado.

    Args:
        df: DataFrame bruto da BDGD
        distribuidora: Nome da distribuidora

    Returns:
        GeoDataFrame com schema padronizado
    """
    logger.info("Transformando dados da BDGD")

    # Mapeamento de colunas (pode variar por distribuidora)
    # Assumindo formato padrão ANEEL
    column_mapping = {
        "COD_ID": "cod_id",
        "TEN": "tensao_fornecimento",
        "CTMT": "circuito_mt",
        "CEG": "ceg",
        "PAC_1": "municipio",
        "BAI_1": "bairro",
        "X": "longitude",
        "Y": "latitude",
        "CLAS_SUB": "classe_consumo",  # Residencial, Comercial, etc.
        "ENE_01": "energia_jan",       # Energia mensal
    }

    # Renomear colunas se existirem
    df_clean = df.copy()
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df_clean.rename(columns={old_col: new_col}, inplace=True)

    # Calcular consumo médio mensal (média de 12 meses se disponível)
    colunas_energia = [f"energia_{mes}" for mes in [
        "jan", "fev", "mar", "abr", "mai", "jun",
        "jul", "ago", "set", "out", "nov", "dez"
    ]]
    colunas_energia_disponiveis = [col for col in colunas_energia if col in df_clean.columns]

    if colunas_energia_disponiveis:
        df_clean["energia_kwh_mes"] = df_clean[colunas_energia_disponiveis].mean(axis=1)
    else:
        # Fallback: usar coluna única se houver
        df_clean["energia_kwh_mes"] = df_clean.get("energia_kwh", 0)

    # Normalizar classes de consumo
    df_clean["classe_consumo"] = df_clean.get("classe_consumo", "Residencial").str.strip()
    df_clean["classe_consumo"] = df_clean["classe_consumo"].map({
        "RESIDENCIAL": "Residencial",
        "COMERCIAL": "Comercial",
        "INDUSTRIAL": "Industrial",
        "RURAL": "Rural",
        "PODER PUBLICO": "Poder Público",
        "ILUMINACAO PUBLICA": "Iluminação Pública",
    }).fillna("Outros")

    # Criar geometria
    if "longitude" in df_clean.columns and "latitude" in df_clean.columns:
        df_clean = df_clean.dropna(subset=["longitude", "latitude"])
        geometry = [Point(xy) for xy in zip(df_clean.longitude, df_clean.latitude)]
        gdf = gpd.GeoDataFrame(df_clean, geometry=geometry, crs="EPSG:4326")
    else:
        logger.warning("Coordenadas não encontradas, criando geometria nula")
        gdf = gpd.GeoDataFrame(df_clean, geometry=[None] * len(df_clean), crs="EPSG:4326")

    # Selecionar colunas finais
    gdf["distribuidora"] = distribuidora
    gdf["data_referencia"] = date.today().replace(day=1)  # Primeiro dia do mês atual

    colunas_finais = [
        "cod_id", "distribuidora", "classe_consumo", "subclasse",
        "energia_kwh_mes", "tensao_fornecimento", "municipio", "bairro",
        "geometry", "data_referencia"
    ]

    # Garantir que todas as colunas existam
    for col in colunas_finais:
        if col not in gdf.columns and col != "geometry":
            gdf[col] = None

    gdf = gdf[colunas_finais]

    logger.info(f"BDGD transformada: {len(gdf)} UCs")
    return gdf


def load_bdgd_data(gdf: gpd.GeoDataFrame) -> int:
    """
    Carrega dados da BDGD no banco de dados.

    Args:
        gdf: GeoDataFrame com dados transformados

    Returns:
        Número de registros inseridos
    """
    logger.info("Carregando BDGD no banco de dados")

    engine = create_db_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Limpar dados antigos da mesma distribuidora e data de referência
        distribuidora = gdf["distribuidora"].iloc[0] if not gdf.empty else None
        data_ref = gdf["data_referencia"].iloc[0] if not gdf.empty else None

        if distribuidora and data_ref:
            deleted = session.query(BDGDUnidadeConsumidora).filter(
                BDGDUnidadeConsumidora.distribuidora == distribuidora,
                BDGDUnidadeConsumidora.data_referencia == data_ref
            ).delete()
            logger.info(f"Removidos {deleted} registros antigos")

        # Inserir novos dados
        registros = []
        for _, row in gdf.iterrows():
            uc = BDGDUnidadeConsumidora(
                cod_id=row.get("cod_id"),
                distribuidora=row["distribuidora"],
                classe_consumo=row["classe_consumo"],
                subclasse=row.get("subclasse"),
                energia_kwh_mes=row.get("energia_kwh_mes"),
                tensao_fornecimento=row.get("tensao_fornecimento"),
                municipio=row.get("municipio"),
                bairro=row.get("bairro"),
                geom=f"SRID=4326;{row['geometry'].wkt}" if row["geometry"] else None,
                data_referencia=row["data_referencia"]
            )
            registros.append(uc)

        session.bulk_save_objects(registros)
        session.commit()

        logger.info(f"[OK] {len(registros)} UCs carregadas no banco")
        return len(registros)

    except Exception as exc:
        session.rollback()
        logger.error(f"Erro ao carregar BDGD: {exc}", exc_info=True)
        raise
    finally:
        session.close()


def _generate_mock_bdgd(distribuidora: str, num_ucs: int = 1000) -> pd.DataFrame:
    """
    Gera dados mock da BDGD para testes.

    Args:
        distribuidora: Nome da distribuidora
        num_ucs: Número de UCs a gerar

    Returns:
        DataFrame com dados sintéticos
    """
    import numpy as np

    logger.info(f"Gerando {num_ucs} UCs mock para {distribuidora}")

    # Distribuição realista de classes
    classes = np.random.choice(
        ["Residencial", "Comercial", "Industrial", "Rural", "Poder Público"],
        size=num_ucs,
        p=[0.80, 0.12, 0.03, 0.04, 0.01]  # Proporções típicas do Brasil
    )

    # Consumo médio por classe (kWh/mês)
    consumo_medio = {
        "Residencial": (50, 300),      # 50-300 kWh/mês
        "Comercial": (300, 2000),      # 300-2000 kWh/mês
        "Industrial": (5000, 50000),   # 5-50 MWh/mês
        "Rural": (100, 1000),          # 100-1000 kWh/mês
        "Poder Público": (500, 5000)   # 500-5000 kWh/mês
    }

    energia = []
    for classe in classes:
        min_kwh, max_kwh = consumo_medio.get(classe, (100, 1000))
        energia.append(np.random.uniform(min_kwh, max_kwh))

    # Coordenadas mock (região de São Paulo)
    lats = np.random.uniform(-24.0, -23.0, num_ucs)
    lons = np.random.uniform(-47.0, -46.0, num_ucs)

    return pd.DataFrame({
        "cod_id": [f"UC{i:06d}" for i in range(num_ucs)],
        "classe_consumo": classes,
        "energia_kwh_mes": energia,
        "latitude": lats,
        "longitude": lons,
        "municipio": "São Paulo",
        "tensao_fornecimento": np.random.choice(["BT", "MT"], num_ucs, p=[0.95, 0.05])
    })


def run_bdgd_etl(
    distribuidora: str,
    arquivo_csv: Optional[Path] = None,
    url: Optional[str] = None,
    use_mock: bool = False
):
    """
    Executa ETL completo da BDGD.

    Args:
        distribuidora: Nome da distribuidora
        arquivo_csv: Caminho para arquivo CSV local
        url: URL para download
        use_mock: Se True, usa dados mock
    """
    logger.info(f"Iniciando ETL da BDGD para {distribuidora}")

    try:
        # Extract
        if use_mock:
            df = _generate_mock_bdgd(distribuidora)
        else:
            df = extract_bdgd_csv(distribuidora, arquivo_csv, url)

        # Transform
        gdf = transform_bdgd_csv(df, distribuidora)

        # Load
        registros = load_bdgd_data(gdf)

        logger.info(f"[SUCCESS] ETL BDGD concluído: {registros} UCs carregadas")

    except Exception as exc:
        logger.error(f"[ERRO] ETL BDGD falhou: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    # Teste com dados mock
    logging.basicConfig(level=logging.INFO)
    run_bdgd_etl("ENEL-SP", use_mock=True)
