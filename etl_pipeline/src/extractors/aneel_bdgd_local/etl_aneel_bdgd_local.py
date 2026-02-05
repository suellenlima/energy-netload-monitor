"""
ETL ANEEL BDGD - Extração Local de Dados Geoespaciais
=====================================================

Extrai dados de arquivos GDB (Geodatabase) localizados em:
C:\Hackathon\Git\energy-netload-monitor\data\aneel_bdgd

Cada pasta contém dados de uma distribuidora diferente (BDGD - Base de Dados Geográficos)
e são carregados nas tabelas:
- transformadores_aneel
- subestacoes_aneel
- consumidores_bt_aneel (Unidades Consumidoras de Baixa Tensão - UCBT)
- consumidores_mt_aneel (Unidades Consumidoras de Média Tensão - UCMT)
- consumidores_at_aneel (Unidades Consumidoras de Alta Tensão - UCAT)

Uso:
    python etl_aneel_bdgd_local.py [--distribuidora NOME] [--debug]
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import argparse

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text, inspect, exc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Adicionar paths
# Dentro do Docker: /app/src/extractors/aneel_bdgd_local/etl_aneel_bdgd_local.py
# Com o novo volume, o /src fica disponível diretamente
import os
if os.path.exists("/src"):
    # Rodando dentro do Docker
    SRC_DIR = Path("/src")
else:
    # Rodando localmente
    SRC_DIR = Path(__file__).resolve().parents[3]  # etl_pipeline/src/extractors/aneel_bdgd_local -> src
    
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importar services centralizados
from services.aneel_bdgd_service import (
    TransformerService,
    SubstationService,
    ConsumerService,
    DistributorService,
    AreaService,
    GeometryService,
    ClassificationService
)

try:
    from core import create_db_engine, load_settings, table_exists
    settings = load_settings()
    DB_URL = settings.database.url
    if not DB_URL:
        raise ValueError("DATABASE_URL não configurada")
    engine = create_db_engine(DB_URL)
    logger.info(f"✓ Conectado ao banco: {DB_URL.split('@')[1] if '@' in DB_URL else 'local'}")
except Exception as e:
    logger.error(f"❌ Erro ao conectar ao banco: {e}")
    sys.exit(1)

# Configurações do ETL
# Dentro do Docker: __file__ = /app/src/extractors/aneel_bdgd_local/etl_aneel_bdgd_local.py
# Localmente: __file__ = etl_pipeline/src/extractors/aneel_bdgd_local/etl_aneel_bdgd_local.py
import os
if os.path.exists("/app/data"):
    # Rodando dentro do Docker
    ANEEL_BDGD_DIR = Path("/app/data/aneel_bdgd")
else:
    # Rodando localmente
    ANEEL_BDGD_DIR = Path(__file__).resolve().parents[4] / "data" / "aneel_bdgd"

CACHE_DIR = ANEEL_BDGD_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Camadas de interesse e seus aliases
LAYER_PATTERNS = {
    'transformadores': ['UNTRD', 'transformador', 'trafo', 'TRANSFORMADOR', 'TRAFO'],
    'subestacoes': ['CTMT', 'SUB', 'subestacao', 'subest', 'SUBESTACAO', 'SUBEST', 'SE'],
    'consumidores': ['UC', 'consumidor', 'ponto', 'CONSUMIDOR', 'PONTO', 'cliente'],
}

# Mapeamento de campos para cada camada
FIELD_MAPPING = {
    'transformadores': {
        'codigo': ['COD_ID', 'CODIGO', 'ID', 'code', 'Code'],
        'distribuidora': ['DISTRIBUIDORA', 'DISTRIB', 'distribuidora', 'distribuidor'],
        'nome': ['NOME', 'NAME', 'nome'],
        'potencia_kva': ['POT_NOM', 'POTENCIA', 'POWER', 'potencia', 'power'],
        'tensao_primaria_kv': ['TEN_PRI', 'TENSAO_PRI', 'V_PRI', 'tensao_primaria'],
        'tensao_secundaria_kv': ['TEN_SEC', 'TENSAO_SEC', 'V_SEC', 'tensao_secundaria'],
        'latitude': ['LAT', 'LATITUDE', 'latitude'],
        'longitude': ['LON', 'LONGITUDE', 'longitude'],
    },
    'subestacoes': {
        'codigo': ['COD_ID', 'CODIGO', 'ID', 'code', 'Code'],
        'nome': ['NOME', 'NAME', 'nome', 'NOM', 'DESCR', 'description'],
        'distribuidora': ['DISTRIBUIDORA', 'DISTRIB', 'distribuidora', 'distribuidor', 'DIST'],
        'tensao_kv': ['TENSAO', 'TENSAO_KV', 'V', 'voltagem', 'voltage', 'CLAS_TEN', 'TEN', 'VOLTAGEM'],
        'latitude': ['LAT', 'LATITUDE', 'latitude'],
        'longitude': ['LON', 'LONGITUDE', 'longitude'],
    },
    'consumidores_bt': {
        'codigo': ['COD_ID', 'CODIGO', 'ID', 'code', 'Code'],
        'distribuidora': ['DISTRIBUIDORA', 'DISTRIB', 'distribuidora', 'DIST'],
        'subestacao_codigo': ['SUB', 'SUBEST', 'subestacao'],
        'circuito_mt_codigo': ['CTMT', 'circuito_mt'],
        'transformador_mt_codigo': ['UNI_TR_MT', 'transformador_mt'],
        'transformador_at_codigo': ['UNI_TR_AT', 'transformador_at'],
        'ramal_codigo': ['RAMAL', 'ramal'],
        'ponto_notavel_codigo': ['PN_CON', 'ponto_notavel'],
        'pac_codigo': ['PAC', 'pac'],
        'conjunto_codigo': ['CONJ', 'conjunto'],
        'municipio_codigo': ['MUN', 'municipio'],
        'geracao_distribuida_codigo': ['CODGD', 'geracao_dist'],
        'logradouro': ['LGRD', 'logradouro', 'endereço'],
        'bairro': ['BRR', 'bairro'],
        'cep': ['CEP', 'cep'],
        'classe_subclasse_codigo': ['CLAS_SUB', 'classe_sub'],
        'cnae_codigo': ['CNAE', 'cnae'],
        'curva_carga_codigo': ['TIP_CC', 'curva_carga'],
        'fases_conexao_codigo': ['FAS_CON', 'fases'],
        'grupo_tensao_codigo': ['GRU_TEN', 'grupo_tensao'],
        'tensao_fornecimento_codigo': ['TEN_FORN', 'tensao_fornec'],
        'grupo_tarifario_codigo': ['GRU_TAR', 'grupo_tarifario'],
        'situacao_ativacao_codigo': ['SIT_ATIV', 'situacao'],
        'data_conexao': ['DAT_CON', 'data_conexao'],
        'carga_instalada_kw': ['CAR_INST', 'carga_instalada'],
        'consumidor_livre': ['LIV', 'livre'],
        'area_localizacao_codigo': ['ARE_LOC', 'area_localizacao'],
        'sem_rede': ['SEMRED', 'sem_rede'],
        'latitude': ['LAT', 'LATITUDE', 'latitude'],
        'longitude': ['LON', 'LONGITUDE', 'longitude'],
        'descricao': ['DESCR', 'descricao', 'description'],
    },
    'consumidores_mt': {
        'codigo': ['COD_ID', 'CODIGO', 'ID', 'code', 'Code'],
        'distribuidora': ['DISTRIBUIDORA', 'DISTRIB', 'distribuidora', 'DIST'],
        'subestacao_codigo': ['SUB', 'SUBEST', 'subestacao'],
        'circuito_mt_codigo': ['CTMT', 'circuito_mt'],
        'transformador_at_codigo': ['UNI_TR_AT', 'transformador_at'],
        'ponto_notavel_codigo': ['PN_CON', 'ponto_notavel'],
        'pac_codigo': ['PAC', 'pac'],
        'conjunto_codigo': ['CONJ', 'conjunto'],
        'municipio_codigo': ['MUN', 'municipio'],
        'geracao_distribuida_codigo': ['CODGD', 'geracao_dist'],
        'logradouro': ['LGRD', 'logradouro', 'endereço'],
        'bairro': ['BRR', 'bairro'],
        'cep': ['CEP', 'cep'],
        'classe_subclasse_codigo': ['CLAS_SUB', 'classe_sub'],
        'cnae_codigo': ['CNAE', 'cnae'],
        'curva_carga_codigo': ['TIP_CC', 'curva_carga'],
        'fases_conexao_codigo': ['FAS_CON', 'fases'],
        'grupo_tensao_codigo': ['GRU_TEN', 'grupo_tensao'],
        'tensao_fornecimento_codigo': ['TEN_FORN', 'tensao_fornec'],
        'grupo_tarifario_codigo': ['GRU_TAR', 'grupo_tarifario'],
        'situacao_ativacao_codigo': ['SIT_ATIV', 'situacao'],
        'data_conexao': ['DAT_CON', 'data_conexao'],
        'carga_instalada_kw': ['CAR_INST', 'carga_instalada'],
        'demanda_contratada_kw': ['DEM_CONT', 'demanda_contratada'],
        'consumidor_livre': ['LIV', 'livre'],
        'area_localizacao_codigo': ['ARE_LOC', 'area_localizacao'],
        'sem_rede': ['SEMRED', 'sem_rede'],
        'latitude': ['LAT', 'LATITUDE', 'latitude'],
        'longitude': ['LON', 'LONGITUDE', 'longitude'],
        'descricao': ['DESCR', 'descricao', 'description'],
    },
    'consumidores_at': {
        'codigo': ['COD_ID', 'CODIGO', 'ID', 'code', 'Code'],
        'distribuidora': ['DISTRIBUIDORA', 'DISTRIB', 'distribuidora', 'DIST'],
        'subestacao_codigo': ['SUB', 'SUBEST', 'subestacao'],
        'circuito_at_codigo': ['CTAT', 'circuito_at'],
        'ponto_notavel_codigo': ['PN_CON', 'ponto_notavel'],
        'pac_codigo': ['PAC', 'pac'],
        'conjunto_codigo': ['CONJ', 'conjunto'],
        'municipio_codigo': ['MUN', 'municipio'],
        'geracao_distribuida_codigo': ['CODGD', 'geracao_dist'],
        'logradouro': ['LGRD', 'logradouro', 'endereço'],
        'bairro': ['BRR', 'bairro'],
        'cep': ['CEP', 'cep'],
        'classe_subclasse_codigo': ['CLAS_SUB', 'classe_sub'],
        'cnae_codigo': ['CNAE', 'cnae'],
        'curva_carga_codigo': ['TIP_CC', 'curva_carga'],
        'fases_conexao_codigo': ['FAS_CON', 'fases'],
        'grupo_tensao_codigo': ['GRU_TEN', 'grupo_tensao'],
        'tensao_fornecimento_codigo': ['TEN_FORN', 'tensao_fornec'],
        'grupo_tarifario_codigo': ['GRU_TAR', 'grupo_tarifario'],
        'situacao_ativacao_codigo': ['SIT_ATIV', 'situacao'],
        'data_conexao': ['DAT_CON', 'data_conexao'],
        'carga_instalada_kw': ['CAR_INST', 'carga_instalada'],
        'demanda_contratada_kw': ['DEM_CONT', 'demanda_contratada'],
        'consumidor_livre': ['LIV', 'livre'],
        'area_localizacao_codigo': ['ARE_LOC', 'area_localizacao'],
        'latitude': ['LAT', 'LATITUDE', 'latitude'],
        'longitude': ['LON', 'LONGITUDE', 'longitude'],
        'descricao': ['DESCR', 'descricao', 'description'],
    },
}


def get_distribuidoras() -> Dict[str, Path]:
    """
    Retorna dicionário de distribuidoras disponíveis e seus paths
    Suporta duas estruturas:
    1. Cada distribuidora em sua pasta: aneel_bdgd/DIST_X/gdb/...
    2. GDB diretamente: aneel_bdgd/*.gdb/
    
    Returns:
        Dict[str, Path]: {nome_distribuidora: caminho_completo_gdb}
    """
    distribuidoras = {}
    
    if not ANEEL_BDGD_DIR.exists():
        logger.error(f"❌ Diretório não encontrado: {ANEEL_BDGD_DIR}")
        return distribuidoras
    
    # Procura estrutura 1: pastas com subfolder gdb
    for folder in ANEEL_BDGD_DIR.iterdir():
        if folder.is_dir() and folder.suffix != '.gdb':
            gdb_path = folder / 'gdb'
            if gdb_path.exists():
                dist_name = folder.name
                distribuidoras[dist_name] = gdb_path
                logger.info(f"  ✓ Encontrada distribuidora (estrutura 1): {dist_name}")
    
    # Procura estrutura 2: GDB diretamente
    for gdb_file in ANEEL_BDGD_DIR.glob('*.gdb'):
        if gdb_file.is_dir():
            dist_name = gdb_file.stem  # Nome sem a extensão .gdb
            if dist_name not in distribuidoras:  # Não sobrescrever se já encontrado
                distribuidoras[dist_name] = gdb_file
                logger.info(f"  ✓ Encontrada distribuidora (estrutura 2): {dist_name}")
    
    return distribuidoras


def simplificar_nome_distribuidora(nome_completo: str) -> str:
    """
    Simplifica nome da distribuidora extrayendo apenas a primeira palavra
    Ex: "IENERGIA_87_2021-02-28_M10_20210902-1755" -> "IENERGIA"
    
    Args:
        nome_completo: Nome completo da distribuidora
        
    Returns:
        str: Nome simplificado (primeira palavra)
    """
    if not nome_completo:
        return ""
    
    # Extrair primeira palavra (antes de underscore ou número)
    primeiro_nome = nome_completo.split('_')[0].upper()
    return primeiro_nome


def discover_layers(gdb_path: Path) -> Dict[str, List[str]]:
    """
    Descobre todas as camadas disponíveis em um GDB
    
    IMPORTANTE para SUBESTAÇÕES:
    - Prioridade 1: SUB (entidade geográfica oficial conforme BDGD)
    - Fallback: CTMT (Barramentos com dados reais de subestação)
    
    Args:
        gdb_path: Path ao arquivo GDB
        
    Returns:
        Dict[str, List[str]]: {tipo: [nomes_das_camadas]}
    """
    layers_found = {k: [] for k in LAYER_PATTERNS.keys()}
    
    try:
        # Tentar usar fiona.listlayers (mais compatível)
        import fiona
        layer_list = fiona.listlayers(str(gdb_path))
        logger.debug(f"  Camadas disponíveis: {layer_list}")
        
        for layer_name in layer_list:
            if isinstance(layer_name, tuple):
                layer_name = layer_name[0]
            layer_lower = str(layer_name).lower()
            
            for tipo, patterns in LAYER_PATTERNS.items():
                if any(pattern.lower() in layer_lower for pattern in patterns):
                    if layer_lower == 'base' and tipo == 'subestacoes':
                        continue
                    if layer_name not in layers_found[tipo]:
                        layers_found[tipo].append(layer_name)
                    break
    
    except Exception as e:
        logger.warning(f"  ⚠ Erro ao listar camadas: {e}")
    
    return layers_found


def normalize_geometry(gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, Dict]:
    """
    Normaliza geometria para EPSG:4326 e extrai coordenadas
    
    Args:
        gdf: GeoDataFrame com geometria
        
    Returns:
        Tuple[GeoDataFrame com lat/lon, dicionário de stats]
    """
    stats = {'total': len(gdf), 'com_geometria': 0, 'sem_geometria': 0}
    
    # Reprojetar para WGS84 se necessário
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        try:
            gdf = gdf.to_crs(epsg=4326)
            logger.debug(f"    Reprojetada de {gdf.crs} para EPSG:4326")
        except Exception as e:
            logger.warning(f"    ⚠ Erro ao reprojetar: {e}")
    
    # Extrair coordenadas
    gdf['latitude'] = None
    gdf['longitude'] = None
    
    for idx, row in gdf.iterrows():
        try:
            if row.geometry and hasattr(row.geometry, 'x'):
                gdf.at[idx, 'latitude'] = row.geometry.y
                gdf.at[idx, 'longitude'] = row.geometry.x
                stats['com_geometria'] += 1
            else:
                stats['sem_geometria'] += 1
        except Exception as e:
            logger.debug(f"      Erro ao extrair coords da linha {idx}: {e}")
            stats['sem_geometria'] += 1
    
    return gdf, stats


def map_fields(df: pd.DataFrame, campo_mapping: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Mapeia campos do GDB para schema do banco
    
    Args:
        df: DataFrame com dados do GDB
        campo_mapping: Mapeamento {campo_db: [campos_possiveis_gdb]}
        
    Returns:
        DataFrame com campos renomeados
    """
    df_mapped = pd.DataFrame()
    
    for db_field, possible_fields in campo_mapping.items():
        found = False
        for possible in possible_fields:
            if possible in df.columns:
                df_mapped[db_field] = df[possible]
                found = True
                logger.debug(f"      {db_field} <- {possible}")
                break
        
        if not found and db_field not in df_mapped.columns:
            df_mapped[db_field] = None
    
    return df_mapped


def classificar_tipo_tensao(ten_pri: float, ten_sec: float) -> str:
    """Classifica transformador como BT, MT ou AT baseado em tensões
    
    Convenção:
    - BT (Baixa Tensão): tensao_secundaria < 1 kV
    - AT (Alta Tensão): tensao_primaria > 35 kV ou tensao_secundaria > 35 kV
    - MT (Média Tensão): demais casos (1-35 kV)
    """
    if pd.isna(ten_sec) and pd.isna(ten_pri):
        return None
    
    # Verificar BT
    if not pd.isna(ten_sec) and ten_sec < 1:
        return 'BT'
    
    # Verificar AT
    if (not pd.isna(ten_pri) and ten_pri > 35) or (not pd.isna(ten_sec) and ten_sec > 35):
        return 'AT'
    
    # Default MT para tensões intermediárias
    return 'MT'


def extract_transformadores(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
    """Extrai dados de transformadores com referência para subestação"""
    logger.info(f"    Extraindo transformadores...")
    
    gdf, stats = normalize_geometry(gdf)
    logger.debug(f"      Geometrias: {stats}")
    
    # Mapear campos
    df = map_fields(gdf, FIELD_MAPPING['transformadores'])
    
    # Adicionar distribuidora
    if 'distribuidora' not in df.columns or df['distribuidora'].isna().all():
        df['distribuidora'] = distribuidora
    
    # Adicionar referência para subestação (campo SUB do GDB)
    if 'SUB' in gdf.columns:
        df['subestacao_codigo'] = gdf['SUB']
        logger.debug(f"      Adicionado campo subestacao_codigo")
    
    # Classificar tipo de tensão (BT, MT, AT)
    if 'tensao_primaria_kv' in df.columns or 'tensao_secundaria_kv' in df.columns:
        df['tipo_tensao'] = df.apply(
            lambda row: classificar_tipo_tensao(
                row.get('tensao_primaria_kv'), 
                row.get('tensao_secundaria_kv')
            ),
            axis=1
        )
        bt_count = (df['tipo_tensao'] == 'BT').sum()
        mt_count = (df['tipo_tensao'] == 'MT').sum()
        at_count = (df['tipo_tensao'] == 'AT').sum()
        logger.debug(f"      Classificados: BT={bt_count}, MT={mt_count}, AT={at_count}")
    
    # Adicionar campos de auditoria
    df['data_criacao'] = datetime.now()
    df['data_atualizacao'] = datetime.now()
    
    # Remover duplicatas por código
    if 'codigo' in df.columns:
        df = df.drop_duplicates(subset=['codigo'], keep='first')
    
    logger.info(f"      ✓ {len(df)} registros extraídos")
    return df


def _limpar_nome_subestacao(nome: str, codigo_sub: str) -> str:
    """
    Limpa e normaliza o nome da subestação, removendo prefixos redundantes.
    
    Exemplo:
        "ALIMENTOR SUP 12 CELESC X IGUAÇU SUP 12" → "SUP 12 CELESC X IGUAÇU"
        "SUPRIMENTO 4 SE CELESC X IGUAAU" → "SE CELESC X IGUAAU"
    """
    import re
    
    if not nome or pd.isna(nome):
        return None
    
    nome = str(nome).strip()
    
    # Prefixos a remover (ordem importa: mais específicos primeiro)
    prefixos_remover = [
        r'^ALIMENTOR\s+',  # ALIMENTOR ... 
        r'^SUPRIMENTO\s+\d+\s+',  # SUPRIMENTO 4 ...
        r'^SUPRIM\.\s+',  # SUPRIM. ...
        r'^ALIMENTADOR\s+',  # ALIMENTADOR ...
    ]
    
    # Remover prefixos
    for prefixo in prefixos_remover:
        nome = re.sub(prefixo, '', nome, flags=re.IGNORECASE).strip()
    
    # Remover duplicação de código no final (ex: "SUP 12 ... SUP 12")
    # Tentar com espaços também
    if codigo_sub:
        # Variações do código (com e sem espaço)
        variacoes = [
            rf'\s+{re.escape(codigo_sub)}\s*$',  # SUP12
            rf'\s+{re.escape(codigo_sub.replace("12", " 12"))}\s*$',  # SUP 12
        ]
        for padrao in variacoes:
            nome = re.sub(padrao, '', nome, flags=re.IGNORECASE).strip()
    
    return nome if nome else None


def extract_subestacoes(gdf: gpd.GeoDataFrame, distribuidora: str, gdb_path: Path = None, layer_used: str = None) -> pd.DataFrame:
    """
    Extrai dados de subestações com prioridade SUB > CTMT
    
    Conforme especificação BDGD 2.1:
    - Entidade geográfica: Subestação
    - Modelagem: SUB (código identificador)
    - Camada oficial: SUB
    
    Se SUB vazia, usa CTMT (Barramentos) como fallback
    
    Campos obrigatórios do BDGD:
    - COD_ID (código identificador) 
    - DIST (código da distribuidora)
    - NOM (nome)
    - POS (propriedade - posse)
    
    Args:
        gdf: GeoDataFrame com dados
        distribuidora: Nome da distribuidora
        gdb_path: Path ao GDB (para tentar carregar a outra camada se vazia)
        layer_used: Nome da camada que foi carregada
    """
    logger.info(f"    Extraindo subestações...")
    logger.debug(f"      Camada: {layer_used}")
    logger.debug(f"      Registros carregados: {len(gdf)}")
    logger.debug(f"      Colunas: {list(gdf.columns)}")
    
    # Se GDF vazio e temos path, tentar carregar da outra camada
    if gdf.empty and gdb_path and layer_used:
        logger.info(f"    ⚠ Camada {layer_used} vazia, tentando camada alternativa...")
        
        if layer_used.upper() == 'SUB':
            # SUB está vazia, tentar CTMT
            try:
                gdf = gpd.read_file(str(gdb_path), layer='CTMT')
                layer_used = 'CTMT'
                logger.info(f"    ✓ Carregando dados alternativos da camada CTMT")
            except Exception as e:
                logger.warning(f"    ⚠ Erro ao carregar CTMT: {e}")
                return pd.DataFrame()
        elif layer_used.upper() == 'CTMT':
            # CTMT está vazia, tentar SUB
            try:
                gdf = gpd.read_file(str(gdb_path), layer='SUB')
                layer_used = 'SUB'
                logger.info(f"    ✓ Carregando dados alternativos da camada SUB")
            except Exception as e:
                logger.warning(f"    ⚠ Erro ao carregar SUB: {e}")
                return pd.DataFrame()
    
    # Se ainda vazio, retornar
    if gdf.empty:
        logger.warning(f"    ⚠ Nenhum dados disponível em nenhuma camada de subestações")
        return pd.DataFrame()
    
    # Reprojetar para WGS84 se necessário
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        try:
            gdf = gdf.to_crs(epsg=4326)
            logger.debug(f"      Reprojetada para EPSG:4326")
        except Exception as e:
            logger.warning(f"      ⚠ Erro ao reprojetar: {e}")
    
    df = pd.DataFrame()
    
    # Camada SUB (oficial BDGD)
    if layer_used and layer_used.upper() == 'SUB':
        logger.debug(f"      Processando camada SUB (oficial BDGD)")
        if 'COD_ID' in gdf.columns and 'NOM' in gdf.columns:
            df['codigo_bdgd'] = gdf['COD_ID'].astype(str)
            df['codigo'] = gdf['COD_ID'].astype(str)
            df['nome'] = gdf['NOM'].astype(str)
            
            # Descrição
            if 'DESCR' in gdf.columns:
                df['descricao'] = gdf['DESCR'].astype(str)
            
            # Distribuidora (DIST em SUB)
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str)
            
            # Propriedade/Posse (POS)
            if 'POS' in gdf.columns:
                df['propriedade_codigo'] = gdf['POS'].astype(str)
            
            # Coordenadas
            df['latitude'] = gdf.geometry.y
            df['longitude'] = gdf.geometry.x
            
            df['fonte_camada'] = 'SUB (Oficial BDGD)'
            logger.debug(f"      ✓ {len(df)} subestações extraídas da camada SUB")
        else:
            logger.warning(f"      ⚠ Camada SUB sem campos esperados")
    
    # Camada CTMT (fallback - Barramentos agrupados)
    elif layer_used and layer_used.upper() == 'CTMT':
        logger.debug(f"      Processando camada CTMT (Barramentos - fallback)")
        if 'SUB' in gdf.columns and 'NOM' in gdf.columns:
            # Agrupar por SUB (código da subestação)
            grouped = gdf.groupby('SUB', as_index=False).first()
            logger.debug(f"      {len(grouped)} subestações únicas encontradas")
            
            df['codigo_bdgd'] = grouped['SUB'].astype(str)
            df['codigo'] = grouped['SUB'].astype(str)
            
            # Extrair COD_ID do barramento para referência
            if 'COD_ID' in grouped.columns:
                df['barramento_cod_id'] = grouped['COD_ID'].astype(str)
            
            df['nome'] = grouped['NOM'].astype(str)
            
            # Limpar nome da subestação (remover prefixos redundantes)
            df['nome'] = df.apply(lambda row: _limpar_nome_subestacao(row['nome'], row['codigo']), axis=1)
            logger.debug(f"      Nomes normalizados")
            
            # Distribuidora (DIST em CTMT)
            if 'DIST' in grouped.columns:
                df['dist_codigo'] = grouped['DIST'].astype(str)
            
            # Descrição (DESCR)
            if 'DESCR' in grouped.columns:
                df['descricao'] = grouped['DESCR'].astype(str)
            
            # Tensão nominal (TEN_NOM em CTMT)
            if 'TEN_NOM' in grouped.columns:
                df['tensao_kv'] = pd.to_numeric(grouped['TEN_NOM'], errors='coerce')
            
            # Tensão de operação (TEN_OPE em CTMT)
            if 'TEN_OPE' in grouped.columns:
                df['tensao_operacao_kv'] = pd.to_numeric(grouped['TEN_OPE'], errors='coerce')
            
            # Coordenadas
            df['latitude'] = grouped.geometry.y
            df['longitude'] = grouped.geometry.x
            
            df['fonte_camada'] = 'CTMT (Barramentos - fallback)'
            logger.debug(f"      ✓ {len(df)} subestações extraídas de {len(gdf)} barramentos")
        else:
            logger.warning(f"      ⚠ Camada CTMT sem campos esperados")
    
    # Adicionar campos padrão
    if not df.empty:
        df['distribuidora'] = distribuidora
        df['data_criacao'] = datetime.now()
        df['data_atualizacao'] = datetime.now()
        df['fonte_dados'] = 'aneel_bdgd'
        
        logger.info(f"      ✓ {len(df)} subestações extraídas da camada {layer_used}")
    else:
        logger.warning(f"      ⚠ Nenhuma subestação extraída")
    
    return df


def extract_consumidores(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
    """
    ⚠️ DESCONTINUADA - Esta função foi substituída por:
    - extract_consumidores_bt() para Baixa Tensão
    - extract_consumidores_mt() para Média Tensão
    - extract_consumidores_at() para Alta Tensão
    """
    logger.warning(f"    ⚠ extract_consumidores() foi descontinuada")
    logger.warning(f"    Usar: extract_consumidores_bt(), extract_consumidores_mt(), extract_consumidores_at()")
    return pd.DataFrame()


def extract_consumidores_bt(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
    """Extrai dados de consumidores de Baixa Tensão (UCBT)"""
    logger.info(f"    Extraindo consumidores BT (UCBT)...")
    
    if gdf.empty:
        logger.warning(f"      ⚠ Nenhum dado disponível")
        return pd.DataFrame()
    
    gdf, stats = normalize_geometry(gdf)
    logger.debug(f"      Geometrias: {stats}")
    
    # Mapear campos BDGD para UCBT
    df = map_fields(gdf, FIELD_MAPPING['consumidores_bt'])
    
    if 'distribuidora' not in df.columns or df['distribuidora'].isna().all():
        df['distribuidora'] = distribuidora
    
    # Adicionar campos de auditoria
    df['data_criacao'] = datetime.now()
    df['data_atualizacao'] = datetime.now()
    
    # Remover duplicatas por código
    if 'codigo' in df.columns:
        df = df.drop_duplicates(subset=['codigo'], keep='first')
    
    logger.info(f"      ✓ {len(df)} consumidores BT extraídos")
    return df


def extract_consumidores_mt(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
    """Extrai dados de consumidores de Média Tensão (UCMT)"""
    logger.info(f"    Extraindo consumidores MT (UCMT)...")
    
    if gdf.empty:
        logger.warning(f"      ⚠ Nenhum dado disponível")
        return pd.DataFrame()
    
    gdf, stats = normalize_geometry(gdf)
    logger.debug(f"      Geometrias: {stats}")
    
    # Mapear campos BDGD para UCMT
    df = map_fields(gdf, FIELD_MAPPING['consumidores_mt'])
    
    if 'distribuidora' not in df.columns or df['distribuidora'].isna().all():
        df['distribuidora'] = distribuidora
    
    # Adicionar campos de auditoria
    df['data_criacao'] = datetime.now()
    df['data_atualizacao'] = datetime.now()
    
    # Remover duplicatas por código
    if 'codigo' in df.columns:
        df = df.drop_duplicates(subset=['codigo'], keep='first')
    
    logger.info(f"      ✓ {len(df)} consumidores MT extraídos")
    return df


def extract_consumidores_at(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
    """Extrai dados de consumidores de Alta Tensão (UCAT)"""
    logger.info(f"    Extraindo consumidores AT (UCAT)...")
    
    if gdf.empty:
        logger.warning(f"      ⚠ Nenhum dado disponível")
        return pd.DataFrame()
    
    gdf, stats = normalize_geometry(gdf)
    logger.debug(f"      Geometrias: {stats}")
    
    # Mapear campos BDGD para UCAT
    df = map_fields(gdf, FIELD_MAPPING['consumidores_at'])
    
    if 'distribuidora' not in df.columns or df['distribuidora'].isna().all():
        df['distribuidora'] = distribuidora
    
    # Adicionar campos de auditoria
    df['data_criacao'] = datetime.now()
    df['data_atualizacao'] = datetime.now()
    
    # Remover duplicatas por código
    if 'codigo' in df.columns:
        df = df.drop_duplicates(subset=['codigo'], keep='first')
    
    logger.info(f"      ✓ {len(df)} consumidores AT extraídos")
    return df


def load_transformadores(df: pd.DataFrame, distribuidora: str, transformer_svc: TransformerService) -> int:
    """
    Carrega transformadores usando service centralizado
    (NÃO USAR SQL INLINE - schema é single source of truth)
    
    Returns:
        Número de registros inseridos
    """
    if df.empty:
        logger.warning(f"    ⚠ Nenhum transformador para carregar")
        return 0
    
    try:
        # Schema é gerenciado no banco (veja infrastructure/database/schema.sql (unificado))
        # Service apenas insere dados
        inseridos = transformer_svc.insert(df, distribuidora)
        logger.info(f"    ✓ {inseridos} transformadores carregados")
        return inseridos
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao carregar transformadores: {e}")
        return 0


def load_subestacoes(df: pd.DataFrame, distribuidora: str, substation_svc: SubstationService) -> int:
    """
    Carrega subestações usando service centralizado
    (NÃO USAR SQL INLINE - schema é single source of truth)
    
    Returns:
        Número de registros inseridos
    """
    if df.empty:
        logger.warning(f"    ⚠ Nenhuma subestação para carregar")
        return 0
    
    try:
        # Schema é gerenciado no banco (veja infrastructure/database/schema.sql (unificado))
        # Service apenas insere dados
        inseridos = substation_svc.insert(df, distribuidora)
        logger.info(f"    ✓ {inseridos} subestações carregadas")
        return inseridos
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao carregar subestações: {e}")
        return 0


def load_consumidores(df: pd.DataFrame, distribuidora: str) -> Tuple[int, int]:
    """
    ⚠️ DESCONTINUADA - Esta função foi substituída por:
    - load_consumidores_bt() para Baixa Tensão
    - load_consumidores_mt() para Média Tensão
    - load_consumidores_at() para Alta Tensão
    
    Returns:
        (0, 0) para manter compatibilidade
    """
    logger.warning(f"    ⚠ load_consumidores() foi descontinuada")
    logger.warning(f"    Usar: load_consumidores_bt(), load_consumidores_mt(), load_consumidores_at()")
    return 0, 0


def load_consumidores_bt(df: pd.DataFrame, distribuidora: str) -> Tuple[int, int]:
    """
    Carrega consumidores de Baixa Tensão (UCBT) no banco
    
    Returns:
        (inseridos, atualizados)
    """
    try:
        if df.empty:
            logger.info(f"    ℹ Nenhum consumidor BT para carregar")
            return 0, 0
        
        # Usar to_sql com ifexists='append' para inserir
        df_insert = df.copy()
        df_insert.to_sql('consumidores_bt_aneel', engine, if_exists='append', index=False)
        
        inserted = len(df)
        logger.info(f"    ✓ {inserted} consumidores BT carregados")
        return inserted, 0
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao carregar consumidores BT: {e}")
        return 0, 0


def load_consumidores_mt(df: pd.DataFrame, distribuidora: str) -> Tuple[int, int]:
    """
    Carrega consumidores de Média Tensão (UCMT) no banco
    
    Returns:
        (inseridos, atualizados)
    """
    try:
        if df.empty:
            logger.info(f"    ℹ Nenhum consumidor MT para carregar")
            return 0, 0
        
        # Usar to_sql com ifexists='append' para inserir
        df_insert = df.copy()
        df_insert.to_sql('consumidores_mt_aneel', engine, if_exists='append', index=False)
        
        inserted = len(df)
        logger.info(f"    ✓ {inserted} consumidores MT carregados")
        return inserted, 0
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao carregar consumidores MT: {e}")
        return 0, 0


def load_consumidores_at(df: pd.DataFrame, distribuidora: str) -> Tuple[int, int]:
    """
    Carrega consumidores de Alta Tensão (UCAT) no banco
    
    Returns:
        (inseridos, atualizados)
    """
    try:
        if df.empty:
            logger.info(f"    ℹ Nenhum consumidor AT para carregar")
            return 0, 0
        
        # Usar to_sql com ifexists='append' para inserir
        df_insert = df.copy()
        df_insert.to_sql('consumidores_at_aneel', engine, if_exists='append', index=False)
        
        inserted = len(df)
        logger.info(f"    ✓ {inserted} consumidores AT carregados")
        return inserted, 0
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao carregar consumidores AT: {e}")
        return 0, 0


def process_distribuidora(dist_path: Path, dist_name: str, transformer_svc, substation_svc, consumer_svc, distributor_svc, area_svc) -> Dict:
    """
    Processa uma distribuidora completa usando services centralizados
    
    Returns:
        Estatísticas de processamento
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"Processando distribuidora: {dist_name}")
    logger.info(f"{'='*70}")
    
    stats = {
        'distribuidora': dist_name,
        'distribuidora_real': None,  # Nome verdadeiro extraído dos dados
        'transformadores_inseridos': 0,
        'subestacoes_inseridas': 0,
        'consumidores_inseridos': 0,
        'erros': []
    }
    
    # Detectar estrutura: se dist_path termina com .gdb, já é o caminho do GDB
    if str(dist_path).endswith('.gdb'):
        gdb_path = dist_path
    else:
        gdb_path = dist_path / 'gdb'
    
    if not gdb_path.exists():
        logger.error(f"  ❌ Arquivo GDB não encontrado: {gdb_path}")
        stats['erros'].append(f"GDB not found: {gdb_path}")
        return stats
    
    # Descobrir camadas
    logger.info(f"  Descobrindo camadas...")
    layers = discover_layers(gdb_path)
    
    # 1️⃣ Processar subestações PRIMEIRO (prioridade: SUB > CTMT)
    logger.info(f"  \n  🏢 SUBESTAÇÕES")
    
    # Tentar SUB primeiro (camada oficial BDGD)
    try:
        logger.info(f"    Tentando camada SUB (oficial BDGD)...")
        gdf_sub = gpd.read_file(str(gdb_path), layer='SUB')
        logger.info(f"      ✓ SUB carregada: {len(gdf_sub)} registros")
        
        df = SubstationService.extract(gdf_sub, dist_name)
        
        # Se SUB vazia, tentar CTMT
        if df.empty:
            logger.info(f"    SUB vazia, tentando CTMT (fallback)...")
            try:
                gdf_ctmt = gpd.read_file(str(gdb_path), layer='CTMT')
                logger.info(f"      ✓ CTMT carregada: {len(gdf_ctmt)} registros")
                df = SubstationService.extract(gdf_ctmt, dist_name)
            except Exception as e:
                logger.warning(f"    ⚠ Erro ao carregar CTMT: {e}")
        
        if not df.empty:
            n_inserted = substation_svc.insert(df, dist_name)
            stats['subestacoes_inseridas'] = n_inserted
        else:
            logger.warning(f"    ⚠ Nenhuma subestação carregada (SUB e CTMT vazias)")
    
    except Exception as e:
        logger.error(f"    ❌ Erro ao processar subestações: {e}")
        stats['erros'].append(f"Subestações: {str(e)}")
    
    # 2️⃣ Processar transformadores DEPOIS (com referência para subestações)
    if layers['transformadores']:
        logger.info(f"  \n  📊 TRANSFORMADORES")
        try:
            layer_name = layers['transformadores'][0]
            logger.info(f"    Camada: {layer_name}")
            
            gdf = gpd.read_file(str(gdb_path), layer=layer_name)
            df = TransformerService.extract(gdf, dist_name)
            
            if not df.empty:
                # Extrair nome verdadeiro da distribuidora (primeiro valor único)
                dist_real = df['distribuidora'].unique()
                if len(dist_real) > 0 and pd.notna(dist_real[0]):
                    # Simplificar nome: "IENERGIA_87_2021-02-28..." -> "IENERGIA"
                    stats['distribuidora_real'] = simplificar_nome_distribuidora(str(dist_real[0]))
                    logger.info(f"  ✓ Distribuidora identificada: {stats['distribuidora_real']}")
                
                n_inserted = transformer_svc.insert(df, dist_name)
                stats['transformadores_inseridos'] = n_inserted
        
        except Exception as e:
            logger.error(f"    ❌ Erro ao processar transformadores: {e}")
            stats['erros'].append(f"Transformadores: {str(e)}")
    
    # 3️⃣ Processar consumidores separados por tensão (BT, MT, AT)
    logger.info(f"  \n  👤 CONSUMIDORES")
    
    # CONSUMIDORES DE BAIXA TENSÃO (UCBT)
    try:
        logger.info(f"    Processando UCBT (Consumidores de Baixa Tensão)...")
        gdf_ucbt = gpd.read_file(str(gdb_path), layer='UCBT')
        logger.info(f"      ✓ UCBT carregada: {len(gdf_ucbt)} registros")
        
        df_bt = ConsumerService.extract_bt(gdf_ucbt, dist_name)
        if not df_bt.empty:
            n_inserted = consumer_svc.insert_bt(df_bt, dist_name)
            stats['consumidores_bt_inseridos'] = n_inserted
    except Exception as e:
        logger.debug(f"    ℹ UCBT não disponível ou erro: {e}")
        stats['consumidores_bt_inseridos'] = 0
    
    # CONSUMIDORES DE MÉDIA TENSÃO (UCMT)
    try:
        logger.info(f"    Processando UCMT (Consumidores de Média Tensão)...")
        gdf_ucmt = gpd.read_file(str(gdb_path), layer='UCMT')
        logger.info(f"      ✓ UCMT carregada: {len(gdf_ucmt)} registros")
        
        df_mt = ConsumerService.extract_mt(gdf_ucmt, dist_name)
        if not df_mt.empty:
            n_inserted = consumer_svc.insert_mt(df_mt, dist_name)
            stats['consumidores_mt_inseridos'] = n_inserted
    except Exception as e:
        logger.debug(f"    ℹ UCMT não disponível ou erro: {e}")
        stats['consumidores_mt_inseridos'] = 0
    
    # CONSUMIDORES DE ALTA TENSÃO (UCAT)
    try:
        logger.info(f"    Processando UCAT (Consumidores de Alta Tensão)...")
        gdf_ucat = gpd.read_file(str(gdb_path), layer='UCAT')
        logger.info(f"      ✓ UCAT carregada: {len(gdf_ucat)} registros")
        
        df_at = ConsumerService.extract_at(gdf_ucat, dist_name)
        if not df_at.empty:
            n_inserted = consumer_svc.insert_at(df_at, dist_name)
            stats['consumidores_at_inseridos'] = n_inserted
    except Exception as e:
        logger.debug(f"    ℹ UCAT não disponível ou erro: {e}")
        stats['consumidores_at_inseridos'] = 0
    
    # Total consumidores
    total_consumidores = (stats.get('consumidores_bt_inseridos', 0) + 
                         stats.get('consumidores_mt_inseridos', 0) + 
                         stats.get('consumidores_at_inseridos', 0))
    stats['consumidores_inseridos'] = total_consumidores
    logger.info(f"  📊 Total consumidores carregados: {total_consumidores}")
    
    # 4️⃣ 🗺️ CALCULAR ÁREAS DOS TRANSFORMADORES (ConvexHull + Buffer)
    logger.info(f"  \n  🗺️ ÁREAS DE COBERTURA DOS TRANSFORMADORES")
    
    dist_final = stats['distribuidora_real'] if stats['distribuidora_real'] else dist_name
    
    # Calcular para cada tipo de tensão
    for tipo_tensao in ['BT', 'MT', 'AT']:
        try:
            n_areas = area_svc.calculate(tipo_tensao, dist_final)
            stats[f'areas_{tipo_tensao.lower()}_calculadas'] = n_areas
        except Exception as e:
            logger.warning(f"    ⚠ Erro ao calcular áreas {tipo_tensao}: {e}")
            stats[f'areas_{tipo_tensao.lower()}_calculadas'] = 0
    
    # 5️⃣ Atualizar tabela de distribuidoras
    if stats['distribuidora_real']:
        try:
            # Calcular potência total dos transformadores
            potencia_total = 0
            total_consumidores = (stats.get('consumidores_bt_inseridos', 0) + 
                                 stats.get('consumidores_mt_inseridos', 0) + 
                                 stats.get('consumidores_at_inseridos', 0))
            
            try:
                with engine.begin() as conn:
                    result = conn.execute(text("""
                        SELECT COALESCE(SUM(potencia_kva), 0) as total_kva
                        FROM transformadores_aneel
                        WHERE distribuidora = :distribuidora
                    """), {'distribuidora': stats['distribuidora_real']})
                    row = result.fetchone()
                    if row:
                        potencia_total = float(row[0])
            except Exception as e:
                logger.warning(f"  ⚠ Erro ao calcular potência total: {e}")
            
            # Popular distribuidora com todos os dados
            distributor_svc.upsert(
                dist_nome=stats['distribuidora_real'],
                total_trafo=stats['transformadores_inseridos'],
                total_sub=stats['subestacoes_inseridas'],
                total_consumidores=total_consumidores,
                potencia_total_kva=potencia_total if potencia_total > 0 else None,
                dist_arquivo=dist_name
            )
            logger.info(f"  ✓ Distribuidora '{stats['distribuidora_real']}' populada")
        except Exception as e:
            logger.warning(f"  ⚠ Erro ao atualizar tabela de distribuidoras: {e}")
    
    return stats


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--distribuidora', type=str, help='Processar apenas uma distribuidora específica')
    parser.add_argument('--debug', action='store_true', help='Modo debug')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # ✅ Inicializar services centralizados
    transformer_svc = TransformerService(engine)
    substation_svc = SubstationService(engine)
    consumer_svc = ConsumerService(engine)
    distributor_svc = DistributorService(engine)
    area_svc = AreaService(engine)
    
    # Listar distribuidoras
    logger.info(f"\n{'='*70}")
    logger.info(f"ANEEL BDGD - ETL LOCAL")
    logger.info(f"{'='*70}\n")
    
    logger.info(f"Buscando distribuidoras em: {ANEEL_BDGD_DIR}\n")
    distribuidoras = get_distribuidoras()
    
    if not distribuidoras:
        logger.error(f"❌ Nenhuma distribuidora encontrada!")
        return
    
    logger.info(f"✓ {len(distribuidoras)} distribuidoras encontradas:\n")
    for dist_name in distribuidoras.keys():
        logger.info(f"  - {dist_name}")
    
    # Filtrar por distribuidora se especificada
    if args.distribuidora:
        if args.distribuidora not in distribuidoras:
            logger.error(f"❌ Distribuidora não encontrada: {args.distribuidora}")
            return
        distribuidoras = {args.distribuidora: distribuidoras[args.distribuidora]}
    
    # Processar cada distribuidora
    all_stats = []
    for dist_name, dist_path in distribuidoras.items():
        stats = process_distribuidora(dist_path, dist_name, transformer_svc, substation_svc, consumer_svc, distributor_svc, area_svc)
        all_stats.append(stats)
    
    # 🔄 Atualizar totais reais das distribuidoras (após processamento completo)
    logger.info(f"\n  ✅ Atualizando totais reais das distribuidoras...\n")
    try:
        with engine.begin() as conn:
            # Atualizar total_transformadores com dados reais do banco
            conn.execute(text("""
                UPDATE distribuidoras_aneel d SET
                    total_transformadores = COALESCE(
                        (SELECT COUNT(*) FROM transformadores_aneel t WHERE t.distribuidora = d.nome),
                        0
                    )
                WHERE ativo = TRUE
            """))
            
            # Atualizar total_subestacoes com dados reais do banco
            conn.execute(text("""
                UPDATE distribuidoras_aneel d SET
                    total_subestacoes = COALESCE(
                        (SELECT COUNT(*) FROM subestacoes_aneel s WHERE s.distribuidora = d.nome),
                        0
                    )
                WHERE ativo = TRUE
            """))
            
            # Atualizar potencia_total_kva com dados reais do banco
            conn.execute(text("""
                UPDATE distribuidoras_aneel d SET
                    potencia_total_kva = COALESCE(
                        (SELECT SUM(potencia_kva) FROM transformadores_aneel t WHERE t.distribuidora = d.nome),
                        0
                    )
                WHERE ativo = TRUE
            """))
            
            logger.info(f"  ✅ Totais das distribuidoras atualizados com sucesso!")
    except Exception as e:
        logger.warning(f"  ⚠ Erro ao atualizar totais: {e}")
    
    # Resumo final
    logger.info(f"\n{'='*70}")
    logger.info(f"RESUMO FINAL")
    logger.info(f"{'='*70}\n")
    
    total_transformadores = sum(s['transformadores_inseridos'] for s in all_stats)
    total_subestacoes = sum(s['subestacoes_inseridas'] for s in all_stats)
    
    for stats in all_stats:
        logger.info(f"  {stats['distribuidora']:<30} | "
                   f"Trafo: {stats['transformadores_inseridos']:>6} | "
                   f"Sub: {stats['subestacoes_inseridas']:>6}")
        if stats['erros']:
            for erro in stats['erros']:
                logger.warning(f"    ⚠ {erro}")
    
    logger.info(f"\n  {'TOTAL':<30} | "
               f"Trafo: {total_transformadores:>6} | "
               f"Sub: {total_subestacoes:>6}\n")
    
    logger.info(f"✅ ETL Concluído!")


if __name__ == '__main__':
    main()

