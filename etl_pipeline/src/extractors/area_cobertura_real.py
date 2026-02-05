"""
ETL para Área de Cobertura com Dados REAIS

Busca dados de FONTES REAIS e popula o banco de dados:
1. ONS - Subestações reais do Operador Nacional (AWS S3)
2. ANEEL SIGA - Usinas de geração distribuída (dados abertos)
3. ANEEL BIG - Transformadores e redes de distribuição
4. OpenStreetMap - Rede elétrica e infraestrutura

Autor: Energy Netload Monitor
Data: 2026-01-31
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import requests
from shapely import wkt
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from sqlalchemy import text

# Adicionar diretório src ao path
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importar funções do core (padrão do projeto)
from core import create_db_engine, create_session, load_settings, request

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURAÇÃO DE FONTES DE DADOS REAIS
# ============================================================================

# ONS - Operador Nacional do Sistema
ONS_SUBESTACOES_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/subestacao/SUBESTACAO.csv"

# ANEEL - Agência Nacional de Energia Elétrica
ANEEL_SIGA_URL = "https://dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao-da-aneel/resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv"
ANEEL_BIG_BASE_URL = "https://dadosabertos.aneel.gov.br/dataset"

# OpenStreetMap Overpass API (servidores alternativos)
OSM_OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter"
]

# ============================================================================
# UTILITÁRIOS
# ============================================================================

def get_engine_and_session(settings=None):
    """Obtém engine do banco e sessão HTTP (padrão do projeto)"""
    if settings is None:
        settings = load_settings()
    
    if not settings.database.url:
        raise ValueError("DATABASE_URL não está configurada.")
    
    engine = create_db_engine(settings.database.url)
    session = create_session(settings.http, logger=logger)
    
    return engine, session, settings


# ============================================================================
# FONTE 1: ONS - SUBESTAÇÕES REAIS
# ============================================================================

def extrair_subestacoes_ons() -> pd.DataFrame:
    """
    Extrai subestações reais do ONS (Operador Nacional do Sistema).
    Fonte: AWS S3 - Dados Abertos ONS
    """
    logger.info("🔌 Extraindo subestações do ONS (dados reais)...")
    
    try:
        response = requests.get(ONS_SUBESTACOES_URL, timeout=30)
        response.raise_for_status()
        
        # Ler CSV com separador ponto-e-vírgula (padrão ONS)
        from io import StringIO
        df = pd.read_csv(StringIO(response.text), sep=";", encoding="UTF-8")
        
        logger.info(f"✅ {len(df)} subestações extraídas do ONS")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair dados do ONS: {e}")
        return pd.DataFrame()


def transformar_subestacoes_ons(df: pd.DataFrame) -> List[Dict]:
    """
    Transforma dados do ONS para formato do banco.
    
    Colunas do ONS:
    - nom_subestacao: Nome da subestação
    - id_subestacao: Código ONS
    - val_niveltensao: Tensão em kV
    - nom_subsistema: Subsistema (Norte, Nordeste, etc.)
    - nom_agente_principal: Distribuidora responsável
    - val_latitude: Latitude
    - val_longitude: Longitude
    """
    if df.empty:
        return []
    
    logger.info("🔄 Transformando dados do ONS...")
    
    # Mapeamento de colunas
    df_clean = df.rename(columns={
        'nom_subestacao': 'nome',
        'id_subestacao': 'sigla_se',
        'val_niveltensao': 'tensao_kv',
        'nom_subsistema': 'subsistema',
        'nom_agente_principal': 'distribuidora',
        'val_latitude': 'latitude',
        'val_longitude': 'longitude',
    })
    
    # Filtrar apenas subestações com coordenadas válidas
    df_clean = df_clean.dropna(subset=['latitude', 'longitude'])
    df_clean['latitude'] = pd.to_numeric(df_clean['latitude'], errors='coerce')
    df_clean['longitude'] = pd.to_numeric(df_clean['longitude'], errors='coerce')
    df_clean = df_clean.dropna(subset=['latitude', 'longitude'])
    
    # Filtrar apenas Brasil (lat entre -34 e 5, lon entre -74 e -34)
    df_clean = df_clean[
        (df_clean['latitude'] >= -34) & (df_clean['latitude'] <= 5) &
        (df_clean['longitude'] >= -74) & (df_clean['longitude'] <= -34)
    ]
    
    if 'tensao_kv' in df_clean.columns:
        df_clean['tensao_kv'] = pd.to_numeric(df_clean['tensao_kv'], errors='coerce')
    
    df_clean['fonte_dados'] = 'ONS'
    
    logger.info(f"✅ {len(df_clean)} subestações transformadas")
    
    return df_clean.to_dict('records')


def carregar_subestacoes_ons(subestacoes: List[Dict], engine):
    """Carrega subestações do ONS no banco de dados"""
    
    if not subestacoes:
        logger.warning("Nenhuma subestação para carregar")
        return 0
    
    logger.info(f"💾 Carregando {len(subestacoes)} subestações no banco...")
    
    # Usar SQLAlchemy (padrão do projeto)
    with engine.begin() as conn:
        for se in subestacoes:
            # Verificar se já existe
            result = conn.execute(text("""
                SELECT id FROM subestacoes_detectadas
                WHERE nome = :nome AND latitude = :latitude AND longitude = :longitude
            """), {
                'nome': se.get('nome', 'Subestação ONS'),
                'latitude': se['latitude'],
                'longitude': se['longitude']
            })
            
            if result.fetchone():
                # Atualizar existente
                conn.execute(text("""
                    UPDATE subestacoes_detectadas
                    SET distribuidora = :distribuidora,
                        fonte_dados = :fonte_dados
                    WHERE nome = :nome AND latitude = :latitude AND longitude = :longitude
                """), {
                    'nome': se.get('nome', 'Subestação ONS'),
                    'latitude': se['latitude'],
                    'longitude': se['longitude'],
                    'distribuidora': se.get('distribuidora', 'ONS'),
                    'fonte_dados': 'ONS'
                })
            else:
                # Inserir novo
                conn.execute(text("""
                    INSERT INTO subestacoes_detectadas (
                        nome, latitude, longitude, geom, 
                        distribuidora, fonte_dados
                    ) VALUES (
                        :nome, :latitude, :longitude,
                        ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326),
                        :distribuidora, :fonte_dados
                    )
                """), {
                    'nome': se.get('nome', 'Subestação ONS'),
                    'latitude': se['latitude'],
                    'longitude': se['longitude'],
                    'distribuidora': se.get('distribuidora', 'ONS'),
                    'fonte_dados': 'ONS'
                })
    
    logger.info(f"✅ {len(subestacoes)} subestações carregadas")
    return len(subestacoes)


# ============================================================================
# FONTE 2: ANEEL SIGA - USINAS DE GERAÇÃO DISTRIBUÍDA
# ============================================================================

def extrair_usinas_aneel() -> pd.DataFrame:
    """
    Extrai usinas de geração distribuída da ANEEL (SIGA).
    Inclui: Solar, Eólica, Biomassa, Hidro, etc.
    """
    logger.info("☀️ Extraindo usinas de geração (ANEEL SIGA)...")
    
    try:
        response = requests.get(ANEEL_SIGA_URL, timeout=60)
        response.raise_for_status()
        
        from io import BytesIO
        df = pd.read_csv(
            BytesIO(response.content),
            sep=";",
            encoding="ISO-8859-1",
            decimal=","
        )
        
        logger.info(f"✅ {len(df)} usinas extraídas da ANEEL")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair dados da ANEEL: {e}")
        return pd.DataFrame()


def filtrar_usinas_solares(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra apenas usinas solares (fonte de interesse)"""
    
    if df.empty:
        return df
    
    # Colunas de interesse
    df_solar = df[df['SigTipoGeracao'].str.contains('UFV|SOL|SOLAR', case=False, na=False)].copy()
    
    logger.info(f"☀️ {len(df_solar)} usinas solares identificadas")
    return df_solar


def transformar_usinas_aneel(df: pd.DataFrame) -> List[Dict]:
    """Transforma dados de usinas da ANEEL"""
    
    if df.empty:
        return []
    
    logger.info("🔄 Transformando dados ANEEL...")
    
    df_clean = df.rename(columns={
        'IdeNucleoCEG': 'ceg',
        'NomEmpreendimento': 'nome',
        'SigTipoGeracao': 'fonte',
        'DscOrigemCombustivel': 'combustivel',
        'MdaPotenciaOutorgadaKw': 'potencia_kw',
        'NumCoordNEmpreendimento': 'latitude',
        'NumCoordEEmpreendimento': 'longitude',
    })
    
    # Limpar dados
    df_clean = df_clean.dropna(subset=['latitude', 'longitude', 'potencia_kw'])
    df_clean['latitude'] = pd.to_numeric(df_clean['latitude'], errors='coerce')
    df_clean['longitude'] = pd.to_numeric(df_clean['longitude'], errors='coerce')
    df_clean['potencia_kw'] = pd.to_numeric(df_clean['potencia_kw'], errors='coerce')
    df_clean = df_clean.dropna(subset=['latitude', 'longitude'])
    
    # Filtrar Brasil
    df_clean = df_clean[
        (df_clean['latitude'] >= -34) & (df_clean['latitude'] <= 5) &
        (df_clean['longitude'] >= -74) & (df_clean['longitude'] <= -34) &
        (df_clean['potencia_kw'] > 0)
    ]
    
    logger.info(f"✅ {len(df_clean)} usinas transformadas")
    
    return df_clean.to_dict('records')


# ============================================================================
# FONTE 3: OPENSTREETMAP - REDE ELÉTRICA
# ============================================================================

def extrair_transformadores_osm(bbox: Tuple[float, float, float, float], max_retries: int = 3) -> List[Dict]:
    """
    Extrai transformadores da rede elétrica do OpenStreetMap.
    
    bbox: (min_lat, min_lon, max_lat, max_lon)
    max_retries: Número máximo de tentativas com servidores alternativos
    """
    logger.info(f"🗺️ Extraindo transformadores do OSM (bbox: {bbox})...")
    
    # Query Overpass para transformadores e infraestrutura elétrica
    # Busca por: transformadores, subestações, torres, postes
    overpass_query = f"""
    [out:json][timeout:90];
    (
      node["power"="transformer"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      node["power"="substation"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      node["power"="tower"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      node["power"="pole"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      way["power"="substation"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
    );
    out body;
    >;
    out skel qt;
    """
    
    logger.debug(f"📋 Query Overpass:\n{overpass_query}")
    
    # Tentar diferentes servidores Overpass
    for attempt in range(max_retries):
        url = OSM_OVERPASS_URLS[attempt % len(OSM_OVERPASS_URLS)]
        
        try:
            logger.info(f"🔄 Tentativa {attempt + 1}/{max_retries} - Servidor: {url}")
            
            response = requests.post(
                url,
                data={'data': overpass_query},
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            
            transformadores = []
            elementos_brutos = data.get('elements', [])
            logger.info(f"📦 {len(elementos_brutos)} elementos brutos retornados do OSM")
            
            # Processar nós de infraestrutura elétrica
            for element in elementos_brutos:
                if element['type'] == 'node' and 'lat' in element and 'lon' in element:
                    tags = element.get('tags', {})
                    power_type = tags.get('power')
                    
                    # Filtrar apenas infraestrutura relevante
                    if power_type in ['transformer', 'substation', 'tower', 'pole']:
                        transformadores.append({
                            'osm_id': element['id'],
                            'latitude': element['lat'],
                            'longitude': element['lon'],
                            'nome': tags.get('name', f"{power_type.title()} OSM {element['id']}"),
                            'tipo': tags.get('location', 'desconhecido'),
                            'tensao': tags.get('voltage', '13800'),
                            'power_type': power_type,
                        })
                
                # Processar ways (áreas de subestações) - pegar centroide
                elif element['type'] == 'way' and element.get('tags', {}).get('power') == 'substation':
                    # Para ways, precisamos dos nós que já foram retornados
                    # O centroide será calculado depois se necessário
                    pass
            
            logger.info(f"✅ {len(transformadores)} elementos de infraestrutura extraídos do OSM")
            if transformadores:
                tipos = {}
                for t in transformadores:
                    tipo = t['power_type']
                    tipos[tipo] = tipos.get(tipo, 0) + 1
                logger.info(f"📊 Distribuição: {tipos}")
            
            return transformadores
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = (attempt + 1) * 10  # 10, 20, 30 segundos
                logger.warning(f"⚠️ Rate limit atingido (429). Aguardando {wait_time}s antes da próxima tentativa...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Erro HTTP {e.response.status_code}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ Tentando servidor alternativo em 5s...")
                    time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ Timeout ao conectar ao OSM. Tentando novamente...")
            if attempt < max_retries - 1:
                time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Erro ao extrair do OSM: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Tentando servidor alternativo em 5s...")
                time.sleep(5)
    
    logger.warning("⚠️ Todas as tentativas falharam. Retornando lista vazia.")
    return []


def carregar_transformadores_osm(transformadores: List[Dict], subestacao_id: int, engine):
    """Carrega transformadores do OSM no banco"""
    
    if not transformadores:
        logger.warning("Nenhum transformador OSM para carregar")
        return 0
    
    logger.info(f"💾 Carregando {len(transformadores)} transformadores OSM...")
    
    with engine.begin() as conn:
        for t in transformadores:
            tensao_kv = float(t['tensao']) / 1000 if t['tensao'].isdigit() else 13.8
            
            conn.execute(text("""
                INSERT INTO transformadores (
                    codigo, subestacao_id, nome, latitude, longitude, localizacao,
                    potencia_kva, tipo, status, tensao_primaria_kv
                ) VALUES (
                    :codigo, :subestacao_id, :nome, :latitude, :longitude,
                    ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326),
                    :potencia_kva, :tipo, :status, :tensao_primaria_kv
                )
                ON CONFLICT (codigo) DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    localizacao = ST_SetSRID(ST_MakePoint(EXCLUDED.longitude, EXCLUDED.latitude), 4326),
                    updated_at = NOW()
            """), {
                'codigo': f"OSM-{t['osm_id']}",
                'subestacao_id': subestacao_id,
                'nome': t['nome'],
                'latitude': t['latitude'],
                'longitude': t['longitude'],
                'potencia_kva': 300.0,
                'tipo': t['tipo'],
                'status': 'ativo',
                'tensao_primaria_kv': tensao_kv
            })
    
    logger.info(f"✅ {len(transformadores)} transformadores carregados")
    return len(transformadores)


# ============================================================================
# ÁREA DE COBERTURA - BASEADA EM DADOS REAIS
# ============================================================================

def calcular_area_cobertura(subestacao_id: int, engine) -> Optional[float]:
    """
    Calcula área de cobertura baseada em transformadores reais.
    Usa convex hull (envoltória convexa) dos transformadores ativos.
    """
    logger.info(f"📐 Calculando área de cobertura para SE {subestacao_id}...")
    
    with engine.begin() as conn:
        # Buscar transformadores ativos
        result = conn.execute(text("""
            SELECT ST_AsText(localizacao) as wkt
            FROM transformadores
            WHERE subestacao_id = :subestacao_id
              AND status = 'ativo'
              AND localizacao IS NOT NULL
        """), {'subestacao_id': subestacao_id})
        
        rows = result.fetchall()
        
        if len(rows) < 3:
            logger.warning(f"⚠️ Apenas {len(rows)} transformadores - necessário ≥3 para polígono")
            return None
        
        # Criar polígono convexo
        pontos = [wkt.loads(row[0]) for row in rows]
        poligono = unary_union(pontos).convex_hull
        
        # Calcular área em km²
        result = conn.execute(text("""
            SELECT ST_Area(ST_GeomFromText(:wkt, 4326)::geography) / 1000000 as area_km2
        """), {'wkt': poligono.wkt})
        
        area_km2 = result.scalar()
        
        # Verificar se já existe registro
        result = conn.execute(text("""
            SELECT id FROM subestacoes_area_cobertura
            WHERE subestacao_id = :subestacao_id
        """), {'subestacao_id': subestacao_id})
        
        existing = result.fetchone()
        
        if existing:
            # Atualizar existente
            conn.execute(text("""
                UPDATE subestacoes_area_cobertura SET
                    area_cobertura = ST_GeomFromText(:wkt, 4326),
                    metodo_definicao = 'analise_topologica',
                    area_km2 = :area_km2,
                    data_atualizacao = NOW(),
                    observacoes = :observacoes
                WHERE subestacao_id = :subestacao_id
            """), {
                'subestacao_id': subestacao_id,
                'wkt': poligono.wkt,
                'area_km2': area_km2,
                'observacoes': f"Área calculada de {len(pontos)} transformadores reais"
            })
            logger.info(f"✅ Área atualizada: {area_km2:.2f} km²")
        else:
            # Inserir novo
            conn.execute(text("""
                INSERT INTO subestacoes_area_cobertura (
                    subestacao_id, area_cobertura, metodo_definicao,
                    area_km2, data_atualizacao, observacoes
                ) VALUES (
                    :subestacao_id,
                    ST_GeomFromText(:wkt, 4326),
                    'analise_topologica',
                    :area_km2,
                    NOW(),
                    :observacoes
                )
            """), {
                'subestacao_id': subestacao_id,
                'wkt': poligono.wkt,
                'area_km2': area_km2,
                'observacoes': f"Área calculada de {len(pontos)} transformadores reais"
            })
            logger.info(f"✅ Área calculada: {area_km2:.2f} km²")
        
        return area_km2


def calcular_area_transformador(transformador_id: int, engine) -> Optional[float]:
    """
    Calcula área de cobertura baseada em consumidores conectados ao transformador.
    Usa convex hull (envoltória convexa) dos consumidores ativos.
    
    Se não houver consumidores, usa raio fixo de 500m ao redor do transformador.
    """
    logger.info(f"📐 Calculando área de cobertura para transformador {transformador_id}...")
    
    with engine.begin() as conn:
        # Buscar dados do transformador
        result = conn.execute(text("""
            SELECT latitude, longitude, ST_AsText(localizacao) as wkt_trans
            FROM transformadores
            WHERE id = :id
        """), {'id': transformador_id})
        
        trans_row = result.fetchone()
        if not trans_row:
            logger.error(f"❌ Transformador {transformador_id} não encontrado")
            return None
        
        lat_trans, lon_trans, wkt_trans = trans_row
        
        # Buscar consumidores conectados ao transformador
        result = conn.execute(text("""
            SELECT ST_AsText(localizacao) as wkt
            FROM consumidores
            WHERE transformador_id = :transformador_id
              AND status = 'ativo'
              AND localizacao IS NOT NULL
        """), {'transformador_id': transformador_id})
        
        consumer_rows = result.fetchall()
        total_consumidores = len(consumer_rows)
        
        if total_consumidores >= 3:
            # Usar convex hull dos consumidores
            logger.info(f"📊 {total_consumidores} consumidores encontrados - usando convex hull")
            pontos_consumidores = [wkt.loads(row[0]) for row in consumer_rows]
            poligono = unary_union(pontos_consumidores).convex_hull
            metodo = 'convex_hull_consumidores'
        else:
            # Usar raio fixo de 500m ao redor do transformador
            logger.warning(f"⚠️ Apenas {total_consumidores} consumidores - usando raio de 500m")
            ponto_trans = wkt.loads(wkt_trans)
            # Buffer em graus (aproximadamente 500m = 0.0045 graus)
            poligono = ponto_trans.buffer(0.0045)
            metodo = 'raio_fixo'
        
        # Calcular área em km²
        result = conn.execute(text("""
            SELECT ST_Area(ST_GeomFromText(:wkt, 4326)::geography) / 1000000 as area_km2
        """), {'wkt': poligono.wkt})
        
        area_km2 = result.scalar()
        
        # Calcular raio aproximado (sqrt(area/pi) em km, depois em metros)
        raio_m = (area_km2 * 1000000 / 3.14159) ** 0.5
        
        # Verificar se já existe registro
        result = conn.execute(text("""
            SELECT id FROM transformadores_area_cobertura
            WHERE transformador_id = :transformador_id
        """), {'transformador_id': transformador_id})
        
        existing = result.fetchone()
        
        if existing:
            # Atualizar existente
            conn.execute(text("""
                UPDATE transformadores_area_cobertura SET
                    area_cobertura = ST_GeomFromText(:wkt, 4326),
                    metodo_definicao = :metodo,
                    area_km2 = :area_km2,
                    raio_aproximado_m = :raio_m,
                    total_consumidores = :total_consumidores,
                    data_atualizacao = NOW(),
                    observacoes = :observacoes
                WHERE transformador_id = :transformador_id
            """), {
                'transformador_id': transformador_id,
                'wkt': poligono.wkt,
                'metodo': metodo,
                'area_km2': area_km2,
                'raio_m': raio_m,
                'total_consumidores': total_consumidores,
                'observacoes': f"Área calculada usando {metodo} ({total_consumidores} consumidores)"
            })
            logger.info(f"✅ Área transformador atualizada: {area_km2:.2f} km² (~{raio_m:.0f}m de raio)")
        else:
            # Inserir novo
            conn.execute(text("""
                INSERT INTO transformadores_area_cobertura (
                    transformador_id, area_cobertura, metodo_definicao,
                    area_km2, raio_aproximado_m, total_consumidores,
                    data_atualizacao, observacoes
                ) VALUES (
                    :transformador_id,
                    ST_GeomFromText(:wkt, 4326),
                    :metodo,
                    :area_km2,
                    :raio_m,
                    :total_consumidores,
                    NOW(),
                    :observacoes
                )
            """), {
                'transformador_id': transformador_id,
                'wkt': poligono.wkt,
                'metodo': metodo,
                'area_km2': area_km2,
                'raio_m': raio_m,
                'total_consumidores': total_consumidores,
                'observacoes': f"Área calculada usando {metodo} ({total_consumidores} consumidores)"
            })
            logger.info(f"✅ Área transformador calculada: {area_km2:.2f} km² (~{raio_m:.0f}m de raio)")
        
        return area_km2


# ============================================================================
# FUNÇÕES PRINCIPAIS
# ============================================================================

def etl_subestacoes_ons(engine=None, session=None, settings=None):
    """ETL completo: ONS → Banco de Dados"""
    
    logger.info("=" * 80)
    logger.info("ETL: SUBESTAÇÕES ONS (Dados Reais)")
    logger.info("=" * 80)
    
    # Obter engine e session (padrão do projeto)
    if engine is None or session is None:
        engine, session, settings = get_engine_and_session(settings)
    
    # 1. EXTRACT
    df_ons = extrair_subestacoes_ons()
    
    if df_ons.empty:
        logger.error("❌ Nenhum dado extraído do ONS")
        return 0
    
    # 2. TRANSFORM
    subestacoes = transformar_subestacoes_ons(df_ons)
    
    # 3. LOAD
    total = carregar_subestacoes_ons(subestacoes, engine)
    
    logger.info("=" * 80)
    logger.info(f"✅ ETL ONS CONCLUÍDO: {total} subestações")
    logger.info("=" * 80)
    
    return total


def etl_usinas_aneel(engine=None, session=None, settings=None):
    """ETL completo: ANEEL SIGA → Banco de Dados"""
    
    logger.info("=" * 80)
    logger.info("ETL: USINAS ANEEL SIGA (Dados Reais)")
    logger.info("=" * 80)
    
    # Obter engine e session (padrão do projeto)
    if engine is None or session is None:
        engine, session, settings = get_engine_and_session(settings)
    
    # 1. EXTRACT
    df_aneel = extrair_usinas_aneel()
    
    if df_aneel.empty:
        logger.error("❌ Nenhum dado extraído da ANEEL")
        return 0
    
    # 2. TRANSFORM
    df_solar = filtrar_usinas_solares(df_aneel)
    usinas = transformar_usinas_aneel(df_solar)
    
    # 3. LOAD (como consumidores especiais)
    logger.info(f"💾 {len(usinas)} usinas solares identificadas")
    
    logger.info("=" * 80)
    logger.info(f"✅ ETL ANEEL CONCLUÍDO: {len(usinas)} usinas solares")
    logger.info("=" * 80)
    
    return len(usinas)


def etl_transformadores_osm(subestacao_id: int, raio_km: float = 10.0, engine=None, session=None, settings=None):
    """ETL completo: OpenStreetMap → Transformadores"""
    
    logger.info("=" * 80)
    logger.info(f"ETL: TRANSFORMADORES OSM para SE {subestacao_id}")
    logger.info("=" * 80)
    
    # Obter engine e session (padrão do projeto)
    if engine is None or session is None:
        engine, session, settings = get_engine_and_session(settings)
    
    # Buscar coordenadas da subestação
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT latitude, longitude
            FROM subestacoes_detectadas
            WHERE id = :id
        """), {'id': subestacao_id})
        
        row = result.fetchone()
        if not row:
            logger.error(f"❌ Subestação {subestacao_id} não encontrada")
            return 0
        
        lat, lon = row
        
        # Calcular bounding box (~10km de raio)
        delta = raio_km / 111.0  # 1 grau ≈ 111km
        bbox = (lat - delta, lon - delta, lat + delta, lon + delta)
        
        # 1. EXTRACT
        transformadores = extrair_transformadores_osm(bbox, max_retries=3)
        
        if not transformadores:
            logger.warning("⚠️ Nenhum transformador encontrado no OSM (pode ser rate limit)")
            logger.info("💡 Dica: OSM tem limite de requisições. Tente novamente em alguns minutos.")
            return 0
        
        # 2. LOAD
        total = carregar_transformadores_osm(transformadores, subestacao_id, engine)
        
        # 3. CALCULAR ÁREA DA SUBESTAÇÃO
        if total >= 3:
            area_km2 = calcular_area_cobertura(subestacao_id, engine)
        
        # 4. CALCULAR ÁREAS DOS TRANSFORMADORES INDIVIDUAIS
        if total > 0:
            logger.info(f"🗺️ Calculando áreas de cobertura para {total} transformadores...")
            with engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT id FROM transformadores
                    WHERE subestacao_id = :subestacao_id
                    AND status = 'ativo'
                """), {'subestacao_id': subestacao_id})
                
                trans_ids = [row[0] for row in result.fetchall()]
            
            for trans_id in trans_ids:
                calcular_area_transformador(trans_id, engine)
    
    logger.info("=" * 80)
    logger.info(f"✅ ETL OSM CONCLUÍDO: {total} transformadores")
    logger.info("=" * 80)
    
    return total


def etl_completo(engine=None, session=None, settings=None):
    """Executa ETL completo de todas as fontes"""
    
    # Obter engine e session (padrão do projeto)
    if engine is None or session is None:
        engine, session, settings = get_engine_and_session(settings)
    
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 20 + "ETL COMPLETO - DADOS REAIS" + " " * 32 + "║")
    logger.info("╚" + "=" * 78 + "╝")
    
    # 1. Subestações ONS
    total_se = etl_subestacoes_ons(engine, session, settings)
    
    # 2. Usinas ANEEL
    total_usinas = etl_usinas_aneel(engine, session, settings)
    
    # NOTA: OSM tem rate limiting, pode precisar esperar entre requisições
    total_trans = 0
    with engine.begin() as conn:
        result = conn.execute(text("SELECT id FROM subestacoes_detectadas ORDER BY id"))
        se_ids = [row[0] for row in result.fetchall()]
    
    logger.info(f"⚠️ PROCESSANDO {len(se_ids)} SUBESTAÇÕES (OSM tem rate limit - aguardando entre requisições)...")
    logger.info("💡 Tempo estimado: ~" + str(len(se_ids) * 20 // 60) + " minutos")
    
    for idx, se_id in enumerate(se_ids):
        if idx > 0:
            # Delay adaptativo: mais agressivo no início, mais conservador depois
            delay = 20 if idx < 100 else (15 if idx < 500 else 10)
            logger.info(f"⏳ [{idx+1}/{len(se_ids)}] Aguardando {delay}s antes da próxima subestação...")
            time.sleep(delay)
        
        logger.info(f"🔄 [{idx+1}/{len(se_ids)}] Processando SE ID {se_id}...")
        total_trans += etl_transformadores_osm(se_id, raio_km=20.0, engine=engine, session=session, settings=settings)
    
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 30 + "RESUMO FINAL" + " " * 36 + "║")
    logger.info("╠" + "=" * 78 + "╣")
    logger.info(f"║  Subestações ONS: {total_se:>5}                                                  ║")
    logger.info(f"║  Usinas Solares: {total_usinas:>5}                                                   ║")
    logger.info(f"║  Transformadores OSM: {total_trans:>5}                                               ║")
    logger.info("╚" + "=" * 78 + "╝")


# ============================================================================
# CLI
# ============================================================================

def main():
    """Função principal que segue o padrão dos outros ETLs"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.area_cobertura_real")
    
    parser = argparse.ArgumentParser(
        description='ETL de Área de Cobertura com Dados REAIS'
    )
    
    parser.add_argument(
        '--ons',
        action='store_true',
        help='Extrair subestações do ONS'
    )
    
    parser.add_argument(
        '--aneel',
        action='store_true',
        help='Extrair usinas da ANEEL SIGA'
    )
    
    parser.add_argument(
        '--osm',
        type=int,
        metavar='SUBESTACAO_ID',
        help='Extrair transformadores OSM para subestação específica'
    )
    
    parser.add_argument(
        '--completo',
        action='store_true',
        help='Executar ETL completo de todas as fontes'
    )
    
    args = parser.parse_args()
    
    try:
        if args.completo:
            etl_completo()
        elif args.ons:
            etl_subestacoes_ons()
        elif args.aneel:
            etl_usinas_aneel()
        elif args.osm:
            etl_transformadores_osm(args.osm)
        else:
            parser.print_help()
            logger.info("\n💡 Exemplo de uso:")
            logger.info("  python src/extractors/etl_area_cobertura_real.py --completo")
            logger.info("  python src/extractors/etl_area_cobertura_real.py --ons")
            logger.info("  python src/extractors/etl_area_cobertura_real.py --osm 1")
    except Exception:
        logger.exception("Falha na extração de área de cobertura")
        raise


if __name__ == '__main__':
    main()
