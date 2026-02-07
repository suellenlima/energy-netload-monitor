"""
ANEEL BDGD Shared Service
========================

Serviço centralizado com lógicas comuns para ETL ANEEL BDGD.
Evita duplicação de código entre diferentes ETLs.

Classes:
- GeometryService: Operações geométricas (normalização, projeção)
- ClassificationService: Classificação de dados (tipo tensão, etc)
- TransformerService: Extração e inserção de transformadores
- SubstationService: Extração e inserção de subestações
- ConsumerService: Extração e inserção de consumidores (BT/MT/AT)
- DistributorService: Gestão de distribuidoras
- AreaService: Cálculo de áreas de cobertura (ConvexHull + Buffer)
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def truncate_string(value, max_length: int = 50) -> str:
    """Trunca string para o tamanho máximo permitido"""
    if value is None or pd.isna(value):
        return None
    str_val = str(value)
    if len(str_val) > max_length:
        return str_val[:max_length]
    return str_val


class GeometryService:
    """Operações geométricas e normalização"""
    
    @staticmethod
    def normalize_geometry(gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, Dict]:
        """
        Normaliza geometria do GeoDataFrame:
        - Reprojetar para WGS84 se necessário
        - Corrigir geometrias inválidas
        """
        stats = {'reprojected': False, 'invalid_fixed': 0}
        
        # Reprojetar se necessário
        if gdf.crs is None:
            gdf.set_crs('EPSG:4326', inplace=True)
            stats['reprojected'] = True
        elif gdf.crs.to_epsg() != 4326:
            try:
                gdf = gdf.to_crs(epsg=4326)
                stats['reprojected'] = True
            except Exception as e:
                logger.warning(f"Erro ao reprojetar: {e}")
        
        # Corrigir geometrias inválidas
        if not gdf.geometry.is_valid.all():
            invalid_count = (~gdf.geometry.is_valid).sum()
            gdf['geometry'] = gdf.geometry.buffer(0)
            stats['invalid_fixed'] = invalid_count
        
        return gdf, stats
    
    @staticmethod
    def clean_substation_name(nome: str, codigo_sub: str) -> str:
        """Remove prefixos redundantes e códigos duplicados de nomes"""
        if not nome or pd.isna(nome):
            return None
        
        nome = str(nome).strip()
        
        # Prefixos a remover
        prefixos_remover = [
            r'^ALIMENTOR\s+',
            r'^SUPRIMENTO\s+\d+\s+',
            r'^SUPRIM\.\s+',
            r'^ALIMENTADOR\s+',
        ]
        
        for prefixo in prefixos_remover:
            nome = re.sub(prefixo, '', nome, flags=re.IGNORECASE).strip()
        
        # Remover duplicação de código no final
        if codigo_sub:
            variacoes = [
                rf'\s+{re.escape(codigo_sub)}\s*$',
                rf'\s+{re.escape(codigo_sub.replace("12", " 12"))}\s*$',
            ]
            for padrao in variacoes:
                nome = re.sub(padrao, '', nome, flags=re.IGNORECASE).strip()
        
        return nome if nome else None


class ClassificationService:
    """Serviço de classificação de dados"""
    
    @staticmethod
    def classify_transformer_type(tensao_primaria: float, tensao_secundaria: float) -> Optional[str]:
        """
        Classifica transformador como BT, MT ou AT
        
        Convenção:
        - BT (Baixa Tensão): tensão_secundária < 1 kV
        - AT (Alta Tensão): tensão_primária > 35 kV ou tensão_secundária > 35 kV
        - MT (Média Tensão): demais casos (1-35 kV)
        """
        if pd.isna(tensao_secundaria) and pd.isna(tensao_primaria):
            return None
        
        # Verificar BT
        if not pd.isna(tensao_secundaria) and tensao_secundaria < 1:
            return 'BT'
        
        # Verificar AT
        if (not pd.isna(tensao_primaria) and tensao_primaria > 35) or \
           (not pd.isna(tensao_secundaria) and tensao_secundaria > 35):
            return 'AT'
        
        # Default MT
        return 'MT'


class TransformerService:
    """Serviço de transformadores"""
    
    def __init__(self, engine):
        self.engine = engine
    
    @staticmethod
    def extract(gdf: gpd.GeoDataFrame, distribuidora: str, layer_name: str = None) -> pd.DataFrame:
        """
        Extrai dados de transformadores
        
        Args:
            gdf: GeoDataFrame com dados
            distribuidora: Nome da distribuidora
            layer_name: Nome da camada (usado para inferir tipo quando tensões não existem)
        """
        logger.info(f"Extraindo transformadores...")
        logger.debug(f"  Colunas disponíveis: {list(gdf.columns)}")
        
        gdf, stats = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        
        # Procurar coluna de código
        cod_col = None
        for col in ['COD_ID', 'CODID', 'ID', 'codigo']:
            if col in gdf.columns:
                cod_col = col
                break
        
        if cod_col is None:
            logger.warning(f"  ⚠ Coluna de código não encontrada. Esperado: COD_ID/codigo")
            logger.warning(f"    Colunas disponíveis: {list(gdf.columns)}")
            return df
        
        df['codigo'] = gdf[cod_col].astype(str)
        df['nome'] = gdf.get('NOME', None) or gdf.get('NOM', None)
        df['distribuidora'] = distribuidora
        
        if 'SUB' in gdf.columns:
            df['subestacao_codigo'] = gdf['SUB'].astype(str)
        if 'POT_NOM' in gdf.columns:
            df['potencia_kva'] = pd.to_numeric(gdf['POT_NOM'], errors='coerce')
        if 'TEN_PRI' in gdf.columns:
            df['tensao_primaria_kv'] = pd.to_numeric(gdf['TEN_PRI'], errors='coerce')
        if 'TEN_SEC' in gdf.columns:
            df['tensao_secundaria_kv'] = pd.to_numeric(gdf['TEN_SEC'], errors='coerce')
        
        # Classificar tipo - IMPORTANTE: inferir da camada se tensões não disponíveis
        def classify_type(row, layer_nm):
            # Primeiro tenta por tensões
            tipo = ClassificationService.classify_transformer_type(
                row.get('tensao_primaria_kv'), row.get('tensao_secundaria_kv')
            )
            if tipo is not None:
                return tipo
            
            # Se não encontrou, infere pela camada (UNTRMT=MT, UNTRAT=AT, UNTRD=genérico)
            if layer_nm:
                layer_upper = str(layer_nm).upper()
                if 'UNTRMT' in layer_upper or 'TRANSFORMADOR_MT' in layer_upper:
                    return 'MT'
                elif 'UNTRAT' in layer_upper or 'TRANSFORMADOR_AT' in layer_upper:
                    return 'AT'
            
            return None
        
        df['tipo_tensao'] = df.apply(lambda row: classify_type(row, layer_name), axis=1)
        
        # Calcular coordenadas - suporta Point e Polygon
        def get_coords(geom):
            if geom.geom_type == 'Point':
                return geom.y, geom.x
            else:  # Polygon, LineString, etc
                centroid = geom.centroid
                return centroid.y, centroid.x
        
        coords = gdf.geometry.apply(get_coords)
        df['latitude'] = coords.apply(lambda x: x[0])
        df['longitude'] = coords.apply(lambda x: x[1])
        df['data_criacao'] = datetime.now()
        df['data_atualizacao'] = datetime.now()
        
        # Remover duplicatas
        if 'codigo' in df.columns:
            df = df.drop_duplicates(subset=['codigo'], keep='first')
        
        bt_count = (df['tipo_tensao'] == 'BT').sum()
        mt_count = (df['tipo_tensao'] == 'MT').sum()
        at_count = (df['tipo_tensao'] == 'AT').sum()
        logger.info(f"✓ {len(df)} transformadores extraídos (BT: {bt_count}, MT: {mt_count}, AT: {at_count})")
        
        return df
    
    def insert(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere ou atualiza transformadores no banco usando UPSERT
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        """
        if df.empty:
            logger.warning(f"Nenhum transformador para carregar")
            return 0
        
        inserted = 0
        try:
            from sqlalchemy import text
            
            # Preparar dados para UPSERT
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    stmt = text("""
                        INSERT INTO transformadores_aneel 
                            (codigo, nome, distribuidora, subestacao_codigo, potencia_kva, 
                             tensao_primaria_kv, tensao_secundaria_kv, tipo_tensao, 
                             latitude, longitude, data_criacao, data_atualizacao)
                        VALUES 
                            (:cod, :nome, :dist, :sub, :pot, :tp, :ts, :tipo, :lat, :lon, :cr, :au)
                        ON CONFLICT (codigo) DO UPDATE SET
                            nome = EXCLUDED.nome,
                            distribuidora = EXCLUDED.distribuidora,
                            subestacao_codigo = EXCLUDED.subestacao_codigo,
                            potencia_kva = EXCLUDED.potencia_kva,
                            tensao_primaria_kv = EXCLUDED.tensao_primaria_kv,
                            tensao_secundaria_kv = EXCLUDED.tensao_secundaria_kv,
                            tipo_tensao = EXCLUDED.tipo_tensao,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            data_atualizacao = EXCLUDED.data_atualizacao
                    """)
                    
                    conn.execute(stmt, {
                        'cod': str(row.get('codigo')),
                        'nome': row.get('nome'),
                        'dist': str(row.get('distribuidora')),
                        'sub': row.get('subestacao_codigo'),
                        'pot': float(row.get('potencia_kva')) if pd.notna(row.get('potencia_kva')) else None,
                        'tp': float(row.get('tensao_primaria_kv')) if pd.notna(row.get('tensao_primaria_kv')) else None,
                        'ts': float(row.get('tensao_secundaria_kv')) if pd.notna(row.get('tensao_secundaria_kv')) else None,
                        'tipo': row.get('tipo_tensao'),
                        'lat': float(row.get('latitude')) if pd.notna(row.get('latitude')) else None,
                        'lon': float(row.get('longitude')) if pd.notna(row.get('longitude')) else None,
                        'cr': row.get('data_criacao'),
                        'au': row.get('data_atualizacao')
                    })
                    inserted += 1
                
                conn.commit()
            
            logger.info(f"✓ {inserted} transformadores carregados/atualizados")
        
        except Exception as e:
            logger.error(f"Erro ao inserir transformadores: {e}")
            raise
        
        return inserted


class SubstationService:
    """Serviço de subestações"""
    
    def __init__(self, engine):
        self.engine = engine
    
    @staticmethod
    def extract(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai dados de subestações"""
        logger.info(f"Extraindo subestações...")
        logger.debug(f"  Colunas disponíveis: {list(gdf.columns)}")
        
        gdf, stats = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        
        # Tentar diferentes padrões de coluna para COD_ID e NOM
        cod_col = None
        nom_col = None
        
        # Procurar coluna de código
        for col in ['COD_ID', 'CODID', 'ID', 'codigo']:
            if col in gdf.columns:
                cod_col = col
                break
        
        # Procurar coluna de nome
        for col in ['NOM', 'NOME', 'nome', 'NAME']:
            if col in gdf.columns:
                nom_col = col
                break
        
        if cod_col is None or nom_col is None:
            logger.warning(f"  ⚠ Colunas não encontradas. Esperado: COD_ID/codigo e NOM/nome")
            logger.warning(f"    Colunas disponíveis: {list(gdf.columns)}")
            return df
        
        grouped = gdf.groupby(cod_col, as_index=False).first()
        
        df['codigo'] = grouped[cod_col].astype(str)
        df['nome'] = grouped[nom_col].astype(str)
        df['nome'] = df.apply(
            lambda row: GeometryService.clean_substation_name(row['nome'], row['codigo']),
            axis=1
        )
        
        if 'CLAS_TEN' in grouped.columns:
            df['tensao_kv'] = grouped['CLAS_TEN']
        
        # Calcular coordenadas - suporta Point e Polygon
        def get_coords(geom):
            if geom.geom_type == 'Point':
                return geom.y, geom.x
            else:  # Polygon, LineString, etc
                centroid = geom.centroid
                return centroid.y, centroid.x
        
        coords = grouped.geometry.apply(get_coords)
        df['latitude'] = coords.apply(lambda x: x[0])
        df['longitude'] = coords.apply(lambda x: x[1])
        df['distribuidora'] = distribuidora
        df['data_criacao'] = datetime.now()
        df['data_atualizacao'] = datetime.now()
        
        logger.info(f"✓ {len(df)} subestações extraídas")
        
        return df
    
    def insert(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere ou atualiza subestações no banco usando UPSERT
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        """
        if df.empty:
            logger.warning(f"Nenhuma subestação para carregar")
            return 0
        
        inserted = 0
        try:
            from sqlalchemy import text
            
            # Preparar dados para UPSERT
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    stmt = text("""
                        INSERT INTO subestacoes_aneel 
                            (codigo, nome, latitude, longitude, distribuidora, tensao_kv, data_criacao, data_atualizacao)
                        VALUES 
                            (:codigo, :nome, :lat, :lon, :dist, :tensao, :data_cri, :data_atu)
                        ON CONFLICT (codigo) DO UPDATE SET
                            nome = EXCLUDED.nome,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            distribuidora = EXCLUDED.distribuidora,
                            tensao_kv = EXCLUDED.tensao_kv,
                            data_atualizacao = EXCLUDED.data_atualizacao
                    """)
                    
                    conn.execute(stmt, {
                        'codigo': str(row.get('codigo')),
                        'nome': row.get('nome'),
                        'lat': float(row.get('latitude')) if pd.notna(row.get('latitude')) else None,
                        'lon': float(row.get('longitude')) if pd.notna(row.get('longitude')) else None,
                        'dist': str(row.get('distribuidora')),
                        'tensao': row.get('tensao_kv'),
                        'data_cri': row.get('data_criacao'),
                        'data_atu': row.get('data_atualizacao')
                    })
                    inserted += 1
                
                conn.commit()
            
            logger.info(f"✓ {inserted} subestações carregadas/atualizadas")
        
        except Exception as e:
            logger.error(f"Erro ao inserir subestações: {e}")
            raise
        
        return inserted


class ConsumerService:
    """Serviço de consumidores (BT/MT/AT)"""
    
    def __init__(self, engine):
        self.engine = engine
    
    @staticmethod
    def extract_bt(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai consumidores BT"""
        logger.info(f"Extraindo consumidores BT... ({len(gdf)} registros brutos)")
        if gdf.empty:
            return pd.DataFrame()
        
        logger.info(f"  Normalizando geometrias...")
        gdf, _ = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns:
            df['codigo'] = gdf['COD_ID'].astype(str).apply(lambda x: truncate_string(x, 255))
            df['distribuidora'] = truncate_string(distribuidora, 50)
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CAR_INST' in gdf.columns:
                df['carga_instalada_kw'] = pd.to_numeric(gdf['CAR_INST'], errors='coerce')
            
            df['latitude'] = gdf.geometry.y
            df['longitude'] = gdf.geometry.x
            df['data_criacao'] = datetime.now()
            df['data_atualizacao'] = datetime.now()
            
            df = df.drop_duplicates(subset=['codigo'], keep='first')
            logger.debug(f"✓ {len(df)} consumidores BT extraídos")
        
        return df
    
    @staticmethod
    def extract_mt(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai consumidores MT"""
        logger.debug(f"Extraindo consumidores MT...")
        if gdf.empty:
            return pd.DataFrame()
        
        gdf, _ = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns:
            df['codigo'] = gdf['COD_ID'].astype(str).apply(lambda x: truncate_string(x, 50))
            df['distribuidora'] = truncate_string(distribuidora, 50)
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CTMT' in gdf.columns:
                df['circuito_mt_codigo'] = gdf['CTMT'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CAR_INST' in gdf.columns:
                df['carga_instalada_kw'] = pd.to_numeric(gdf['CAR_INST'], errors='coerce')
            if 'DEM_CONT' in gdf.columns:
                df['demanda_contratada_kw'] = pd.to_numeric(gdf['DEM_CONT'], errors='coerce')
            
            df['latitude'] = gdf.geometry.y
            df['longitude'] = gdf.geometry.x
            df['data_criacao'] = datetime.now()
            df['data_atualizacao'] = datetime.now()
            
            df = df.drop_duplicates(subset=['codigo'], keep='first')
            logger.debug(f"✓ {len(df)} consumidores MT extraídos")
        
        return df
    
    @staticmethod
    def extract_at(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai consumidores AT"""
        logger.debug(f"Extraindo consumidores AT...")
        if gdf.empty:
            return pd.DataFrame()
        
        gdf, _ = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns:
            df['codigo'] = gdf['COD_ID'].astype(str).apply(lambda x: truncate_string(x, 50))
            df['distribuidora'] = truncate_string(distribuidora, 50)
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CTAT' in gdf.columns:
                df['circuito_at_codigo'] = gdf['CTAT'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str).apply(lambda x: truncate_string(x, 50))
            if 'CAR_INST' in gdf.columns:
                df['carga_instalada_kw'] = pd.to_numeric(gdf['CAR_INST'], errors='coerce')
            if 'DEM_CONT' in gdf.columns:
                df['demanda_contratada_kw'] = pd.to_numeric(gdf['DEM_CONT'], errors='coerce')
            
            df['latitude'] = gdf.geometry.y
            df['longitude'] = gdf.geometry.x
            df['data_criacao'] = datetime.now()
            df['data_atualizacao'] = datetime.now()
            
            df = df.drop_duplicates(subset=['codigo'], keep='first')
            logger.debug(f"✓ {len(df)} consumidores AT extraídos")
        
        return df
    
    def insert_bt(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere ou atualiza consumidores BT usando BATCH INSERT (OTIMIZADO)
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método usa pandas to_sql() com batch para performance
        """
        if df.empty:
            return 0
        
        try:
            logger.info(f"  Inserindo {len(df)} consumidores BT em batch...")
            
            # Usar to_sql com chunksize para inserir em lotes
            df.to_sql(
                'consumidores_bt_aneel', 
                self.engine, 
                if_exists='append', 
                index=False,
                chunksize=1000,  # Inserir em lotes de 1000
                method='multi'  # Usar multi-row INSERT
            )
            
            logger.info(f"✓ {len(df)} consumidores BT carregados")
            return len(df)
        
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores BT: {e}")
            # Se erro por duplicatas, tentar row-by-row com UPSERT
            if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                logger.warning(f"  Detectadas duplicatas, usando UPSERT...")
                return self._insert_bt_upsert(df)
            raise
    
    def _insert_bt_upsert(self, df: pd.DataFrame) -> int:
        """Fallback: UPSERT linha por linha (mais lento, mas lida com duplicatas)"""
        from sqlalchemy import text
        inserted = 0
        
        with self.engine.connect() as conn:
            for idx, row in df.iterrows():
                if idx % 1000 == 0:
                    logger.info(f"    Progresso UPSERT: {idx}/{len(df)} registros")
                
                stmt = text("""
                    INSERT INTO consumidores_bt_aneel 
                        (codigo, distribuidora, dist_codigo, subestacao_codigo, 
                         classe_subclasse_codigo, tensao_fornecimento_codigo, 
                         carga_instalada_kw, latitude, longitude, 
                         data_criacao, data_atualizacao)
                    VALUES 
                        (:codigo, :dist, :dist_cod, :sub_cod, :classe, :tensao, 
                         :carga, :lat, :lon, :cr, :au)
                    ON CONFLICT (codigo) DO UPDATE SET
                        distribuidora = EXCLUDED.distribuidora,
                        dist_codigo = EXCLUDED.dist_codigo,
                        subestacao_codigo = EXCLUDED.subestacao_codigo,
                        classe_subclasse_codigo = EXCLUDED.classe_subclasse_codigo,
                        tensao_fornecimento_codigo = EXCLUDED.tensao_fornecimento_codigo,
                        carga_instalada_kw = EXCLUDED.carga_instalada_kw,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        data_atualizacao = EXCLUDED.data_atualizacao
                """)
                
                conn.execute(stmt, {
                    'codigo': str(row.get('codigo')),
                    'dist': str(row.get('distribuidora')),
                    'dist_cod': row.get('dist_codigo'),
                    'sub_cod': row.get('subestacao_codigo'),
                    'classe': row.get('classe_subclasse_codigo'),
                    'tensao': row.get('tensao_fornecimento_codigo'),
                    'carga': float(row.get('carga_instalada_kw')) if pd.notna(row.get('carga_instalada_kw')) else None,
                    'lat': float(row.get('latitude')) if pd.notna(row.get('latitude')) else None,
                    'lon': float(row.get('longitude')) if pd.notna(row.get('longitude')) else None,
                    'cr': row.get('data_criacao'),
                    'au': row.get('data_atualizacao')
                })
                inserted += 1
            
            conn.commit()
        
        logger.info(f"✓ {inserted} consumidores BT carregados via UPSERT")
        return inserted
        
        return inserted
    
    def insert_mt(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere consumidores MT usando BATCH INSERT (OTIMIZADO)
        """
        if df.empty:
            return 0
        
        try:
            logger.info(f"  Inserindo {len(df)} consumidores MT em batch...")
            df.to_sql('consumidores_mt_aneel', self.engine, if_exists='append', index=False, chunksize=1000, method='multi')
            logger.info(f"✓ {len(df)} consumidores MT carregados")
            return len(df)
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores MT: {e}")
            raise
    
    def insert_at(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere consumidores AT usando BATCH INSERT (OTIMIZADO)
        """
        if df.empty:
            return 0
        
        try:
            logger.info(f"  Inserindo {len(df)} consumidores AT em batch...")
            df.to_sql('consumidores_at_aneel', self.engine, if_exists='append', index=False, chunksize=1000, method='multi')
            logger.info(f"✓ {len(df)} consumidores AT carregados")
            return len(df)
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores AT: {e}")
            raise


class DistributorService:
    """Serviço de distribuidoras"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def upsert(self, dist_nome: str, total_trafo: int = 0, total_sub: int = 0, 
               total_consumidores: int = 0, potencia_total_kva: float = None, 
               dist_arquivo: str = None) -> bool:
        """
        Insere ou atualiza uma distribuidora na tabela distribuidoras_aneel
        
        Args:
            dist_nome: Nome da distribuidora (ex: "IENERGIA_87_2021-02-28_M10_20210902-1755")
            total_trafo: Total de transformadores
            total_sub: Total de subestações
            total_consumidores: Total de consumidores
            potencia_total_kva: Potência total em kVA
            dist_arquivo: Nome do arquivo original
            
        Returns:
            True se inserido/atualizado, False se erro
        """
        try:
            with self.engine.begin() as conn:
                # Extrair estado e região do nome da distribuidora (se possível)
                estado, regiao = self._extrair_estado_regiao(dist_nome)
                
                codigo_arquivo = dist_arquivo or dist_nome
                
                # Primeiro tentar UPDATE
                update_query = text("""
                    UPDATE distribuidoras_aneel
                    SET total_transformadores = :total_trafo,
                        total_subestacoes = :total_sub,
                        total_consumidores = :total_consumidores,
                        potencia_total_kva = :potencia_total_kva,
                        data_carregamento = NOW()
                    WHERE nome = :nome
                """)
                
                result = conn.execute(update_query, {
                    'nome': dist_nome,
                    'total_trafo': total_trafo,
                    'total_sub': total_sub,
                    'total_consumidores': total_consumidores,
                    'potencia_total_kva': potencia_total_kva,
                })
                
                # Se nenhum registro foi atualizado, fazer INSERT
                if result.rowcount == 0:
                    insert_query = text("""
                        INSERT INTO distribuidoras_aneel 
                        (nome, codigo_arquivo, estado, regiao, data_carregamento, 
                         total_transformadores, total_subestacoes, total_consumidores, 
                         potencia_total_kva, ativo, observacoes)
                        VALUES (:nome, :codigo_arquivo, :estado, :regiao, NOW(),
                                :total_trafo, :total_sub, :total_consumidores,
                                :potencia_total_kva, TRUE, :observacoes)
                    """)
                    
                    conn.execute(insert_query, {
                        'nome': dist_nome,
                        'codigo_arquivo': codigo_arquivo,
                        'estado': estado,
                        'regiao': regiao,
                        'total_trafo': total_trafo,
                        'total_sub': total_sub,
                        'total_consumidores': total_consumidores,
                        'potencia_total_kva': potencia_total_kva,
                        'observacoes': f'Inserido em {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                    })
                    logger.info(f"✓ Distribuidora '{dist_nome}' INSERIDA com sucesso "
                               f"(Trafo: {total_trafo}, Sub: {total_sub}, Consumidores: {total_consumidores})")
                else:
                    logger.info(f"✓ Distribuidora '{dist_nome}' ATUALIZADA com sucesso "
                               f"(Trafo: {total_trafo}, Sub: {total_sub}, Consumidores: {total_consumidores})")
                
                return True
        
        except Exception as e:
            logger.error(f"❌ Erro ao popular distribuidora '{dist_nome}': {e}")
            return False
    
    def update(self, dist_real: str, total_trafo: int, total_sub: int, dist_arquivo: str):
        """
        Atualiza tabela de distribuidoras (compatibilidade com código legado)
        
        Args:
            dist_real: Nome real da distribuidora
            total_trafo: Total de transformadores carregados
            total_sub: Total de subestações carregadas
            dist_arquivo: Nome do arquivo/pasta original
        """
        self.upsert(
            dist_nome=dist_real,
            total_trafo=total_trafo,
            total_sub=total_sub,
            dist_arquivo=dist_arquivo
        )
    
    def _extrair_estado_regiao(self, dist_nome: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extrai estado e região do nome da distribuidora
        
        Exemplos de padrões:
        - "IENERGIA_87_2021-02-28_M10_20210902-1755"
        - "COPEL"
        - "CELESC"
        
        Returns:
            (estado, regiao) - Ambos None se não conseguir identificar
        """
        # Dicionário de distribuidoras conhecidas -> (estado, região)
        distribuidoras_mapa = {
            'copel': ('PR', 'Sul'),
            'celesc': ('SC', 'Sul'),
            'ienergia': ('SC', 'Sul'),  # IENERGIA opera em SC
            'eletrobras': ('Nacional', 'Sudeste'),
            'eletrosul': ('SC', 'Sul'),
            'ceee': ('RS', 'Sul'),
            'cemig': ('MG', 'Sudeste'),
            'eletropaulo': ('SP', 'Sudeste'),
            'enel': ('SP', 'Sudeste'),
            'enersul': ('MS', 'Centro-Oeste'),
            'equatorial': ('Nacional', 'Norte'),
            'light': ('RJ', 'Sudeste'),
            'neoenergia': ('Nacional', 'Nordeste'),
            'cpfl': ('SP', 'Sudeste'),
        }
        
        # Extrair primeira parte do nome antes de underscores ou números
        nome_lower = dist_nome.lower().split('_')[0]
        
        # Procurar no mapa
        for chave, (estado, regiao) in distribuidoras_mapa.items():
            if chave in nome_lower:
                return estado, regiao
        
        # Se não encontrar, retornar None
        return None, None
    
    def listar_todas(self) -> pd.DataFrame:
        """Lista todas as distribuidoras cadastradas"""
        try:
            query = "SELECT * FROM distribuidoras_aneel ORDER BY nome"
            df = pd.read_sql(query, self.engine)
            logger.info(f"✓ {len(df)} distribuidoras listadas")
            return df
        except Exception as e:
            logger.error(f"❌ Erro ao listar distribuidoras: {e}")
            return pd.DataFrame()
    
    def obter_stats(self, dist_nome: str = None) -> pd.DataFrame:
        """Obtém estatísticas de distribuidoras"""
        try:
            if dist_nome:
                query = text("""
                    SELECT nome, total_transformadores, total_subestacoes, 
                           total_consumidores, potencia_total_kva, data_carregamento
                    FROM distribuidoras_aneel
                    WHERE nome ILIKE :nome
                    ORDER BY data_carregamento DESC
                """)
                df = pd.read_sql(query, self.engine, params={'nome': f"%{dist_nome}%"})
            else:
                query = """
                    SELECT nome, total_transformadores, total_subestacoes,
                           total_consumidores, potencia_total_kva, data_carregamento
                    FROM distribuidoras_aneel
                    ORDER BY nome
                """
                df = pd.read_sql(query, self.engine)
            
            logger.info(f"✓ Estatísticas obtidas para {len(df)} distribuidora(s)")
            return df
        except Exception as e:
            logger.error(f"❌ Erro ao obter estatísticas: {e}")
            return pd.DataFrame()


class AreaService:
    """Serviço de cálculo de áreas de cobertura"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def calculate(self, tipo_tensao: str, distribuidora: str) -> Tuple[int, int]:
        """
        Calcula áreas poligonais dos transformadores via stored procedure
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Usa a stored procedure sp_calcular_area_transformadores() que implementa:
        - ≥3 consumidores → ConvexHull
        - <3 consumidores → Buffer (raio adaptado: BT=500m, MT=1km, AT=2km)
        """
        try:
            with self.engine.begin() as conn:
                # Chamar stored procedure que faz TODO o cálculo no banco
                result = conn.execute(text("""
                    SELECT * FROM sp_calcular_area_transformadores(:tipo_tensao, :distribuidora, FALSE)
                """), {
                    'tipo_tensao': tipo_tensao,
                    'distribuidora': distribuidora
                })
                
                row = result.fetchone()
                if row:
                    calculadas = row[3] if len(row) > 3 else 0  # areas_criadas
                    logger.info(f"✓ {calculadas} áreas {tipo_tensao} calculadas via sp_calcular_area_transformadores()")
                    return calculadas, 0
                else:
                    logger.warning(f"Nenhuma área {tipo_tensao} calculada")
                    return 0, 0
        
        except Exception as e:
            logger.warning(f"⚠ sp_calcular_area_transformadores() não disponível: {e}")
            logger.warning(f"  Execute primeiro: docker-compose up db (schema carregado automaticamente)")
            return 0, 0

