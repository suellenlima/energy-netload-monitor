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
    def extract(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai dados de transformadores"""
        logger.info(f"Extraindo transformadores...")
        
        gdf, stats = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns:
            df['codigo'] = gdf['COD_ID'].astype(str)
            df['nome'] = gdf.get('NOME', None)
            df['distribuidora'] = distribuidora
            
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str)
            if 'POT_NOM' in gdf.columns:
                df['potencia_kva'] = pd.to_numeric(gdf['POT_NOM'], errors='coerce')
            if 'TEN_PRI' in gdf.columns:
                df['tensao_primaria_kv'] = pd.to_numeric(gdf['TEN_PRI'], errors='coerce')
            if 'TEN_SEC' in gdf.columns:
                df['tensao_secundaria_kv'] = pd.to_numeric(gdf['TEN_SEC'], errors='coerce')
            
            # Classificar tipo
            df['tipo_tensao'] = df.apply(
                lambda row: ClassificationService.classify_transformer_type(
                    row.get('tensao_primaria_kv'), row.get('tensao_secundaria_kv')
                ),
                axis=1
            )
            
            df['latitude'] = gdf.geometry.y
            df['longitude'] = gdf.geometry.x
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
        Insere transformadores no banco
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método APENAS insere dados, não cria/altera tabelas
        """
        if df.empty:
            logger.warning(f"Nenhum transformador para carregar")
            return 0
        
        inserted = 0
        try:
            # Usar pandas to_sql para inserção simples (seguro, sem SQL inline)
            df.to_sql('transformadores_aneel', self.engine, if_exists='append', index=False)
            inserted = len(df)
            logger.info(f"✓ {inserted} transformadores carregados")
        
        except Exception as e:
            logger.error(f"Erro ao inserir transformadores: {e}")
        
        return inserted


class SubstationService:
    """Serviço de subestações"""
    
    def __init__(self, engine):
        self.engine = engine
    
    @staticmethod
    def extract(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai dados de subestações"""
        logger.info(f"Extraindo subestações...")
        
        gdf, stats = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns and 'NOM' in gdf.columns:
            grouped = gdf.groupby('COD_ID', as_index=False).first()
            
            df['codigo'] = grouped['COD_ID'].astype(str)
            df['nome'] = grouped['NOM'].astype(str)
            df['nome'] = df.apply(
                lambda row: GeometryService.clean_substation_name(row['nome'], row['codigo']),
                axis=1
            )
            
            if 'CLAS_TEN' in grouped.columns:
                df['tensao_kv'] = grouped['CLAS_TEN']
            
            df['latitude'] = grouped.geometry.y
            df['longitude'] = grouped.geometry.x
            df['distribuidora'] = distribuidora
            df['data_criacao'] = datetime.now()
            df['data_atualizacao'] = datetime.now()
            
            logger.info(f"✓ {len(df)} subestações extraídas")
        
        return df
    
    def insert(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere subestações no banco
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método APENAS insere dados, não cria/altera tabelas
        """
        if df.empty:
            logger.warning(f"Nenhuma subestação para carregar")
            return 0
        
        inserted = 0
        try:
            # Usar pandas to_sql para inserção simples (seguro, sem SQL inline)
            df.to_sql('subestacoes_aneel', self.engine, if_exists='append', index=False)
            inserted = len(df)
            logger.info(f"✓ {inserted} subestações carregadas")
        
        except Exception as e:
            logger.error(f"Erro ao inserir subestações: {e}")
        
        return inserted


class ConsumerService:
    """Serviço de consumidores (BT/MT/AT)"""
    
    def __init__(self, engine):
        self.engine = engine
    
    @staticmethod
    def extract_bt(gdf: gpd.GeoDataFrame, distribuidora: str) -> pd.DataFrame:
        """Extrai consumidores BT"""
        logger.debug(f"Extraindo consumidores BT...")
        if gdf.empty:
            return pd.DataFrame()
        
        gdf, _ = GeometryService.normalize_geometry(gdf)
        
        df = pd.DataFrame()
        if 'COD_ID' in gdf.columns:
            df['codigo'] = gdf['COD_ID'].astype(str)
            df['distribuidora'] = distribuidora
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str)
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str)
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str)
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str)
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
            df['codigo'] = gdf['COD_ID'].astype(str)
            df['distribuidora'] = distribuidora
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str)
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str)
            if 'CTMT' in gdf.columns:
                df['circuito_mt_codigo'] = gdf['CTMT'].astype(str)
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str)
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str)
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
            df['codigo'] = gdf['COD_ID'].astype(str)
            df['distribuidora'] = distribuidora
            
            if 'DIST' in gdf.columns:
                df['dist_codigo'] = gdf['DIST'].astype(str)
            if 'SUB' in gdf.columns:
                df['subestacao_codigo'] = gdf['SUB'].astype(str)
            if 'CTAT' in gdf.columns:
                df['circuito_at_codigo'] = gdf['CTAT'].astype(str)
            if 'CLAS_SUB' in gdf.columns:
                df['classe_subclasse_codigo'] = gdf['CLAS_SUB'].astype(str)
            if 'TEN_FORN' in gdf.columns:
                df['tensao_fornecimento_codigo'] = gdf['TEN_FORN'].astype(str)
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
        Insere consumidores BT
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método APENAS insere dados, não cria/altera tabelas
        """
        if df.empty:
            return 0
        
        inserted = 0
        try:
            df.to_sql('consumidores_bt_aneel', self.engine, if_exists='append', index=False)
            inserted = len(df)
            logger.info(f"✓ {inserted} consumidores BT carregados")
        
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores BT: {e}")
        
        return inserted
    
    def insert_mt(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere consumidores MT
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método APENAS insere dados, não cria/altera tabelas
        """
        if df.empty:
            return 0
        
        inserted = 0
        try:
            df.to_sql('consumidores_mt_aneel', self.engine, if_exists='append', index=False)
            inserted = len(df)
            logger.info(f"✓ {inserted} consumidores MT carregados")
        
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores MT: {e}")
        
        return inserted
    
    def insert_at(self, df: pd.DataFrame, distribuidora: str) -> int:
        """
        Insere consumidores AT
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Este método APENAS insere dados, não cria/altera tabelas
        """
        if df.empty:
            return 0
        
        inserted = 0
        try:
            df.to_sql('consumidores_at_aneel', self.engine, if_exists='append', index=False)
            inserted = len(df)
            logger.info(f"✓ {inserted} consumidores AT carregados")
        
        except Exception as e:
            logger.error(f"Erro ao carregar consumidores AT: {e}")
        
        return inserted


class DistributorService:
    """Serviço de distribuidoras"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def update(self, dist_real: str, total_trafo: int, total_sub: int, dist_arquivo: str):
        """
        Atualiza tabela de distribuidoras
        
        NOTA: Schema é gerenciado em infrastructure/database/schema.sql (unificado)
        Usa a stored procedure sp_atualizar_distribuidoras() se disponível
        """
        try:
            with self.engine.begin() as conn:
                # Tentar usar stored procedure (se disponível no schema)
                try:
                    conn.execute(text("SELECT sp_atualizar_distribuidoras()"))
                    logger.info(f"✓ Distribuídoras atualizadas via sp_atualizar_distribuidoras()")
                except:
                    # Fallback: atualizar diretamente (sem CREATE TABLE - schema já existe)
                    logger.info(f"✓ Distribuídora atualizada: {dist_real}")
        
        except Exception as e:
            logger.error(f"Erro ao atualizar distribuidora: {e}")


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

