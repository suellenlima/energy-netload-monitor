"""
ETL para extrair/estimar consumo granular por classe de consumo.

Dados de consumo granular agregados por:
- Distribuidora
- Classe de consumo (Residencial, Comercial, Industrial)

Fonte: Proporcionalidade baseada em BDGD + Padrões históricos
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, load_settings

logger = logging.getLogger(__name__)

# Distribuição típica de consumo por classe (percentual da carga total)
DISTRIBUICAO_CONSUMO_POR_CLASSE = {
    "RESIDENCIAL": 0.45,      # 45% consumo
    "COMERCIAL": 0.35,        # 35% consumo
    "INDUSTRIAL": 0.15,       # 15% consumo
    "RURAL": 0.03,            # 3% consumo
    "ILUMINACAO_PUBLICA": 0.015,
    "SERVICO_PUBLICO": 0.015,
}

# Distribuição de carga líquida por distribuidora
# (baseada em número de transformadores e padrão de consumo)
DISTRIBUICAO_CARGA_DISTRIBUIDORA = {
    "LIGHT": 0.35,        # 35% (177.82 MW / 449 MW total)
    "ENEL": 0.59,         # 59% (265.55 MW / 449 MW total)
    "IENERGIA": 0.06,     # 6% (6.63 MW / 449 MW total)
}


def criar_tabela_consumo_granular(engine) -> None:
    """Cria tabela de consumo granular se não existir."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS consumo_granular_classe (
                    id SERIAL PRIMARY KEY,
                    distribuidora VARCHAR(100) NOT NULL,
                    classe_consumo VARCHAR(50) NOT NULL,
                    consumo_kwh FLOAT NOT NULL,
                    data_medicao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(distribuidora, classe_consumo, data_medicao)
                )
            """))
            conn.commit()
            logger.info("✅ Tabela consumo_granular_classe verificada/criada")
    except Exception as e:
        logger.warning(f"⚠️ Erro ao criar tabela: {e}")


def get_engine():
    """Helper para obter engine com URL do banco."""
    settings = load_settings()
    return create_db_engine(settings.database.url)


def estimar_consumo_granular_classe() -> pd.DataFrame:
    """
    Extrai consumo REAL granular por classe de consumo de gd_granular.
    
    Estratégia:
    1. Agregar potência instalada (kW) de gd_granular por distribuidora + classe
    2. Converter para consumo diário: potencia_kw * 24h / 1000 = MWh = consumo_kwh
    3. Mapear classe_consumo (residencia → RESIDENCIAL, comercio → COMERCIAL, etc)
    
    Returns:
        DataFrame com [distribuidora, classe_consumo, consumo_kwh, data_medicao]
    """
    try:
        engine = get_engine()
        
        # Mapeamento de tipo_estabelecimento para classe_consumo padrão
        tipo_para_classe = {
            'residencia': 'RESIDENCIAL',
            'predio_residencial': 'RESIDENCIAL',
            'comercio': 'COMERCIAL',
            'predio_comercial': 'COMERCIAL',
            'industria': 'INDUSTRIAL',
            'outro': 'OUTRO',
        }
        
        with engine.connect() as conn:
            # Agregar potência REAL por distribuidora e tipo de estabelecimento
            resultado = conn.execute(text("""
                SELECT 
                    UPPER(TRIM(distribuidora)) as distribuidora,
                    tipo_estabelecimento,
                    SUM(potencia_kw) as potencia_total_kw,
                    COUNT(*) as qtd_estabelecimentos
                FROM gd_granular
                GROUP BY UPPER(TRIM(distribuidora)), tipo_estabelecimento
                ORDER BY distribuidora, tipo_estabelecimento
            """))
            
            linhas = resultado.fetchall()
        
        dados = []
        timestamp_agora = datetime.now()
        
        for distribuidora, tipo_est, potencia_kw, qtd in linhas:
            # Converter potência para consumo diário
            # potencia_kw * 24h / 1000 = consumo_mwh
            # consumo_mwh * 1000 = consumo_kwh
            consumo_kwh = (potencia_kw * 24)
            
            # Mapear para classe padrão
            classe = tipo_para_classe.get(tipo_est, 'OUTRO')
            
            dados.append({
                'distribuidora': distribuidora,
                'classe_consumo': classe,
                'consumo_kwh': consumo_kwh,
                'data_medicao': timestamp_agora,
            })
            
            logger.info(
                f"✅ {distribuidora:12} | {classe:20} | {qtd:6} est. | "
                f"{potencia_kw:12,.0f} kW → {consumo_kwh:12,.0f} kWh/dia"
            )
        
        # Agregar por classe (consolidar múltiplos estabelecimentos da mesma classe)
        if dados:
            df = pd.DataFrame(dados)
            df_consolidado = df.groupby(['distribuidora', 'classe_consumo', 'data_medicao'], as_index=False).agg({
                'consumo_kwh': 'sum'
            })
            logger.info(f"📈 Extraídos {len(df_consolidado)} registros REAIS de consumo granular por classe")
            return df_consolidado
        else:
            logger.warning("⚠️ Nenhum dado encontrado em gd_granular")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair consumo granular: {e}", exc_info=True)
        return pd.DataFrame()


def carregar_consumo_granular_classe(df: pd.DataFrame, engine) -> int:
    """
    Carrega dados de consumo granular na tabela.
    Remove dados antigos e insere novos.
    
    Args:
        df: DataFrame com dados de consumo
        engine: SQLAlchemy engine
        
    Returns:
        Número de linhas inseridas
    """
    if df.empty:
        logger.info("⚠️ Nenhum dado para carregar")
        return 0
    
    try:
        # Limpar dados antigos (exceto últimas 48h)
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM consumo_granular_classe
                WHERE data_medicao < NOW() - INTERVAL '48 hours'
            """))
        
        # Inserir novos dados em batch menor para evitar transaction abort
        total_linhas = 0
        batch_size = 50
        batch = []
        
        for _, row in df.iterrows():
            batch.append({
                'dist': row['distribuidora'],
                'classe': row['classe_consumo'],
                'consumo': row['consumo_kwh'],
                'data': row['data_medicao'],
            })
            
            if len(batch) >= batch_size:
                # Inserir batch com sua própria transação
                try:
                    with engine.begin() as conn:
                        for params in batch:
                            conn.execute(text("""
                                INSERT INTO consumo_granular_classe 
                                (distribuidora, classe_consumo, consumo_kwh, data_medicao)
                                VALUES (:dist, :classe, :consumo, :data)
                                ON CONFLICT (distribuidora, classe_consumo, data_medicao) 
                                DO UPDATE SET consumo_kwh = :consumo
                            """), params)
                    total_linhas += len(batch)
                    logger.info(f"✓ Inseridas {len(batch)} linhas")
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao inserir batch: {e}")
                batch = []
        
        # Inserir batch final
        if batch:
            try:
                with engine.begin() as conn:
                    for params in batch:
                        conn.execute(text("""
                            INSERT INTO consumo_granular_classe 
                            (distribuidora, classe_consumo, consumo_kwh, data_medicao)
                            VALUES (:dist, :classe, :consumo, :data)
                            ON CONFLICT (distribuidora, classe_consumo, data_medicao) 
                            DO UPDATE SET consumo_kwh = :consumo
                        """), params)
                total_linhas += len(batch)
                logger.info(f"✓ Inseridas {len(batch)} linhas (batch final)")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao inserir batch final: {e}")
        
        logger.info(f"✅ Carregadas {total_linhas} linhas em consumo_granular_classe")
        return total_linhas
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados: {e}")
        return 0


def executar_etl_consumo_granular():
    """Executa ETL completo de consumo granular."""
    
    logger.info("=" * 80)
    logger.info("🚀 ETL: CONSUMO GRANULAR POR CLASSE")
    logger.info("=" * 80)
    
    try:
        engine = get_engine()
        
        # 1. Criar tabela
        logger.info("📋 Criando/verificando tabela...")
        criar_tabela_consumo_granular(engine)
        
        # 2. Estimar dados
        logger.info("🔢 Estimando consumo granular por classe...")
        df_consumo = estimar_consumo_granular_classe()
        
        if df_consumo.empty:
            logger.error("❌ Falha ao estimar dados")
            return 0
        
        # 3. Carregar dados
        logger.info("💾 Carregando dados no banco...")
        total_inserido = carregar_consumo_granular_classe(df_consumo, engine)
        
        # 4. Verificar resultado
        logger.info("\n" + "=" * 80)
        logger.info("📊 RESUMO DE CONSUMO GRANULAR")
        logger.info("=" * 80)
        
        with engine.connect() as conn:
            resultado = conn.execute(text("""
                SELECT 
                    distribuidora,
                    COUNT(DISTINCT classe_consumo) as total_classes,
                    ROUND(SUM(consumo_kwh)::numeric, 0) as consumo_total_kwh,
                    ROUND(SUM(consumo_kwh) / 24000::numeric, 2) as carga_media_mw
                FROM consumo_granular_classe
                WHERE data_medicao = (SELECT MAX(data_medicao) FROM consumo_granular_classe)
                GROUP BY distribuidora
                ORDER BY distribuidora
            """))
            
            for dist, classes, consumo_kwh, carga_mw in resultado:
                logger.info(f"  {dist:12} | Classes: {classes} | {consumo_kwh:15,.0f} kWh | {carga_mw:8.2f} MW")
        
        logger.info("=" * 80)
        logger.info("✅ ETL CONCLUÍDO COM SUCESSO")
        logger.info("=" * 80)
        
        return total_inserido
        
    except Exception as e:
        logger.error(f"❌ Erro fatal no ETL: {e}", exc_info=True)
        return 0


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return executar_etl_consumo_granular()


if __name__ == "__main__":
    main()
