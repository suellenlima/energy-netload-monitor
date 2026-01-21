#!/usr/bin/env python
"""
Script de demonstração para testar a funcionalidade de detecção de subestações.
Pode ser executado após inicializar o banco de dados.

Uso:
    python demo_subestacoes.py
"""

import logging
import sys
from pathlib import Path

# Setup paths
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Imports
import pandas as pd
from sqlalchemy import create_engine, text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("demo")

def demo_subestacoes():
    """Demonstração completa das funcionalidades de subestações."""
    
    logger.info("=" * 60)
    logger.info("🏢 DEMONSTRAÇÃO - DETECÇÃO DE SUBESTAÇÕES")
    logger.info("=" * 60)
    
    # Conectar ao banco
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/energy_monitor"
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # 1. Verificar tabelas
            logger.info("\n1️⃣ Verificando tabelas...")
            
            tabelas = ["subestacoes_ons", "subestacoes_detectadas", "usinas_siga", "gd_detalhada"]
            for tabela in tabelas:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{tabela}'
                """)).scalar()
                status = "✅ Existe" if result else "❌ Não existe"
                logger.info(f"  - {tabela}: {status}")
            
            # 2. Contar registros por tabela
            logger.info("\n2️⃣ Contando registros...")
            
            queries = {
                "subestacoes_ons": "SELECT COUNT(*) as count FROM subestacoes_ons",
                "subestacoes_detectadas": "SELECT COUNT(*) as count FROM subestacoes_detectadas",
                "usinas_siga": "SELECT COUNT(*) as count FROM usinas_siga",
                "gd_detalhada": "SELECT COUNT(*) as count FROM gd_detalhada"
            }
            
            for tabela, query in queries.items():
                try:
                    result = conn.execute(text(query)).scalar()
                    logger.info(f"  - {tabela}: {result} registros")
                except Exception as e:
                    logger.warning(f"  - {tabela}: Erro ao contar ({e})")
            
            # 3. Visualizar dados da ONS
            logger.info("\n3️⃣ Amostra de Subestações ONS...")
            
            try:
                df_ons = pd.read_sql(
                    """
                    SELECT nome, sigla_se, tensao_kv, subsistema, distribuidora 
                    FROM subestacoes_ons 
                    LIMIT 5
                    """,
                    engine
                )
                
                if not df_ons.empty:
                    logger.info("\n" + df_ons.to_string(index=False))
                else:
                    logger.info("  (Nenhum registro ainda)")
            except Exception as e:
                logger.warning(f"  Erro ao buscar dados ONS: {e}")
            
            # 4. Visualizar dados detectados
            logger.info("\n4️⃣ Amostra de Subestações Detectadas...")
            
            try:
                df_det = pd.read_sql(
                    """
                    SELECT nome, cluster_id, quantidade_gd, potencia_total_mw, 
                           raio_deteccao_km, distribuidora 
                    FROM subestacoes_detectadas 
                    LIMIT 5
                    """,
                    engine
                )
                
                if not df_det.empty:
                    logger.info("\n" + df_det.to_string(index=False))
                else:
                    logger.info("  (Nenhum registro ainda)")
            except Exception as e:
                logger.warning(f"  Erro ao buscar dados detectados: {e}")
            
            # 5. Resumo por distribuidora
            logger.info("\n5️⃣ Resumo por Distribuidora...")
            
            try:
                df_resumo = pd.read_sql(
                    """
                    SELECT 
                        distribuidora,
                        COUNT(CASE WHEN origem = 'ons' THEN 1 END) as total_ons,
                        COUNT(CASE WHEN origem = 'detectada' THEN 1 END) as total_detectadas,
                        COUNT(*) as total
                    FROM (
                        SELECT distribuidora, 'ons' as origem FROM subestacoes_ons
                        UNION ALL
                        SELECT distribuidora, 'detectada' as origem FROM subestacoes_detectadas
                    ) t
                    GROUP BY distribuidora
                    ORDER BY total DESC
                    LIMIT 10
                    """,
                    engine
                )
                
                if not df_resumo.empty:
                    logger.info("\n" + df_resumo.to_string(index=False))
                else:
                    logger.info("  (Nenhum registro ainda)")
            except Exception as e:
                logger.warning(f"  Erro ao gerar resumo: {e}")
            
            # 6. Estatísticas geoespaciais
            logger.info("\n6️⃣ Estatísticas Geoespaciais...")
            
            try:
                stats = pd.read_sql(
                    """
                    SELECT 
                        subsistema,
                        COUNT(*) as quantidade,
                        ROUND(AVG(potencia_total_mw)::numeric, 2) as potencia_media_mw,
                        ROUND(MAX(potencia_total_mw)::numeric, 2) as potencia_maxima_mw,
                        ROUND(AVG(raio_deteccao_km)::numeric, 2) as raio_medio_km
                    FROM subestacoes_detectadas
                    GROUP BY subsistema
                    ORDER BY quantidade DESC
                    """,
                    engine
                )
                
                if not stats.empty:
                    logger.info("\n" + stats.to_string(index=False))
                else:
                    logger.info("  (Nenhum dado detectado)")
            except Exception as e:
                logger.warning(f"  Erro ao gerar estatísticas: {e}")
            
    except Exception as e:
        logger.error(f"❌ Erro de conexão: {e}")
        logger.info("\nDica: Verifique se o banco PostgreSQL está rodando:")
        logger.info("  docker-compose up -d postgres")
        sys.exit(1)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ DEMONSTRAÇÃO CONCLUÍDA")
    logger.info("=" * 60)
    
    logger.info("\n📊 Próximos passos:")
    logger.info("  1. Para carregar dados ONS: python -m etl_pipeline.src.extractors.subestacoes_client")
    logger.info("  2. Para detectar subestações: POST /subestacoes/detectadas/atualizar")
    logger.info("  3. Visualizar no frontend: streamlit run frontend/src/app.py")


if __name__ == "__main__":
    demo_subestacoes()
