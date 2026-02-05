"""
Verificador e Validador de Dados ANEEL BDGD
==============================================

Script auxiliar para:
1. Validar integridade dos dados carregados
2. Listar camadas disponíveis em cada GDB
3. Gerar relatório de cobertura por distribuidora
4. Limpar dados duplicados

Uso:
    python aneel_bdgd_validator.py [--list-layers] [--report] [--clean]


O validator é complementar e muito útil para:

--report → Gera relatório de cobertura (já executado acima)
--list-layers → Lista camadas disponíveis em cada GDB
--export → Exporta dados por distribuidora em CSV
--clean → Remove registros duplicados do banco
--all → Executa tudo acima
Recomendação: Mantenha o validator! Ele é útil para:

Validar qualidade dos dados após cada ETL
Gerar relatórios de cobertura automaticamente
Limpar duplicatas periodicamente
Exportar dados para análise em Excel/BI

"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple
import json

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text, inspect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Adicionar paths
import os
if os.path.exists("/src"):
    # Rodando dentro do Docker
    SRC_DIR = Path("/src")
else:
    # Rodando localmente
    SRC_DIR = Path(__file__).resolve().parents[3]  # etl_pipeline/src/extractors/aneel_bdgd_local -> src
    
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from core import create_db_engine, load_settings
    settings = load_settings()
    DB_URL = settings.database.url
    if not DB_URL:
        raise ValueError("DATABASE_URL não configurada")
    engine = create_db_engine(DB_URL)
    logger.info(f"✓ Conectado ao banco: {DB_URL.split('@')[1] if '@' in DB_URL else 'local'}")
except Exception as e:
    logger.error(f"❌ Erro ao conectar ao banco: {e}")
    sys.exit(1)

ANEEL_BDGD_DIR = Path(__file__).resolve().parents[4] / "data" / "aneel_bdgd" if not os.path.exists("/app/data") else Path("/app/data/aneel_bdgd")
REPORTS_DIR = ANEEL_BDGD_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def list_layers_in_distribuidoras():
    """Lista todas as camadas disponíveis em cada GDB"""
    logger.info(f"\n{'='*80}")
    logger.info(f"CAMADAS DISPONÍVEIS NOS GDBs")
    logger.info(f"{'='*80}\n")
    
    report = {}
    
    for folder in sorted(ANEEL_BDGD_DIR.iterdir()):
        if not folder.is_dir() or not (folder / 'gdb').exists():
            continue
        
        dist_name = folder.name
        gdb_path = folder / 'gdb'
        
        logger.info(f"📂 {dist_name}")
        report[dist_name] = []
        
        try:
            layers = gpd.io.file.listlayers(str(gdb_path))
            for layer in layers:
                layer_name = layer if isinstance(layer, str) else layer[0]
                logger.info(f"    - {layer_name}")
                report[dist_name].append(layer_name)
        except Exception as e:
            logger.error(f"    ❌ Erro ao listar camadas: {e}")
    
    # Salvar relatório
    report_file = REPORTS_DIR / "layers_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"\n✓ Relatório salvo em: {report_file}\n")
    
    return report


def generate_coverage_report():
    """Gera relatório de cobertura de dados"""
    logger.info(f"\n{'='*80}")
    logger.info(f"RELATÓRIO DE COBERTURA")
    logger.info(f"{'='*80}\n")
    
    report = {}
    
    # Dados de transformadores
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    distribuidora,
                    COUNT(*) as total,
                    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as com_coordenadas,
                    COUNT(CASE WHEN potencia_kva IS NOT NULL THEN 1 END) as com_potencia,
                    ROUND(AVG(potencia_kva)::numeric, 2) as potencia_media_kva
                FROM transformadores_aneel
                GROUP BY distribuidora
                ORDER BY total DESC
            """))
            
            logger.info(f"{'TRANSFORMADORES':^80}")
            logger.info(f"{'Distribuidora':<30} {'Total':>10} {'Com Coord':>12} {'Potência Média':>15}")
            logger.info(f"{'-'*80}")
            
            trafo_data = []
            for row in result:
                dist, total, com_coord, com_pot, media_pot = row
                logger.info(f"{str(dist):<30} {total:>10} {com_coord:>12} {media_pot:>14.2f} kVA")
                trafo_data.append({
                    'distribuidora': dist,
                    'total': total,
                    'com_coordenadas': com_coord,
                    'com_potencia': com_pot,
                    'potencia_media_kva': float(media_pot) if media_pot else 0
                })
            report['transformadores'] = trafo_data
    except Exception as e:
        logger.warning(f"⚠ Erro ao gerar relatório de transformadores: {e}")
    
    # Dados de subestações
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    distribuidora,
                    COUNT(*) as total,
                    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as com_coordenadas
                FROM subestacoes_aneel
                GROUP BY distribuidora
                ORDER BY total DESC
            """))
            
            logger.info(f"\n{'SUBESTAÇÕES':^80}")
            logger.info(f"{'Distribuidora':<30} {'Total':>10} {'Com Coord':>12}")
            logger.info(f"{'-'*80}")
            
            subest_data = []
            for row in result:
                dist, total, com_coord = row
                logger.info(f"{str(dist):<30} {total:>10} {com_coord:>12}")
                subest_data.append({
                    'distribuidora': dist,
                    'total': total,
                    'com_coordenadas': com_coord
                })
            report['subestacoes'] = subest_data
    except Exception as e:
        logger.warning(f"⚠ Erro ao gerar relatório de subestações: {e}")
    
    # Salvar relatório
    report_file = REPORTS_DIR / "coverage_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"\n✓ Relatório salvo em: {report_file}\n")
    
    return report


def clean_duplicates():
    """Remove registros duplicados mantendo o mais recente"""
    logger.info(f"\n{'='*80}")
    logger.info(f"LIMPEZA DE DUPLICATAS")
    logger.info(f"{'='*80}\n")
    
    try:
        with engine.connect() as conn:
            # Limpar transformadores duplicados
            result = conn.execute(text("""
                WITH ranked AS (
                    SELECT *, 
                        ROW_NUMBER() OVER (PARTITION BY codigo ORDER BY data_atualizacao DESC) as rn
                    FROM transformadores_aneel
                )
                DELETE FROM transformadores_aneel
                WHERE id IN (
                    SELECT id FROM ranked WHERE rn > 1
                )
            """))
            logger.info(f"✓ Transformadores duplicados removidos: {result.rowcount}")
            
            # Limpar subestações duplicadas
            result = conn.execute(text("""
                WITH ranked AS (
                    SELECT *, 
                        ROW_NUMBER() OVER (PARTITION BY codigo ORDER BY data_atualizacao DESC) as rn
                    FROM subestacoes_aneel
                )
                DELETE FROM subestacoes_aneel
                WHERE id IN (
                    SELECT id FROM ranked WHERE rn > 1
                )
            """))
            logger.info(f"✓ Subestações duplicadas removidas: {result.rowcount}")
            
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Erro ao limpar duplicatas: {e}")


def export_data_by_distribuidora():
    """Exporta dados por distribuidora em CSV"""
    logger.info(f"\n{'='*80}")
    logger.info(f"EXPORTANDO DADOS POR DISTRIBUIDORA")
    logger.info(f"{'='*80}\n")
    
    export_dir = REPORTS_DIR / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Transformadores
        df_trafo = pd.read_sql_table('transformadores_aneel', engine)
        for dist in df_trafo['distribuidora'].unique():
            df_dist = df_trafo[df_trafo['distribuidora'] == dist]
            file_path = export_dir / f"transformadores_{dist}.csv"
            df_dist.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"  ✓ {len(df_dist)} transformadores de {dist}")
        
        # Subestações
        df_subest = pd.read_sql_table('subestacoes_aneel', engine)
        for dist in df_subest['distribuidora'].unique():
            df_dist = df_subest[df_subest['distribuidora'] == dist]
            file_path = export_dir / f"subestacoes_{dist}.csv"
            df_dist.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"  ✓ {len(df_dist)} subestações de {dist}")
        
        logger.info(f"\n✓ Dados exportados em: {export_dir}\n")
    
    except Exception as e:
        logger.error(f"❌ Erro ao exportar dados: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list-layers', action='store_true', help='Listar camadas disponíveis')
    parser.add_argument('--report', action='store_true', help='Gerar relatório de cobertura')
    parser.add_argument('--clean', action='store_true', help='Limpar duplicatas')
    parser.add_argument('--export', action='store_true', help='Exportar dados por distribuidora')
    parser.add_argument('--all', action='store_true', help='Executar tudo')
    
    args = parser.parse_args()
    
    if args.all:
        args.list_layers = True
        args.report = True
        args.export = True
    
    if args.list_layers:
        list_layers_in_distribuidoras()
    
    if args.report:
        generate_coverage_report()
    
    if args.export:
        export_data_by_distribuidora()
    
    if args.clean:
        clean_duplicates()
    
    if not any([args.list_layers, args.report, args.clean, args.export]):
        logger.info("Use --help para ver opções disponíveis")


if __name__ == '__main__':
    main()
