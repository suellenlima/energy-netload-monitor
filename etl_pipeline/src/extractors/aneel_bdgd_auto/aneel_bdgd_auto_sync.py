"""
ETL ANEEL BDGD - Download automático de ZIPs e inserção no banco

Este script:
1. Lê items do JSON com metadata de ANEEL
2. Filtra por tags específicas (BDGD, SIG-R, Distribuicao)
3. Baixa arquivos ZIP com dados (um de cada vez)
4. Extrai e processa shapefiles
5. Carrega no PostgreSQL com PostGIS

NOTA: Utiliza services centralizados para evitar duplicação de lógica
"""

import os
import sys
import logging
import requests
import zipfile
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import geopandas as gpd
from sqlalchemy import create_engine, text, exc
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Adicionar src ao path para importar services
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importar services centralizados (schema é single source of truth)
from services.aneel_bdgd_service import (
    TransformerService,
    SubstationService,
    ConsumerService,
    DistributorService,
    AreaService,
    GeometryService,
    ClassificationService
)

# Configurações do Banco de Dados (obter de variáveis de ambiente)
try:
    from core import create_db_engine, load_settings, table_exists
    settings = load_settings()
    DB_URL = settings.database.url
    if not DB_URL:
        raise ValueError("DATABASE_URL não configurada")
    engine = create_db_engine(DB_URL)
    logger.info(f"✓ Conectado ao banco: {DB_URL.split('@')[1] if '@' in DB_URL else 'local'}")
    
    # Inicializar services (todas as operações vão por aqui)
    transformer_svc = TransformerService(engine)
    substation_svc = SubstationService(engine)
    consumer_svc = ConsumerService(engine)
    distributor_svc = DistributorService(engine)
    area_svc = AreaService(engine)
    
except Exception as e:
    logger.error(f"❌ Erro ao conectar ao banco: {e}")
    sys.exit(1)

# Tags para filtrar items (case-insensitive)
TAGS_PARA_BUSCAR = ['BDGD', 'SIG-R', 'Distribuicao']
TAGS_PARA_BAIXAR = set(t.lower() for t in TAGS_PARA_BUSCAR)

# Diretório de downloads e JSON de metadados
DOWNLOADS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "aneel_downloads"
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

JSON_RESUMO = DOWNLOADS_DIR / "aneel_items_resumo.json"
JSON_COMPLETO = DOWNLOADS_DIR / "aneel_items_completo.json"

# Mapeamento de tipos de dados
LAYER_MAPPING = {
    'transformadores_aneel': ['UNTRD', 'transformador', 'trafo'],
    'subestacoes_aneel': ['SUB', 'subestacao', 'subest'],
    'consumidores_bt_aneel': ['UCBT', 'consumidor_bt', 'bt'],
    'consumidores_mt_aneel': ['UCMT', 'consumidor_mt', 'mt'],
    'consumidores_at_aneel': ['UCAT', 'consumidor_at', 'at'],
}

# Field mapping (BDGD -> DB schema)
FIELD_MAPPING = {
    'transformadores_aneel': {
        'COD_ID': 'codigo_aneel',
        'DISTRIBUIDORA': 'distribuidora',
        'POT_NOM': 'potencia_kva',
        'TEN_PRI': 'tensao_primaria_kv',
        'TEN_SEC': 'tensao_secundaria_kv',
    },
    'subestacoes_aneel': {
        'COD_ID': 'codigo_aneel',
        'NOME': 'nome',
        'DISTRIBUIDORA': 'distribuidora',
    },
    'consumidores_bt_aneel': {
        'COD_ID': 'codigo',
        'DIST': 'dist_codigo',
        'SUB': 'subestacao_codigo',
        'CLAS_SUB': 'classe_subclasse_codigo',
        'TEN_FORN': 'tensao_fornecimento_codigo',
        'CAR_INST': 'carga_instalada_kw',
    },
    'consumidores_mt_aneel': {
        'COD_ID': 'codigo',
        'DIST': 'dist_codigo',
        'SUB': 'subestacao_codigo',
        'CTMT': 'circuito_mt_codigo',
        'CLAS_SUB': 'classe_subclasse_codigo',
        'TEN_FORN': 'tensao_fornecimento_codigo',
        'CAR_INST': 'carga_instalada_kw',
        'DEM_CONT': 'demanda_contratada_kw',
    },
    'consumidores_at_aneel': {
        'COD_ID': 'codigo',
        'DIST': 'dist_codigo',
        'SUB': 'subestacao_codigo',
        'CTAT': 'circuito_at_codigo',
        'CLAS_SUB': 'classe_subclasse_codigo',
        'TEN_FORN': 'tensao_fornecimento_codigo',
        'CAR_INST': 'carga_instalada_kw',
        'DEM_CONT': 'demanda_contratada_kw',
    }
}

def baixar_e_extrair(url: str, destino: Path) -> bool:
    """Baixa ZIP e extrai, depois deleta o ZIP para economizar espaço"""
    try:
        logger.info(f"   ⬇️  Baixando: {url[:70]}...")
        destino.mkdir(parents=True, exist_ok=True)
        
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        
        zip_path = destino / "dados.zip"
        total_size = int(r.headers.get('content-length', 0))
        
        with open(zip_path, 'wb') as f:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    percent = (downloaded / total_size) * 100
                    if downloaded % (1024*1024) == 0:  # A cada 1MB
                        logger.debug(f"      {percent:.1f}%")
        
        logger.info(f"   ✓ Extraindo: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destino)
        
        # Deletar ZIP imediatamente após extrair para economizar espaço
        if zip_path.exists():
            zip_path.unlink()
            logger.debug(f"   🗑️  ZIP deletado para liberar espaço")
        
        logger.info(f"   ✓ Extração concluída")
        return True
    
    except Exception as e:
        logger.error(f"   ❌ Erro: {e}")
        return False




def processar_bdgd(pasta: Path, distribuidora: str = "DESCONHECIDA") -> Dict:
    """Processa arquivos SHP/GDB da pasta usando services centralizados"""
    stats = {
        'transformadores_inseridos': 0,
        'subestacoes_inseridas': 0,
        'consumidores_bt_inseridos': 0,
        'consumidores_mt_inseridos': 0,
        'consumidores_at_inseridos': 0,
        'total': 0
    }
    
    try:
        # 1️⃣ Processsar subestações PRIMEIRO
        for shp_file in pasta.rglob("*.shp"):
            if 'SUB' in shp_file.name.upper() or 'BARRAMENTO' in shp_file.name.upper() or 'CTMT' in shp_file.name.upper():
                logger.info(f"      📄 Processando subestações: {shp_file.name}")
                try:
                    gdf = gpd.read_file(shp_file)
                    df = SubstationService.extract(gdf, distribuidora)
                    if not df.empty:
                        inseridos = substation_svc.insert(df, distribuidora)
                        stats['subestacoes_inseridas'] += inseridos
                except Exception as e:
                    logger.error(f"      ❌ Erro: {e}")
        
        # 2️⃣ Processar transformadores
        for shp_file in pasta.rglob("*.shp"):
            if 'TRAFO' in shp_file.name.upper() or 'UNTRD' in shp_file.name.upper() or 'TRANSFORMADOR' in shp_file.name.upper():
                logger.info(f"      📄 Processando transformadores: {shp_file.name}")
                try:
                    gdf = gpd.read_file(shp_file)
                    df = TransformerService.extract(gdf, distribuidora)
                    if not df.empty:
                        inseridos = transformer_svc.insert(df, distribuidora)
                        stats['transformadores_inseridos'] += inseridos
                except Exception as e:
                    logger.error(f"      ❌ Erro: {e}")
        
        # 3️⃣ Processar consumidores BT
        for shp_file in pasta.rglob("*.shp"):
            if 'UCBT' in shp_file.name.upper() or ('CONSUMIDOR' in shp_file.name.upper() and 'BT' in shp_file.name.upper()):
                logger.info(f"      📄 Processando consumidores BT: {shp_file.name}")
                try:
                    gdf = gpd.read_file(shp_file)
                    df = ConsumerService.extract_bt(gdf, distribuidora)
                    if not df.empty:
                        inseridos = consumer_svc.insert_bt(df, distribuidora)
                        stats['consumidores_bt_inseridos'] += inseridos
                except Exception as e:
                    logger.error(f"      ❌ Erro: {e}")
        
        # 4️⃣ Processar consumidores MT
        for shp_file in pasta.rglob("*.shp"):
            if 'UCMT' in shp_file.name.upper() or ('CONSUMIDOR' in shp_file.name.upper() and 'MT' in shp_file.name.upper()):
                logger.info(f"      📄 Processando consumidores MT: {shp_file.name}")
                try:
                    gdf = gpd.read_file(shp_file)
                    df = ConsumerService.extract_mt(gdf, distribuidora)
                    if not df.empty:
                        inseridos = consumer_svc.insert_mt(df, distribuidora)
                        stats['consumidores_mt_inseridos'] += inseridos
                except Exception as e:
                    logger.error(f"      ❌ Erro: {e}")
        
        # 5️⃣ Processar consumidores AT
        for shp_file in pasta.rglob("*.shp"):
            if 'UCAT' in shp_file.name.upper() or ('CONSUMIDOR' in shp_file.name.upper() and 'AT' in shp_file.name.upper()):
                logger.info(f"      📄 Processando consumidores AT: {shp_file.name}")
                try:
                    gdf = gpd.read_file(shp_file)
                    df = ConsumerService.extract_at(gdf, distribuidora)
                    if not df.empty:
                        inseridos = consumer_svc.insert_at(df, distribuidora)
                        stats['consumidores_at_inseridos'] += inseridos
                except Exception as e:
                    logger.error(f"      ❌ Erro: {e}")
        
        # 6️⃣ Atualizar tabela de distribuidoras
        distributor_svc.update(distribuidora, stats['transformadores_inseridos'], 
                              stats['subestacoes_inseridas'], distribuidora)
        
        # 7️⃣ CALCULAR ÁREAS DOS TRANSFORMADORES (ConvexHull + Buffer)
        logger.info(f"    🗺️  Calculando áreas de cobertura...")
        for tipo_tensao in ['BT', 'MT', 'AT']:
            area_svc.calculate(tipo_tensao, distribuidora)
        
        stats['total'] = sum([stats['transformadores_inseridos'], stats['subestacoes_inseridas'], 
                             stats['consumidores_bt_inseridos'], stats['consumidores_mt_inseridos'],
                             stats['consumidores_at_inseridos']])
    
    except Exception as e:
        logger.error(f"   ❌ Erro ao varrer pasta: {e}")
    
    # Deletar pasta com arquivos após processar para economizar espaço
    try:
        import shutil
        if pasta.exists():
            shutil.rmtree(pasta)
            logger.info(f"   🗑️  Pasta deletada para liberar espaço ({pasta.name})")
    except Exception as e:
        logger.warning(f"   ⚠️  Erro ao deletar pasta: {e}")
    
    return stats


def identificar_tabela(nome_arquivo: str) -> Optional[str]:
    """Identifica a tabela pelo nome do arquivo"""
    nome_upper = nome_arquivo.upper()
    
    for tabela, palavras_chave in LAYER_MAPPING.items():
        for palavra in palavras_chave:
            if palavra.upper() in nome_upper:
                return tabela
    
    return None


def descobrir_items() -> List[Dict]:
    """Carrega items do JSON com metadata do download anterior"""
    logger.info("📂 Carregando items do arquivo JSON (resumido)...")
    
    items = []
    json_file = JSON_RESUMO
    
    if not json_file.exists():
        logger.error(f"❌ Arquivo JSON não encontrado: {json_file}")
        logger.info(f"   Execute primeiro: python aneel_downloader_final.py")
        return []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        all_items = data.get('items', [])
        logger.info(f"   Total de items no JSON: {len(all_items)}")
        
        # Converter tags para minúsculas para comparação case-insensitive
        tags_alvo_lower = set(t.lower() for t in TAGS_PARA_BUSCAR)
        logger.info(f"   Filtrando items que contenham TODAS as tags: {TAGS_PARA_BUSCAR}")
        
        # Filtrar por tags (case-insensitive) - DEVE TER TODAS AS 3 TAGS
        for item in all_items:
            item_tags_raw = item.get('tags', [])
            item_tags_lower = set(t.lower() for t in item_tags_raw)
            
            # Verificar se tem TODAS as tags desejadas (usando issubset)
            if tags_alvo_lower.issubset(item_tags_lower):
                # URL pode ser null, então tentar construir baseado no ID
                download_url = item.get('url')
                
                if not download_url and item.get('id'):
                    # Tentar construir URL padrão do ArcGIS
                    item_id = item['id']
                    if '_' not in item_id:
                        item_id = f"{item_id}_0"
                    download_url = f"https://dadosabertos-aneel.opendata.arcgis.com/datasets/{item_id}.zip"
                
                items.append({
                    'id': item.get('id'),
                    'name': item.get('name'),
                    'owner': item.get('owner'),
                    'download_url': download_url,
                    'tags': item_tags_raw
                })
        
        logger.info(f"   ✓ {len(items)} items filtrados (com TODAS as tags: {TAGS_PARA_BUSCAR})")
        
        # Estatísticas
        items_com_url = sum(1 for i in items if i.get('download_url'))
        logger.info(f"   ℹ️  Items com URL de download: {items_com_url}/{len(items)}")
        
        return items
    
    except Exception as e:
        logger.error(f"❌ Erro ao carregar JSON: {e}")
        return []


# (Funções antigas de descoberta removidas - usar JSON em vez disso)

def executar_etl():
    """Executa o ETL completo - baixa e processa um item de cada vez"""
    logger.info("\n" + "="*70)
    logger.info("🚀 SINCRONIZAÇÃO ANEEL BDGD - PROCESSAMENTO SEQUENCIAL")
    logger.info("="*70 + "\n")
    
    try:
        # 1. Descobrir/carregar items do JSON
        items = descobrir_items()
        
        if not items:
            logger.warning("❌ Nenhum item para processar")
            return
        
        logger.info(f"\n📋 Total de {len(items)} items para processar (um de cada vez)\n")
        
        # 2. Processar cada item sequencialmente
        total_registros = 0
        total_sucesso = 0
        total_erro = 0
        resultados = []
        
        for i, item in enumerate(items, 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"[{i}/{len(items)}] Processando: {item['name'][:60]}")
            logger.info(f"{'='*70}")
            logger.info(f"     ID: {item['id'][:40]}...")
            logger.info(f"     Owner: {item['owner']}")
            logger.info(f"     Tags: {', '.join(item.get('tags', [])[:3])}")
            
            pasta_destino = DOWNLOADS_DIR / item['id']
            resultado = {
                'item_id': item['id'],
                'name': item['name'],
                'status': 'ERRO',
                'registros': 0,
                'erro': None
            }
            
            try:
                # Validar URL de download
                if not item.get('download_url'):
                    logger.warning(f"   ⚠️  URL de download não disponível")
                    resultado['erro'] = 'URL não disponível'
                    total_erro += 1
                    resultados.append(resultado)
                    continue
                
                logger.info(f"   📥 Baixando arquivo ZIP...")
                
                # Baixar e extrair
                if not baixar_e_extrair(item['download_url'], pasta_destino):
                    logger.warning(f"   ⚠️  Falha no download/extração")
                    resultado['erro'] = 'Falha no download'
                    total_erro += 1
                    resultados.append(resultado)
                    continue
                
                logger.info(f"   📊 Processando dados...")
                
                # Processar com novas funções especializadas
                stats = processar_bdgd(pasta_destino, item['owner'])
                registros = stats['total']
                total_registros += registros
                
                resultado['registros'] = registros
                if registros > 0:
                    total_sucesso += 1
                    resultado['status'] = 'SUCESSO'
                    logger.info(f"   ✅ {registros} registros inseridos com sucesso!")
                else:
                    logger.warning(f"   ⚠️  Nenhum registro encontrado")
                    resultado['status'] = 'SEM_DADOS'
                    total_erro += 1
            
            except Exception as e:
                logger.error(f"   ❌ Erro: {e}", exc_info=True)
                resultado['erro'] = str(e)
                total_erro += 1
            
            resultados.append(resultado)
            
            # Pequeno delay entre items para não sobrecarregar o banco
            if i < len(items):
                logger.info(f"\n   ⏳ Aguardando 2 segundos antes do próximo item...")
                time.sleep(2)
        
        # 3. Resumo final
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 RESUMO DA SINCRONIZAÇÃO")
        logger.info(f"{'='*70}")
        logger.info(f"  Items processados com sucesso: {total_sucesso}/{len(items)}")
        logger.info(f"  Erros: {total_erro}")
        logger.info(f"  Total de registros inseridos: {total_registros}")
        
        # Mostrar detalhes de cada item
        logger.info(f"\n  Detalhes por item:")
        for res in resultados:
            status_icon = "✅" if res['status'] == 'SUCESSO' else "⚠️ " if res['status'] == 'SEM_DADOS' else "❌"
            logger.info(f"    {status_icon} {res['name'][:50]}")
            logger.info(f"       Status: {res['status']} | Registros: {res['registros']}")
            if res['erro']:
                logger.info(f"       Erro: {res['erro'][:60]}")
        
        logger.info(f"{'='*70}\n")
        
        if total_registros > 0:
            logger.info(f"✅ Sincronização concluída com sucesso!")
            logger.info(f"   {total_registros} registros no total foram inseridos no banco!")
        else:
            logger.warning(f"⚠️  Nenhum registro foi inserido")
    
    except Exception as e:
        logger.error(f"❌ Erro geral: {e}", exc_info=True)


if __name__ == "__main__":
    executar_etl()