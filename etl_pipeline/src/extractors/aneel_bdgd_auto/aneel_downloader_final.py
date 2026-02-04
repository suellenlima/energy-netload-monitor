#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ANEEL OpenData Portal - Downloader de todos os items BDGD e Distribuição
Baixa items de TODAS as páginas do portal https://dadosabertos-aneel.opendata.arcgis.com/
"""

import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path

# ==================== CONFIGURAÇÃO ====================
BASE_URL = 'https://dadosabertos-aneel.opendata.arcgis.com/api/v3/datasets'
OUTPUT_DIR = Path('data/aneel_downloads')
SEARCH_TERMS = ['BDGD', 'distribuicao']
PAGE_SIZE = 100  # Máximo por página
MAX_RETRIES = 3
RETRY_DELAY = 2  # segundos

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CLASSE DOWNLOADER ====================
class ANEELDownloader:
    def __init__(self, base_url, output_dir, search_terms, page_size=100):
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.search_terms = search_terms
        self.page_size = page_size
        self.all_items = []
        self.stats = {}

    def fetch_page(self, search_term, page_number, retries=MAX_RETRIES):
        """Busca uma página de items com retry automático."""
        for attempt in range(retries):
            try:
                logger.info(f"Buscando página {page_number} para '{search_term}' (tentativa {attempt + 1}/{retries})")
                
                params = {
                    'q': search_term,
                    'page[number]': page_number,
                    'page[size]': self.page_size
                }
                
                response = requests.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('data', [])
                    meta = data.get('meta', {})
                    page_info = meta.get('page', {})
                    
                    logger.info(f"  ✓ Página {page_number}: {len(items)} items (total: {meta.get('total', 'N/A')})")
                    return items, meta, True
                else:
                    logger.warning(f"  ✗ Status {response.status_code}")
                    if attempt < retries - 1:
                        time.sleep(RETRY_DELAY)
                    continue
                    
            except Exception as e:
                logger.error(f"  ✗ Erro: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY)
                continue
        
        return [], {}, False

    def download_all(self):
        """Baixa todos os items de todos os search terms e todas as páginas."""
        logger.info(f"Iniciando download de BDGD + distribuição")
        logger.info(f"Search terms: {self.search_terms}")
        logger.info(f"Tamanho da página: {self.page_size} items")
        
        for search_term in self.search_terms:
            logger.info(f"\n{'='*70}")
            logger.info(f"PROCESSANDO: {search_term}")
            logger.info(f"{'='*70}")
            
            page_number = 1
            term_items = []
            
            while True:
                items, meta, success = self.fetch_page(search_term, page_number)
                
                if not success or not items:
                    logger.info(f"Fim do download para '{search_term}'")
                    break
                
                term_items.extend(items)
                self.all_items.extend(items)
                
                page_info = meta.get('page', {})
                next_start = page_info.get('nextStart')
                
                # Se não há próxima página, termina
                if not next_start:
                    logger.info(f"Última página alcançada (total: {len(term_items)} items)")
                    break
                
                page_number += 1
                time.sleep(0.5)  # Pequeno delay entre requisições
            
            self.stats[search_term] = len(term_items)
            logger.info(f"Total para '{search_term}': {len(term_items)} items")
        
        logger.info(f"\n{'='*70}")
        logger.info(f"DOWNLOAD COMPLETO!")
        logger.info(f"Total geral: {len(self.all_items)} items")
        logger.info(f"{'='*70}")

    def save_to_json(self, filename='aneel_items_completo.json'):
        """Salva todos os items em JSON com metadata."""
        output_file = self.output_dir / filename
        
        output_data = {
            'metadata': {
                'download_date': datetime.now().isoformat(),
                'total_items': len(self.all_items),
                'source': 'https://dadosabertos-aneel.opendata.arcgis.com/',
                'search_terms': self.search_terms,
                'stats': self.stats
            },
            'items': self.all_items
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ Arquivo salvo: {output_file}")
        return output_file

    def save_summary(self, filename='aneel_items_resumo.json'):
        """Salva resumo com apenas IDs e nomes (arquivo menor)."""
        output_file = self.output_dir / filename
        
        summary_items = []
        for item in self.all_items:
            attrs = item.get('attributes', {})
            summary_items.append({
                'id': item.get('id'),
                'name': attrs.get('name'),
                'owner': attrs.get('owner'),
                'url': attrs.get('url'),
                'tags': attrs.get('tags', [])
            })
        
        output_data = {
            'metadata': {
                'download_date': datetime.now().isoformat(),
                'total_items': len(summary_items),
                'stats': self.stats
            },
            'items': summary_items
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ Resumo salvo: {output_file}")
        return output_file

    def print_statistics(self):
        """Imprime estatísticas finais."""
        logger.info(f"\n{'='*70}")
        logger.info(f"ESTATÍSTICAS FINAIS")
        logger.info(f"{'='*70}")
        
        for term, count in self.stats.items():
            logger.info(f"  {term}: {count} items")
        
        logger.info(f"\n  TOTAL GERAL: {len(self.all_items)} items")
        
        # Estatísticas de tags
        all_tags = {}
        for item in self.all_items:
            tags = item.get('attributes', {}).get('tags', [])
            for tag in tags:
                all_tags[tag] = all_tags.get(tag, 0) + 1
        
        logger.info(f"\n  Tags mais comuns:")
        sorted_tags = sorted(all_tags.items(), key=lambda x: x[1], reverse=True)
        for tag, count in sorted_tags[:10]:
            logger.info(f"    - {tag}: {count}")
        
        logger.info(f"{'='*70}\n")


# ==================== MAIN ====================
if __name__ == '__main__':
    try:
        downloader = ANEELDownloader(
            base_url=BASE_URL,
            output_dir=OUTPUT_DIR,
            search_terms=SEARCH_TERMS,
            page_size=PAGE_SIZE
        )
        
        # Baixar todos os items
        downloader.download_all()
        
        # Salvar em JSON
        downloader.save_to_json()
        downloader.save_summary()
        
        # Imprimir estatísticas
        downloader.print_statistics()
        
        logger.info("✓ Download concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro fatal: {str(e)}", exc_info=True)
