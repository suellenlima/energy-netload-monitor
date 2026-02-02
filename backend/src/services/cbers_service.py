"""
Serviço para acessar imagens CBERS-4A do INPE (Brazil Data Cube)
Resolução: 2 metros por pixel (sensor WPM)
"""

import os
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import requests

logger = logging.getLogger(__name__)


@dataclass
class ImagemCBERS:
    """Representa uma imagem CBERS-4A"""
    id: str
    data: datetime
    cobertura_nuvem: float
    resolucao: str
    sensor: str
    urls: Dict[str, str]  # pan, red, green, blue, nir
    bbox: List[float]
    coleção: str


class CBERSService:
    """
    Serviço para buscar e processar imagens CBERS-4A do INPE
    
    Características CBERS-4A WPM:
    - Resolução PAN: 2 metros
    - Resolução Multiespectral: 8 metros
    - Revisita: ~31 dias
    - Cobertura: América do Sul (prioridade Brasil)
    - Acesso: Gratuito via Brazil Data Cube
    """
    
    def __init__(self):
        # Brazil Data Cube STAC
        self.stac_url = "https://data.inpe.br/bdc/stac/v1"
        
        # Coleções disponíveis
        self.colecoes = {
            "CBERS-4A-WPM": "CBERS-4A-WPM-L4-SR",  # 2m resolução (PAN)
            "CBERS-4-MUX": "CBERS4-MUX-2M-1",      # ~20m resolução
            "CBERS-4-WFI": "CBERS4-WFI-16D-2"      # ~64m resolução
        }
        
        self.colecao_padrao = self.colecoes["CBERS-4A-WPM"]
    
    def buscar_imagens(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5.0,
        data_inicio: Optional[str] = None,
        data_fim: Optional[str] = None,
        cobertura_nuvem_max: int = 30,
        colecao: str = None
    ) -> List[ImagemCBERS]:
        """
        Busca imagens CBERS-4A para uma localização
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            raio_km: Raio de busca em km
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            cobertura_nuvem_max: Cobertura máxima de nuvens (%)
            colecao: Nome da coleção (default: CBERS-4A-WPM)
        
        Returns:
            Lista de imagens encontradas
        """
        try:
            from pystac_client import Client
        except ImportError:
            logger.error("pystac_client não está instalado")
            logger.info("Instale com: pip install pystac-client")
            return []
        
        # Usar coleção padrão se não especificada
        if not colecao:
            colecao = self.colecao_padrao
        
        # Calcular bounding box
        # Aproximação: 1 grau = ~111 km
        delta = raio_km / 111.0
        bbox = [
            longitude - delta,
            latitude - delta,
            longitude + delta,
            latitude + delta
        ]
        
        # Datas padrão se não fornecidas
        if not data_fim:
            data_fim = datetime.now().strftime("%Y-%m-%d")
        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        
        logger.info(f"Buscando CBERS-4A para ({latitude}, {longitude})")
        logger.info(f"  Período: {data_inicio} a {data_fim}")
        logger.info(f"  Raio: {raio_km} km")
        logger.info(f"  Coleção: {colecao}")
        
        try:
            # Conectar ao STAC do INPE
            catalog = Client.open(self.stac_url, timeout=30)
            
            # Buscar imagens
            search = catalog.search(
                collections=[colecao],
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=50
            )
            
            items = list(search.get_items())
            logger.info(f"Encontradas {len(items)} imagens CBERS-4A")
            
            # Filtrar por cobertura de nuvens e converter para ImagemCBERS
            resultados = []
            for item in items:
                props = item.properties
                cloud_cover = props.get('eo:cloud_cover', props.get('cloud_cover', 100))
                
                # Filtrar por nuvens
                if cloud_cover > cobertura_nuvem_max:
                    continue
                
                # Extrair URLs dos assets
                urls = {}
                for asset_key in ['pan', 'red', 'green', 'blue', 'nir']:
                    if asset_key in item.assets:
                        urls[asset_key] = item.assets[asset_key].href
                
                # Criar objeto ImagemCBERS
                imagem = ImagemCBERS(
                    id=item.id,
                    data=datetime.fromisoformat(props['datetime'].replace('Z', '+00:00')),
                    cobertura_nuvem=cloud_cover,
                    resolucao="2m (PAN) / 8m (MS)",
                    sensor="CBERS-4A WPM",
                    urls=urls,
                    bbox=item.bbox,
                    coleção=colecao
                )
                
                resultados.append(imagem)
            
            # Ordenar por data (mais recente primeiro) e cobertura de nuvens
            resultados.sort(key=lambda x: (x.cobertura_nuvem, -x.data.timestamp()))
            
            logger.info(f"Após filtro de nuvens: {len(resultados)} imagens")
            
            return resultados
            
        except Exception as e:
            logger.error(f"Erro ao buscar CBERS-4A: {e}")
            logger.exception(e)
            return []
    
    def criar_composicao_rgb(
        self,
        imagem: ImagemCBERS,
        banda_r: str = 'red',
        banda_g: str = 'green',
        banda_b: str = 'blue'
    ) -> bytes:
        """
        Cria composição RGB a partir das bandas
        
        Args:
            imagem: Objeto ImagemCBERS
            banda_r, banda_g, banda_b: Nomes das bandas
        
        Returns:
            Bytes da imagem RGB (JPEG)
        """
        try:
            import rasterio
            from rasterio.plot import reshape_as_image
            import numpy as np
            from PIL import Image
            from io import BytesIO
            
            logger.info("Criando composição RGB...")
            
            # Baixar bandas
            bandas = {}
            for nome, banda_key in [('R', banda_r), ('G', banda_g), ('B', banda_b)]:
                if banda_key not in imagem.urls:
                    raise ValueError(f"Banda {banda_key} não disponível")
                
                url = imagem.urls[banda_key]
                
                # Ler GeoTIFF diretamente da URL
                with rasterio.open(url) as src:
                    band_data = src.read(1)
                    bandas[nome] = band_data
            
            # Empilhar bandas
            rgb = np.stack([bandas['R'], bandas['G'], bandas['B']], axis=-1)
            
            # Normalizar para 0-255
            rgb_min = rgb.min(axis=(0, 1), keepdims=True)
            rgb_max = rgb.max(axis=(0, 1), keepdims=True)
            rgb_norm = ((rgb - rgb_min) / (rgb_max - rgb_min + 1e-8) * 255).astype(np.uint8)
            
            # Converter para PIL Image
            img = Image.fromarray(rgb_norm)
            
            # Salvar como JPEG
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=85)
            
            logger.info(f"✓ Composição RGB criada: {img.size}")
            
            return buffer.getvalue()
            
        except ImportError:
            logger.error("rasterio não está instalado")
            logger.info("Instale com: pip install rasterio")
            raise
        except Exception as e:
            logger.error(f"Erro ao criar composição RGB: {e}")
            raise
