"""
Serviço para Google Maps Static API

Fornece imagens de alta resolução (0.15-0.6m/pixel) como fallback
quando CBERS-4A não tiver cobertura.

Author: Energy Netload Monitor
Date: 2025-01-30
"""

import logging
import os
from typing import Optional, Tuple
from io import BytesIO
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from dotenv import load_dotenv

# Carregar variáveis de ambiente do .env
env_path = Path(__file__).parent.parent.parent.parent / 'backend' / '.env'
if not env_path.exists():
    env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

logger = logging.getLogger(__name__)


class GoogleMapsService:
    """Serviço para buscar imagens do Google Maps Static API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa o serviço Google Maps
        
        Args:
            api_key: API key do Google Maps (ou use GOOGLE_MAPS_API_KEY no .env)
        """
        self.api_key = api_key or os.getenv('GOOGLE_MAPS_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/staticmap"
        
        # Limites da API
        self.quota_diaria = 25000  # Grátis até 25k imagens/dia
        self.custo_por_imagem = 0.002  # $0.002 após quota
        
        if not self.api_key:
            logger.warning("Google Maps API key não configurada. Use .env ou parâmetro.")
        else:
            logger.info("Google Maps Service inicializado")
    
    def esta_disponivel(self) -> bool:
        """Verifica se API key está configurada"""
        return self.api_key is not None and len(self.api_key) > 0
    
    def buscar_imagem_satelite(
        self,
        latitude: float,
        longitude: float,
        zoom: int = 20,
        tamanho: Tuple[int, int] = (640, 640),
        tipo_mapa: str = "satellite"
    ) -> Optional[np.ndarray]:
        """
        Busca imagem de satélite do Google Maps
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            zoom: Nível de zoom (1-21, onde 21 é mais próximo)
                  zoom=20 → ~0.3m/pixel
                  zoom=19 → ~0.6m/pixel
                  zoom=18 → ~1.2m/pixel
            tamanho: Dimensões da imagem (max 640x640 sem premium)
            tipo_mapa: "satellite", "hybrid", "roadmap"
        
        Returns:
            Imagem como numpy array (RGB) ou None se erro
        """
        if not self.esta_disponivel():
            logger.error("Google Maps API key não configurada")
            return None
        
        try:
            # Construir URL
            params = {
                'center': f"{latitude},{longitude}",
                'zoom': zoom,
                'size': f"{tamanho[0]}x{tamanho[1]}",
                'maptype': tipo_mapa,
                'key': self.api_key,
                'scale': 2  # Retina/alta resolução (max 1280x1280)
            }
            
            logger.info(f"Buscando imagem Google Maps: lat={latitude}, lon={longitude}, zoom={zoom}")
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Verificar se não é erro da API
            content_type = response.headers.get('content-type', '')
            if 'image' not in content_type:
                logger.error(f"Google Maps retornou erro: {response.text}")
                return None
            
            # Converter para numpy array RGB
            imagem = Image.open(BytesIO(response.content))
            
            # Garantir que está em RGB (não RGBA ou Grayscale)
            if imagem.mode != 'RGB':
                imagem = imagem.convert('RGB')
            
            imagem_array = np.array(imagem)
            
            logger.info(f"✓ Imagem Google Maps baixada: {imagem_array.shape}")
            logger.info(f"  Resolução estimada: ~{self._estimar_resolucao(zoom):.2f}m/pixel")
            
            return imagem_array
            
        except requests.RequestException as e:
            logger.error(f"Erro ao buscar Google Maps: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro ao processar imagem Google Maps: {e}", exc_info=True)
            return None
    
    def _estimar_resolucao(self, zoom: int) -> float:
        """
        Estima resolução em metros/pixel baseado no zoom
        
        Fórmula aproximada no equador:
        resolução = 156543.03392 * cos(latitude) / (2 ^ zoom)
        
        Args:
            zoom: Nível de zoom (1-21)
            
        Returns:
            Resolução em metros/pixel
        """
        # Aproximação no equador
        resolucao_base = 156543.03392  # metros/pixel no zoom 0
        return resolucao_base / (2 ** zoom)
