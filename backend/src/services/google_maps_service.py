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
    
    def calcular_zoom_para_resolucao(self, resolucao_desejada_m: float) -> int:
        """
        Calcula zoom necessário para atingir resolução desejada
        
        Args:
            resolucao_desejada_m: Resolução desejada em metros/pixel
            
        Returns:
            Nível de zoom recomendado (1-21)
        """
        import math
        
        resolucao_base = 156543.03392
        zoom = math.log2(resolucao_base / resolucao_desejada_m)
        zoom = max(1, min(21, int(round(zoom))))
        
        return zoom
    
    def calcular_cobertura_area(
        self,
        zoom: int,
        tamanho: Tuple[int, int] = (640, 640),
        latitude: float = 0.0
    ) -> Tuple[float, float]:
        """
        Calcula área coberta pela imagem em km²
        
        Args:
            zoom: Nível de zoom
            tamanho: Dimensões da imagem em pixels
            latitude: Latitude (afeta cálculo de longitude)
            
        Returns:
            (largura_km, altura_km)
        """
        import math
        
        resolucao = self._estimar_resolucao(zoom)
        
        # Corrigir para latitude (longitude é comprimida perto dos polos)
        cos_lat = math.cos(math.radians(latitude))
        
        largura_m = tamanho[0] * resolucao * cos_lat
        altura_m = tamanho[1] * resolucao
        
        return (largura_m / 1000, altura_m / 1000)
    
    def estimar_custo(self, num_imagens: int) -> dict:
        """
        Estima custo para número de imagens
        
        Args:
            num_imagens: Número de imagens a buscar
            
        Returns:
            Dicionário com estimativa de custo
        """
        if num_imagens <= self.quota_diaria:
            custo = 0.0
            imagens_pagas = 0
        else:
            imagens_pagas = num_imagens - self.quota_diaria
            custo = imagens_pagas * self.custo_por_imagem
        
        return {
            "total_imagens": num_imagens,
            "imagens_gratis": min(num_imagens, self.quota_diaria),
            "imagens_pagas": imagens_pagas,
            "custo_usd": round(custo, 2),
            "nota": "Primeiras 25.000 imagens/mês são grátis"
        }
    
    def buscar_multiplas_imagens(
        self,
        coordenadas: list,
        zoom: int = 20,
        **kwargs
    ) -> list:
        """
        Busca múltiplas imagens em lote
        
        Args:
            coordenadas: Lista de (latitude, longitude)
            zoom: Nível de zoom
            **kwargs: Argumentos adicionais para buscar_imagem_satelite
            
        Returns:
            Lista de arrays numpy (pode conter None se falhou)
        """
        resultados = []
        
        logger.info(f"Buscando {len(coordenadas)} imagens do Google Maps")
        
        for idx, (lat, lon) in enumerate(coordenadas, 1):
            logger.info(f"[{idx}/{len(coordenadas)}] Buscando ({lat}, {lon})")
            
            imagem = self.buscar_imagem_satelite(
                latitude=lat,
                longitude=lon,
                zoom=zoom,
                **kwargs
            )
            
            resultados.append(imagem)
        
        sucessos = sum(1 for img in resultados if img is not None)
        logger.info(f"✓ {sucessos}/{len(coordenadas)} imagens baixadas com sucesso")
        
        return resultados
