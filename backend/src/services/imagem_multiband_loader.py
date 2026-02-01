"""
Serviço para carregar e processar imagens multiespectrais (múltiplas bandas).
"""

import logging
import numpy as np
import cv2
import requests
from typing import Dict, Optional
from rasterio.io import MemoryFile
from io import BytesIO


class ImagemMultibandaLoader:
    """Carrega e processa imagens multiespectrais com múltiplas bandas."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def baixar_bandas(
        self,
        urls_bandas: Dict[str, str],
        timeout: int = 30
    ) -> Dict[str, np.ndarray]:
        """
        Baixa múltiplas bandas de uma imagem.
        
        Args:
            urls_bandas: Dicionário {nome_banda: url}
                        Ex: {'blue': 'https://...', 'red': 'https://...'}
            timeout: Timeout em segundos para cada request
        
        Returns:
            Dicionário {nome_banda: array} com as bandas carregadas
        """
        bandas = {}
        
        for nome, url in urls_bandas.items():
            try:
                self.logger.info(f"Baixando banda {nome} de {url[:60]}...")
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                
                # Tentar como TIFF primeiro
                try:
                    with MemoryFile(response.content) as memfile:
                        with memfile.open() as dataset:
                            banda = dataset.read(1)
                            bandas[nome] = banda
                            self.logger.info(
                                f"  OK: {nome} carregado {banda.shape}, "
                                f"range=[{banda.min()}, {banda.max()}]"
                            )
                except Exception as tiff_error:
                    self.logger.debug(f"  Não é TIFF, tentando como imagem comum: {tiff_error}")
                    # Tentar como imagem comum (PNG, JPG)
                    img_array = np.frombuffer(response.content, dtype=np.uint8)
                    img = cv2.imdecode(img_array, cv2.IMREAD_GRAYSCALE)
                    if img is not None:
                        bandas[nome] = img
                        self.logger.info(f"  OK: {nome} carregado como imagem {img.shape}")
                    else:
                        raise ValueError(f"Não foi possível decodificar {nome}")
            
            except Exception as e:
                self.logger.warning(f"Erro ao baixar banda {nome}: {e}")
                continue
        
        return bandas
    
    def normalizar_banda(self, banda: np.ndarray) -> np.ndarray:
        """
        Normaliza uma banda usando percentil 2%-98%.
        
        Args:
            banda: Array da banda (qualquer dtype)
        
        Returns:
            Array normalizado uint8 (0-255)
        """
        banda = banda.astype(np.float32)
        p2 = np.percentile(banda, 2)
        p98 = np.percentile(banda, 98)
        
        if p98 > p2:
            banda_norm = (banda - p2) / (p98 - p2) * 255
        else:
            banda_norm = np.full_like(banda, 128)
        
        return np.clip(banda_norm, 0, 255).astype(np.uint8)
    
    def processar_rgb_clahe(
        self,
        bandas: Dict[str, np.ndarray],
        clip_limit: float = 2.0,
        tile_size: int = 8
    ) -> np.ndarray:
        """
        Processa imagem RGB com normalização e CLAHE.
        
        Args:
            bandas: Dicionário com pelo menos 'red', 'green', 'blue'
                   Se faltar algum, replica o 'red'
            clip_limit: Limite de clipping para CLAHE
            tile_size: Tamanho do tile para CLAHE
        
        Returns:
            Imagem BGR processada (uint8, 3 canais)
        """
        # Normalizar cada banda
        b_norm = self.normalizar_banda(
            bandas.get('blue', bandas.get('red'))
        )
        g_norm = self.normalizar_banda(
            bandas.get('green', bandas.get('red'))
        )
        r_norm = self.normalizar_banda(
            bandas.get('red', bandas.get('red'))
        )
        
        # Aplicar CLAHE
        clahe = cv2.createCLAHE(
            clipLimit=clip_limit,
            tileGridSize=(tile_size, tile_size)
        )
        
        b = clahe.apply(b_norm)
        g = clahe.apply(g_norm)
        r = clahe.apply(r_norm)
        
        self.logger.info(
            f"RGB processado: B(mean={b.mean():.1f}), "
            f"G(mean={g.mean():.1f}), R(mean={r.mean():.1f})"
        )
        
        # Retornar em BGR (ordem do OpenCV)
        return cv2.merge([b, g, r])
    
    def calcular_ndvi(
        self,
        bandas: Dict[str, np.ndarray]
    ) -> Optional[np.ndarray]:
        """
        Calcula NDVI (Normalized Difference Vegetation Index).
        
        NDVI = (NIR - Red) / (NIR + Red)
        
        Args:
            bandas: Dicionário com 'nir' e 'red'
        
        Returns:
            Array NDVI em range [-1, 1], ou None se faltar bandas
        """
        if 'nir' not in bandas or 'red' not in bandas:
            self.logger.debug("NIR ou Red não disponível para NDVI")
            return None
        
        try:
            nir = bandas['nir'].astype(np.float32)
            red = bandas['red'].astype(np.float32)
            
            # Normalizar primeiro
            nir = self.normalizar_banda(nir).astype(np.float32)
            red = self.normalizar_banda(red).astype(np.float32)
            
            denominador = nir + red + 1e-10
            ndvi = (nir - red) / denominador
            ndvi = np.clip(ndvi, -1, 1)
            
            self.logger.info(
                f"NDVI calculado: range=[{ndvi.min():.3f}, {ndvi.max():.3f}], "
                f"mean={ndvi.mean():.3f}"
            )
            
            return ndvi
        
        except Exception as e:
            self.logger.warning(f"Erro ao calcular NDVI: {e}")
            return None
    
    def criar_mascara_urbana(
        self,
        ndvi: np.ndarray,
        threshold: float = 0.3
    ) -> np.ndarray:
        """
        Cria máscara de zona urbana baseada em NDVI.
        
        Args:
            ndvi: Array NDVI
            threshold: Limiar (NDVI < threshold = urbano)
        
        Returns:
            Array booleano (True = urbano, False = vegetação)
        """
        mascara = ndvi < threshold
        
        percentual_urbano = np.sum(mascara) / mascara.size * 100
        self.logger.info(
            f"Máscara urbana: {percentual_urbano:.1f}% urbano, "
            f"{100 - percentual_urbano:.1f}% vegetação"
        )
        
        return mascara
    
    def processar_completo(
        self,
        urls_bandas: Dict[str, str],
        clip_limit: float = 2.0,
        tile_size: int = 8
    ) -> Dict:
        """
        Pipeline completo: baixa, normaliza, processa e calcula índices.
        
        Args:
            urls_bandas: Dicionário {nome_banda: url}
            clip_limit: Parâmetro CLAHE
            tile_size: Parâmetro CLAHE
        
        Returns:
            Dicionário com:
                - 'rgb': Imagem BGR processada
                - 'ndvi': Array NDVI (ou None)
                - 'mascara_urbana': Array booleano (ou None)
                - 'bandas': Dicionário original de bandas
        """
        # 1. Baixar bandas
        bandas = self.baixar_bandas(urls_bandas)
        
        if not bandas:
            raise ValueError("Nenhuma banda foi carregada com sucesso")
        
        # 2. Processar RGB
        rgb = self.processar_rgb_clahe(bandas, clip_limit, tile_size)
        
        # 3. Calcular NDVI
        ndvi = self.calcular_ndvi(bandas)
        
        # 4. Criar máscara urbana
        mascara_urbana = None
        if ndvi is not None:
            mascara_urbana = self.criar_mascara_urbana(ndvi)
        
        return {
            'rgb': rgb,
            'ndvi': ndvi,
            'mascara_urbana': mascara_urbana,
            'bandas': bandas
        }
