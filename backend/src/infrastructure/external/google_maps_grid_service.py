"""
Google Maps Grid Service - Infrastructure Layer

Serviço para gerar grid de URLs do Google Maps Static API
para cobertura de área ao redor de um transformador.

Author: Energy Netload Monitor
Date: 2026-02-07
"""

import math
import logging
from typing import List, Dict, Tuple
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class GridPoint:
    """Ponto do grid com coordenadas e índice"""
    lat: float
    lon: float
    indice: int
    row: int
    col: int


class GoogleMapsGridService:
    """
    Serviço para gerar grid de coordenadas e URLs do Google Maps.
    
    Gera um grid NxN de pontos ao redor de uma coordenada central
    e cria URLs do Google Maps Static API para cada ponto.
    """
    
    # Resolução aproximada por zoom (metros/pixel)
    RESOLUCAO_POR_ZOOM = {
        18: 2.39,   # ~2.4m/pixel
        19: 1.19,   # ~1.2m/pixel
        20: 0.60,   # ~0.6m/pixel
        21: 0.30    # ~0.3m/pixel
    }
    
    # Tamanho padrão da imagem em pixels
    IMAGE_SIZE = 640
    
    def __init__(self, api_key: str = None):
        """
        Inicializa o serviço.
        
        Args:
            api_key: Chave da API do Google Maps (opcional para gerar URLs)
        """
        self.api_key = api_key
        self.logger = logger
    
    def calcular_offset_coordenadas(
        self,
        lat: float,
        lon: float,
        offset_metros: float,
        direcao: str
    ) -> Tuple[float, float]:
        """
        Calcula coordenadas com offset em metros.
        
        Args:
            lat: Latitude central
            lon: Longitude central
            offset_metros: Distância em metros
            direcao: 'N', 'S', 'E', 'W'
            
        Returns:
            Tupla (nova_lat, nova_lon)
        """
        # Conversão aproximada: 1 grau de latitude ≈ 111km
        # Longitude varia com a latitude (menor perto dos polos)
        
        km_offset = offset_metros / 1000.0
        
        if direcao == 'N':
            return (lat + (km_offset / 111.0), lon)
        elif direcao == 'S':
            return (lat - (km_offset / 111.0), lon)
        elif direcao == 'E':
            lon_offset = km_offset / (111.0 * math.cos(math.radians(lat)))
            return (lat, lon + lon_offset)
        elif direcao == 'W':
            lon_offset = km_offset / (111.0 * math.cos(math.radians(lat)))
            return (lat, lon - lon_offset)
        else:
            return (lat, lon)
    
    def gerar_grid_coordenadas(
        self,
        lat_centro: float,
        lon_centro: float,
        grid_size: int,
        raio_metros: float
    ) -> List[GridPoint]:
        """
        Gera grid de coordenadas ao redor de um ponto central.
        
        Args:
            lat_centro: Latitude do centro
            lon_centro: Longitude do centro
            grid_size: Tamanho do grid (ex: 3 = grid 3x3 = 9 pontos)
            raio_metros: Raio de cobertura em metros
            
        Returns:
            Lista de GridPoint com coordenadas do grid
        """
        pontos = []
        
        # Calcular espaçamento entre pontos
        if grid_size == 1:
            # Grid 1x1 = apenas o ponto central
            pontos.append(GridPoint(
                lat=lat_centro,
                lon=lon_centro,
                indice=0,
                row=0,
                col=0
            ))
            return pontos
        
        # Para grid NxN, distribuir pontos uniformemente
        espacamento = (2 * raio_metros) / (grid_size - 1) if grid_size > 1 else 0
        
        indice = 0
        for row in range(grid_size):
            for col in range(grid_size):
                # Calcular offset em relação ao centro
                offset_lat = (row - (grid_size - 1) / 2) * espacamento
                offset_lon = (col - (grid_size - 1) / 2) * espacamento
                
                # Aplicar offsets
                nova_lat = lat_centro + (offset_lat / 1000.0 / 111.0)
                
                # Longitude precisa considerar a latitude
                lon_per_km = 111.0 * math.cos(math.radians(lat_centro))
                nova_lon = lon_centro + (offset_lon / 1000.0 / lon_per_km)
                
                pontos.append(GridPoint(
                    lat=nova_lat,
                    lon=nova_lon,
                    indice=indice,
                    row=row,
                    col=col
                ))
                indice += 1
        
        self.logger.info(
            f"Grid {grid_size}x{grid_size} gerado: {len(pontos)} pontos "
            f"(espaçamento: {espacamento:.1f}m)"
        )
        
        return pontos
    
    def gerar_url_google_maps(
        self,
        lat: float,
        lon: float,
        zoom: int = 20,
        size: int = 640,
        maptype: str = "satellite"
    ) -> str:
        """
        Gera URL do Google Maps Static API.
        
        Args:
            lat: Latitude
            lon: Longitude
            zoom: Nível de zoom (18-21)
            size: Tamanho da imagem em pixels
            maptype: Tipo do mapa (satellite, roadmap, etc)
            
        Returns:
            URL completa do Google Maps Static API
        """
        base_url = "https://maps.googleapis.com/maps/api/staticmap"
        
        params = [
            f"center={lat},{lon}",
            f"zoom={zoom}",
            f"size={size}x{size}",
            f"maptype={maptype}",
            "scale=2"  # Retina display (melhor resolução)
        ]
        
        if self.api_key:
            params.append(f"key={self.api_key}")
        
        url = f"{base_url}?{'&'.join(params)}"
        return url
    
    def gerar_grid_urls(
        self,
        lat_centro: float,
        lon_centro: float,
        grid_size: int = 3,
        zoom: int = 20,
        raio_metros: float = 300
    ) -> List[Dict]:
        """
        Gera grid completo com URLs do Google Maps.
        
        Args:
            lat_centro: Latitude central
            lon_centro: Longitude central
            grid_size: Tamanho do grid (1-5)
            zoom: Zoom do Google Maps (18-21)
            raio_metros: Raio de cobertura em metros
            
        Returns:
            Lista de dicionários com coordenadas, índice e URL
        """
        # Gerar coordenadas do grid
        pontos = self.gerar_grid_coordenadas(
            lat_centro,
            lon_centro,
            grid_size,
            raio_metros
        )
        
        # Gerar URLs para cada ponto
        grid_data = []
        for ponto in pontos:
            url = self.gerar_url_google_maps(
                ponto.lat,
                ponto.lon,
                zoom=zoom,
                size=self.IMAGE_SIZE
            )
            
            grid_data.append({
                'lat': ponto.lat,
                'lon': ponto.lon,
                'indice': ponto.indice,
                'row': ponto.row,
                'col': ponto.col,
                'url': url,
                'zoom': zoom,
                'resolucao_m_pixel': self.RESOLUCAO_POR_ZOOM.get(zoom, 1.0)
            })
        
        self.logger.info(f"✓ Grid completo: {len(grid_data)} URLs geradas (zoom={zoom})")
        
        return grid_data
    
    def estimar_cobertura_area(
        self,
        grid_size: int,
        zoom: int,
        image_size: int = 640
    ) -> Dict:
        """
        Estima a área total coberta pelo grid.
        
        Args:
            grid_size: Tamanho do grid
            zoom: Zoom do Google Maps
            image_size: Tamanho da imagem em pixels
            
        Returns:
            Dict com estatísticas de cobertura
        """
        resolucao = self.RESOLUCAO_POR_ZOOM.get(zoom, 1.0)
        
        # Área coberta por uma imagem
        largura_m = image_size * resolucao
        area_imagem_m2 = largura_m ** 2
        
        # Área total do grid (sem considerar sobreposição)
        num_imagens = grid_size ** 2
        area_total_m2 = area_imagem_m2 * num_imagens
        
        return {
            'grid_size': grid_size,
            'num_imagens': num_imagens,
            'zoom': zoom,
            'resolucao_m_pixel': resolucao,
            'largura_imagem_m': largura_m,
            'area_por_imagem_m2': area_imagem_m2,
            'area_total_coberta_m2': area_total_m2,
            'area_total_coberta_km2': area_total_m2 / 1_000_000
        }
