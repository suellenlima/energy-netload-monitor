#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Serviço para gerenciar múltiplas fontes de imagens para transformadores
- Google Maps (RGB, alta resolução, zoom 19-20)
- CBERS-4A (Multibanda, 2m/pixel)
"""

import os
import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class ImagemMultiFonteService:
    """
    Gerencia imagens de múltiplas fontes para um transformador
    """
    
    def __init__(self, google_maps_api_key: Optional[str] = None):
        """
        Args:
            google_maps_api_key: Chave da API Google Maps
        """
        self.google_maps_api_key = google_maps_api_key or os.getenv('GOOGLE_MAPS_API_KEY')
        self.google_maps_base_url = "https://maps.googleapis.com/maps/api/staticmap"
    
    def gerar_url_google_maps(self, 
                             latitude: float, 
                             longitude: float,
                             zoom: int = 19,
                             largura: int = 640,
                             altura: int = 640,
                             style: Optional[str] = None) -> str:
        """
        Gera URL do Google Maps Static API com zoom máximo
        
        Args:
            latitude: Latitude do transformador
            longitude: Longitude do transformador
            zoom: Nível de zoom (19-20 para máximo)
            largura: Largura da imagem em pixels
            altura: Altura da imagem em pixels
            style: Estilo customizado (satelite, mapa, etc)
            
        Returns:
            URL da imagem do Google Maps
        """
        
        # Parâmetros obrigatórios
        params = {
            'center': f'{latitude},{longitude}',
            'zoom': zoom,
            'size': f'{largura}x{altura}',
            'maptype': 'satellite',  # Satellite view
            'key': self.google_maps_api_key
        }
        
        # Adicionar marcador no transformador
        params['markers'] = f'color:red|{latitude},{longitude}'
        
        # Construir URL
        url = f"{self.google_maps_base_url}?{urlencode(params)}"
        
        logger.info(f"URL Google Maps gerada para ({latitude}, {longitude}) zoom={zoom}")
        return url
    
    def gerar_url_google_maps_com_poligono(self,
                                          latitude: float,
                                          longitude: float,
                                          vertices_poligono: Optional[List[Tuple[float, float]]] = None,
                                          zoom: int = 19,
                                          largura: int = 640,
                                          altura: int = 640) -> str:
        """
        Gera URL do Google Maps com área poligonal destacada
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            vertices_poligono: Lista de (lat, lon) dos vértices do polígono
            zoom: Nível de zoom
            largura: Largura da imagem
            altura: Altura da imagem
            
        Returns:
            URL do Google Maps com polígono
        """
        
        params = {
            'center': f'{latitude},{longitude}',
            'zoom': zoom,
            'size': f'{largura}x{altura}',
            'maptype': 'satellite',
            'key': self.google_maps_api_key
        }
        
        # Adicionar polígono se fornecido
        if vertices_poligono:
            # Formato: path=color:color|weight:weight|fillcolor:fillcolor|lat1,lon1|lat2,lon2|...
            caminho_vertices = '|'.join([f'{lat},{lon}' for lat, lon in vertices_poligono])
            # Fechar o polígono repetindo o primeiro vértice
            if vertices_poligono:
                caminho_vertices += f'|{vertices_poligono[0][0]},{vertices_poligono[0][1]}'
            
            params['path'] = f'color:0xFF0000FF|weight:2|fillcolor:0xFF000033|{caminho_vertices}'
        
        # Marcador no centro
        params['markers'] = f'color:red|{latitude},{longitude}'
        
        url = f"{self.google_maps_base_url}?{urlencode(params)}"
        
        logger.info(f"URL Google Maps com polígono gerada: {len(vertices_poligono or [])} vértices")
        return url
    
    def gerar_urls_todas_fontes(self,
                               transformador_id: int,
                               latitude: float,
                               longitude: float,
                               vertices_poligono: Optional[List[Tuple[float, float]]] = None) -> Dict[str, Dict]:
        """
        Gera URLs de todas as fontes disponíveis para um transformador
        
        Args:
            transformador_id: ID do transformador
            latitude: Latitude
            longitude: Longitude
            vertices_poligono: Vértices da área poligonal
            
        Returns:
            Dicionário com URLs de cada fonte
        """
        
        urls = {}
        
        # Google Maps - com área poligonal
        try:
            url_google = self.gerar_url_google_maps_com_poligono(
                latitude, longitude, vertices_poligono, zoom=19
            )
            urls['google_maps'] = {
                'url': url_google,
                'fonte': 'google_maps',
                'zoom': 19,
                'resolucao': '~1m/pixel',
                'bandas': 'RGB (3)',
                'descricao': 'Google Maps Satellite - Zoom máximo com área poligonal',
                'timestamp_gerado': datetime.now().isoformat()
            }
            logger.info(f"✓ URL Google Maps gerada para transformador {transformador_id}")
        except Exception as e:
            logger.error(f"Erro ao gerar URL Google Maps: {e}")
        
        # Google Maps - sem polígono (alternativa)
        try:
            url_google_simples = self.gerar_url_google_maps(
                latitude, longitude, zoom=19
            )
            urls['google_maps_simples'] = {
                'url': url_google_simples,
                'fonte': 'google_maps_simples',
                'zoom': 19,
                'resolucao': '~1m/pixel',
                'bandas': 'RGB (3)',
                'descricao': 'Google Maps Satellite - Zoom máximo (simples)',
                'timestamp_gerado': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Erro ao gerar URL Google Maps simples: {e}")
        
        # CBERS-4A seria adicionado depois (requer consulta ao banco)
        # Por enquanto só temos Google Maps
        
        return urls

# Teste
if __name__ == "__main__":
    service = ImagemMultiFonteService(google_maps_api_key="sua_chave_aqui")
    
    # Exemplo com transformador fictício
    lat, lon = -15.789, -48.043
    vertices = [
        (lat - 0.001, lon - 0.001),
        (lat + 0.001, lon - 0.001),
        (lat + 0.001, lon + 0.001),
        (lat - 0.001, lon + 0.001),
    ]
    
    urls = service.gerar_urls_todas_fontes(400, lat, lon, vertices)
    
    print("\nURLs Geradas:")
    for fonte, dados in urls.items():
        print(f"\n{fonte}:")
        print(f"  Descrição: {dados['descricao']}")
        print(f"  Bandas: {dados['bandas']}")
        print(f"  URL: {dados['url'][:100]}...")
