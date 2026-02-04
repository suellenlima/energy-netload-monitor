#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Serviço Unificado de Imagens - Consolidação de 3 serviços:
- ImagemMultiFonteService: Geração de URLs de múltiplas fontes
- ImagemSalvamentoService: Salvamento de imagens no banco de dados
- ImagemStrategyService: Estratégia híbrida com fallback automático

Funcionalidades:
1. Google Maps (RGB, alta resolução, zoom 19-20)
2. CBERS-4A (Multibanda, 2m/pixel)
3. Sentinela-2 (Fallback, 10m/pixel)
"""

import os
import logging
import io
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from pathlib import Path
from urllib.parse import urlencode

import numpy as np
import requests
from PIL import Image
from sqlalchemy import text, Engine
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
env_path = Path(__file__).parent.parent.parent.parent / 'backend' / '.env'
if not env_path.exists():
    env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)


# ============================================================================
# DATACLASSES
# ============================================================================

@dataclass
class ImagemObtida:
    """Resultado de busca de imagem"""
    fonte: str  # "cbers", "google_maps", "sentinel"
    imagem: np.ndarray
    resolucao_m: float
    latitude: float
    longitude: float
    timestamp: datetime
    metadata: dict


# ============================================================================
# CLASSE PRINCIPAL - ImageService (Unificada)
# ============================================================================

class ImageService:
    """
    Serviço unificado de imagens consolidando funcionalidades de:
    - ImagemMultiFonteService (geração de URLs)
    - ImagemSalvamentoService (salvamento em BD)
    - ImagemStrategyService (estratégia híbrida)
    """
    
    def __init__(self, engine: Optional[Engine] = None, 
                 google_maps_api_key: Optional[str] = None,
                 preferencia_resolucao: float = 2.0):
        """
        Inicializa o serviço unificado
        
        Args:
            engine: SQLAlchemy Engine para operações de banco de dados
            google_maps_api_key: Chave da API Google Maps
            preferencia_resolucao: Resolução preferida em metros/pixel
        """
        self.engine = engine
        self.google_maps_api_key = google_maps_api_key or os.getenv('GOOGLE_MAPS_API_KEY')
        self.google_maps_base_url = "https://maps.googleapis.com/maps/api/staticmap"
        self.preferencia_resolucao = preferencia_resolucao
        
        # Inicializar serviços relacionados
        try:
            from .inpe_service import CBERSService
            self.cbers = CBERSService()
        except ImportError:
            self.cbers = None
            logger.warning("CBERSService não disponível")
        
        try:
            from .google_maps_service import GoogleMapsService
            self.google_maps = GoogleMapsService()
        except ImportError:
            self.google_maps = None
            logger.warning("GoogleMapsService não disponível")
        
        try:
            from .cache_service import CacheService
            self.cache = CacheService(cache_dir="data/cache/imagens")
        except ImportError:
            self.cache = None
            logger.warning("CacheService não disponível")
        
        # Estatísticas de uso
        self.stats = {
            "cbers": {"tentativas": 0, "sucessos": 0},
            "google_maps": {"tentativas": 0, "sucessos": 0},
            "sentinel": {"tentativas": 0, "sucessos": 0}
        }
        
        logger.info("ImageService inicializado (serviço unificado)")
        logger.info(f"  Resolução preferida: {preferencia_resolucao}m/pixel")
        logger.info(f"  CBERS disponível: {'✓' if self.cbers else '✗'}")
        logger.info(f"  Google Maps disponível: {'✓' if self.google_maps_api_key else '✗'}")

    # ========================================================================
    # PARTE 1: GERAÇÃO DE URLs (Ex-ImagemMultiFonteService)
    # ========================================================================
    
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
        
        params = {
            'center': f'{latitude},{longitude}',
            'zoom': zoom,
            'size': f'{largura}x{altura}',
            'maptype': 'satellite',
            'key': self.google_maps_api_key
        }
        
        # Adicionar marcador no transformador
        params['markers'] = f'color:red|{latitude},{longitude}'
        
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
        
        if vertices_poligono:
            caminho_vertices = '|'.join([f'{lat},{lon}' for lat, lon in vertices_poligono])
            if vertices_poligono:
                caminho_vertices += f'|{vertices_poligono[0][0]},{vertices_poligono[0][1]}'
            
            params['path'] = f'color:0xFF0000FF|weight:2|fillcolor:0xFF000033|{caminho_vertices}'
        
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
        
        return urls

    # ========================================================================
    # PARTE 2: SALVAMENTO DE IMAGENS (Ex-ImagemSalvamentoService)
    # ========================================================================
    
    def salvar_imagem_google_maps(self,
                                  subestacao_id: int,
                                  transformador_id: int,
                                  url: str,
                                  latitude: float,
                                  longitude: float,
                                  zoom: int = 19,
                                  largura: int = 640,
                                  altura: int = 640,
                                  vertices_poligono: Optional[list] = None,
                                  resolucao_m: float = 1.0) -> Dict[str, Any]:
        """
        Salva imagem do Google Maps na tabela satelite_imagens
        
        Args:
            subestacao_id: ID da subestação
            transformador_id: ID do transformador
            url: URL da imagem Google Maps
            latitude: Latitude do centro
            longitude: Longitude do centro
            zoom: Nível de zoom (máximo=19)
            largura: Largura em pixels
            altura: Altura em pixels
            vertices_poligono: Vértices da área poligonal (se houver)
            resolucao_m: Resolução em metros/pixel
            
        Returns:
            Dicionário com resultado do salvamento
        """
        
        if not self.engine:
            logger.error("Engine não configurado para salvamento de imagens")
            return {'sucesso': False, 'erro': 'Engine não configurado'}
        
        try:
            logger.info(f"[GOOGLE MAPS] Salvando imagem para subestação {subestacao_id}")
            
            # Baixar imagem para cálculos
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Erro ao baixar imagem: HTTP {response.status_code}")
            
            img = Image.open(io.BytesIO(response.content))
            
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            img_array = np.array(img)
            
            # Calcular bbox
            bbox = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [longitude - 0.01, latitude - 0.01],
                        [longitude + 0.01, latitude - 0.01],
                        [longitude + 0.01, latitude + 0.01],
                        [longitude - 0.01, latitude + 0.01],
                        [longitude - 0.01, latitude - 0.01]
                    ]]
                }
            }
            
            # Metadados
            propriedades = {
                'transformador_id': transformador_id,
                'zoom': zoom,
                'largura_pixels': largura,
                'altura_pixels': altura,
                'bandas': 3,
                'nome_bandas': ['Red', 'Green', 'Blue'],
                'resolucao_m_pixel': resolucao_m,
                'area_poligonal': 'sim' if vertices_poligono else 'nao',
                'num_vertices_poligono': len(vertices_poligono) if vertices_poligono else 0,
                'rgb_medio': {
                    'r': float(np.mean(img_array[:, :, 0])),
                    'g': float(np.mean(img_array[:, :, 1])),
                    'b': float(np.mean(img_array[:, :, 2]))
                },
                'tamanho_arquivo_bytes': len(response.content)
            }
            
            # Salvar na tabela
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO satelite_imagens (
                        subestacao_id,
                        sensor,
                        data_aquisicao,
                        resolucao_m,
                        url,
                        bbox_json,
                        propriedades_json
                    ) VALUES (
                        :sub_id,
                        :sensor,
                        :data_aquisicao,
                        :resolucao_m,
                        :url,
                        :bbox,
                        :propriedades
                    )
                    RETURNING id;
                """)
                
                result = conn.execute(query, {
                    'sub_id': subestacao_id,
                    'sensor': 'Google_Maps_Satellite',
                    'data_aquisicao': datetime.now(),
                    'resolucao_m': int(resolucao_m),
                    'url': url,
                    'bbox': json.dumps(bbox),
                    'propriedades': json.dumps(propriedades)
                })
                
                imagem_id = result.fetchone()[0]
                conn.commit()
                
                logger.info(f"✓ Google Maps salva com ID {imagem_id}")
                
                return {
                    'sucesso': True,
                    'imagem_id': imagem_id,
                    'sensor': 'Google_Maps_Satellite',
                    'transformador_id': transformador_id,
                    'resolucao_m': int(resolucao_m),
                    'propriedades': propriedades
                }
        
        except Exception as e:
            logger.error(f"✗ Erro ao salvar Google Maps: {e}")
            return {
                'sucesso': False,
                'erro': str(e)
            }
    
    def salvar_imagem_cbers4a(self,
                              subestacao_id: int,
                              transformador_id: int,
                              imagem_id_cbers: int,
                              urls_bandas: Dict[str, str],
                              latitude: float,
                              longitude: float,
                              vertices_poligono: Optional[list] = None,
                              resolucao_m: float = 2.0) -> Dict[str, Any]:
        """
        Salva referência de imagem CBERS-4A com bandas espectrais
        
        Args:
            subestacao_id: ID da subestação
            transformador_id: ID do transformador
            imagem_id_cbers: ID da imagem CBERS-4A
            urls_bandas: Dicionário com URLs das 5 bandas
            latitude: Latitude
            longitude: Longitude
            vertices_poligono: Vértices da área poligonal
            resolucao_m: Resolução (2m para CBERS-4A)
            
        Returns:
            Dicionário com resultado
        """
        
        if not self.engine:
            logger.error("Engine não configurado para salvamento de imagens")
            return {'sucesso': False, 'erro': 'Engine não configurado'}
        
        try:
            logger.info(f"[CBERS-4A] Registrando referência para subestação {subestacao_id}")
            
            propriedades = {
                'transformador_id': transformador_id,
                'imagem_id_cbers': imagem_id_cbers,
                'resolucao_m_pixel': resolucao_m,
                'bandas': 5,
                'nome_bandas': ['Blue (B0)', 'Green (B1)', 'Red (B2)', 'NIR (B3)', 'SWIR (B4)'],
                'comprimentos_onda_nm': {
                    'B0': '0.45-0.52 (Azul)',
                    'B1': '0.52-0.59 (Verde)',
                    'B2': '0.63-0.69 (Vermelho)',
                    'B3': '0.77-0.89 (Infravermelho próximo)',
                    'B4': '1.55-1.75 (Infravermelho de onda curta)'
                },
                'urls_bandas': urls_bandas,
                'area_poligonal': 'sim' if vertices_poligono else 'nao',
                'num_vertices_poligono': len(vertices_poligono) if vertices_poligono else 0,
                'tipo_composicao': 'RGB_3bandas (B2-B1-B0)',
                'fonte_dados': 'INPE - Instituto Nacional de Pesquisas Espaciais'
            }
            
            bbox = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [longitude - 0.05, latitude - 0.05],
                        [longitude + 0.05, latitude - 0.05],
                        [longitude + 0.05, latitude + 0.05],
                        [longitude - 0.05, latitude + 0.05],
                        [longitude - 0.05, latitude - 0.05]
                    ]]
                }
            }
            
            # Salvar na tabela
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO satelite_imagens (
                        subestacao_id,
                        sensor,
                        data_aquisicao,
                        resolucao_m,
                        url,
                        bbox_json,
                        propriedades_json
                    ) VALUES (
                        :sub_id,
                        :sensor,
                        :data_aquisicao,
                        :resolucao_m,
                        :url,
                        :bbox,
                        :propriedades
                    )
                    RETURNING id;
                """)
                
                url_principal = urls_bandas.get('red', '')
                
                result = conn.execute(query, {
                    'sub_id': subestacao_id,
                    'sensor': 'CBERS-4A_Multibanda',
                    'data_aquisicao': datetime.now(),
                    'resolucao_m': int(resolucao_m),
                    'url': url_principal,
                    'bbox': json.dumps(bbox),
                    'propriedades': json.dumps(propriedades)
                })
                
                imagem_id = result.fetchone()[0]
                conn.commit()
                
                logger.info(f"✓ CBERS-4A salva com ID {imagem_id} (5 bandas espectrais)")
                
                return {
                    'sucesso': True,
                    'imagem_id': imagem_id,
                    'sensor': 'CBERS-4A_Multibanda',
                    'transformador_id': transformador_id,
                    'resolucao_m': int(resolucao_m),
                    'bandas': 5,
                    'propriedades': propriedades
                }
        
        except Exception as e:
            logger.error(f"✗ Erro ao salvar CBERS-4A: {e}")
            return {
                'sucesso': False,
                'erro': str(e)
            }

    # ========================================================================
    # PARTE 3: ESTRATÉGIA HÍBRIDA COM FALLBACK (Ex-ImagemStrategyService)
    # ========================================================================
    
    def buscar_imagem_automatica(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5.0,
        usar_cache: bool = True,
        estrategia: str = "auto"
    ) -> Optional[ImagemObtida]:
        """
        Busca imagem com fallback automático
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            raio_km: Raio de busca (para CBERS)
            usar_cache: Usar cache se disponível
            estrategia: "auto", "alta_resolucao", "custo_zero", "rapido"
        
        Returns:
            ImagemObtida ou None se todas as fontes falharam
        """
        logger.info(f"Buscando imagem com estratégia '{estrategia}'")
        logger.info(f"  Localização: ({latitude}, {longitude})")
        logger.info(f"  Raio: {raio_km} km")
        
        ordem = self._definir_ordem_tentativa(estrategia)
        
        for fonte in ordem:
            logger.info(f"\n→ Tentando fonte: {fonte}")
            self.stats[fonte]["tentativas"] += 1
            
            try:
                resultado = self._buscar_de_fonte(
                    fonte=fonte,
                    latitude=latitude,
                    longitude=longitude,
                    raio_km=raio_km,
                    usar_cache=usar_cache
                )
                
                if resultado:
                    self.stats[fonte]["sucessos"] += 1
                    logger.info(f"✓ Imagem obtida de {fonte}")
                    logger.info(f"  Resolução: {resultado.resolucao_m}m/pixel")
                    logger.info(f"  Shape: {resultado.imagem.shape}")
                    return resultado
                else:
                    logger.warning(f"✗ {fonte} não retornou imagem")
                    
            except Exception as e:
                logger.error(f"✗ Erro ao buscar de {fonte}: {e}")
        
        logger.error("✗ FALHA: Nenhuma fonte de imagem disponível")
        self._log_estatisticas()
        return None
    
    def _definir_ordem_tentativa(self, estrategia: str) -> List[str]:
        """Define ordem de tentativa baseado na estratégia"""
        
        if estrategia == "alta_resolucao":
            if self.google_maps and self.google_maps_api_key:
                return ["google_maps", "cbers", "sentinel"]
            else:
                return ["cbers", "sentinel"]
        
        elif estrategia == "custo_zero":
            return ["cbers", "sentinel"]
        
        elif estrategia == "rapido":
            return ["cbers", "google_maps", "sentinel"]
        
        else:  # "auto"
            if self.preferencia_resolucao <= 1.0:
                if self.google_maps and self.google_maps_api_key:
                    return ["google_maps", "cbers", "sentinel"]
                else:
                    return ["cbers", "sentinel"]
            elif self.preferencia_resolucao <= 5.0:
                return ["cbers", "google_maps", "sentinel"]
            else:
                return ["cbers", "sentinel", "google_maps"]
    
    def _buscar_de_fonte(
        self,
        fonte: str,
        latitude: float,
        longitude: float,
        raio_km: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem de uma fonte específica"""
        
        if fonte == "cbers":
            return self._buscar_cbers(latitude, longitude, raio_km, usar_cache)
        elif fonte == "google_maps":
            return self._buscar_google_maps(latitude, longitude, usar_cache)
        elif fonte == "sentinel":
            return self._buscar_sentinel(latitude, longitude, usar_cache)
        else:
            logger.error(f"Fonte desconhecida: {fonte}")
            return None
    
    def _buscar_cbers(
        self,
        latitude: float,
        longitude: float,
        raio_km: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem CBERS-4A"""
        
        if not self.cbers:
            return None
        
        data_fim = datetime.now().strftime("%Y-%m-%d")
        data_inicio = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        
        logger.info(f"  Buscando CBERS (período: {data_inicio} a {data_fim})")
        
        imagens = self.cbers.buscar_cbers4a_coordenadas(
            latitude=latitude,
            longitude=longitude,
            raio_km=raio_km,
            cobertura_nuvem_max=50.0
        )
        
        if not imagens:
            logger.warning(f"  Nenhuma imagem CBERS encontrada")
            return None
        
        imagem_cbers = imagens[0]
        logger.info(f"  Imagem encontrada: {imagem_cbers.id}")
        logger.info(f"    Data: {imagem_cbers.data}")
        logger.info(f"    Nuvens: {imagem_cbers.cobertura_nuvem}%")
        
        rgb = self.cbers.criar_composicao_rgb(imagem_cbers)
        
        if rgb is None:
            return None
        
        return ImagemObtida(
            fonte="cbers",
            imagem=np.array(rgb),
            resolucao_m=2.0,
            latitude=latitude,
            longitude=longitude,
            timestamp=imagem_cbers.data,
            metadata={
                "id": imagem_cbers.id,
                "sensor": imagem_cbers.sensor,
                "nuvens": imagem_cbers.cobertura_nuvem
            }
        )
    
    def _buscar_google_maps(
        self,
        latitude: float,
        longitude: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem Google Maps"""
        
        if not self.google_maps or not self.google_maps_api_key:
            logger.warning("  Google Maps API key não configurada")
            return None
        
        zoom = 20
        logger.info(f"  Buscando Google Maps (zoom={zoom})")
        
        imagem = self.google_maps.buscar_imagem_satelite(
            latitude=latitude,
            longitude=longitude,
            zoom=zoom,
            tamanho=(640, 640)
        )
        
        if imagem is None:
            return None
        
        return ImagemObtida(
            fonte="google_maps",
            imagem=imagem,
            resolucao_m=0.3,
            latitude=latitude,
            longitude=longitude,
            timestamp=datetime.now(),
            metadata={
                "zoom": zoom,
                "api": "google_maps_static"
            }
        )
    
    def _buscar_sentinel(
        self,
        latitude: float,
        longitude: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem Sentinel-2 (fallback final)"""
        
        logger.warning("  Sentinel-2 não implementado nesta estratégia")
        logger.warning("  Resolução 10m inadequada para telhados")
        return None
    
    def _log_estatisticas(self):
        """Log de estatísticas de uso"""
        logger.info("\n" + "="*60)
        logger.info("ESTATÍSTICAS DE USO")
        logger.info("="*60)
        
        for fonte, stats in self.stats.items():
            tentativas = stats["tentativas"]
            sucessos = stats["sucessos"]
            taxa = (sucessos / tentativas * 100) if tentativas > 0 else 0
            
            logger.info(f"{fonte.upper():<15} {tentativas:>3} tentativas | {sucessos:>3} sucessos | {taxa:>5.1f}%")


# ============================================================================
# ALIASES DE COMPATIBILIDADE (para código antigo)
# ============================================================================

# Manter nomes antigos como aliases para retrocompatibilidade
ImagemMultiFonteService = ImageService
ImagemSalvamentoService = ImageService
ImagemStrategyService = ImageService


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def criar_servico_imagem(engine: Optional[Engine] = None,
                        google_maps_api_key: Optional[str] = None) -> ImageService:
    """Factory para criar serviço de imagem"""
    return ImageService(engine=engine, google_maps_api_key=google_maps_api_key)
