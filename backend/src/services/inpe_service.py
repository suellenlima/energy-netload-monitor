"""
Serviço Unificado INPE - Integração com Satélites e Imagens Geoespaciais

Consolida três serviços em um único:
1. INPESatelliteService - Integração com APIs INPE, Landsat, Sentinel-2
2. INPEServiceV2 - Busca CBERS-4A por polígono e transformador
3. CBERSService - Acesso direto a imagens CBERS-4A do INPE

Características:
- Suporte a múltiplas fontes: CBERS-4A, Sentinel-2, Landsat
- Busca por coordenadas, BBOX e polígonos
- Integração com STAC (Space Time Asset Catalog)
- Metadados detalhados com cobertura de nuvens
- Rastreamento no banco de dados
- Composições RGB automatizadas

Author: Energy Netload Monitor
Date: 2026-02-04 (versão unificada)
"""

import logging
import os
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    import numpy as np
except ImportError:
    np = None

try:
    from pystac_client import Client
except ImportError:
    Client = None

try:
    from ..core import table_exists
except ImportError:
    def table_exists(table_name, engine):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
                return True
        except:
            return False

logger = logging.getLogger(__name__)


# ========================================================================
# MODELOS DE DADOS
# ========================================================================

@dataclass
class BoundingBox:
    """Representa uma bounding box geográfica"""
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    
    @property
    def center_lat(self) -> float:
        """Latitude do centro"""
        return (self.min_lat + self.max_lat) / 2
    
    @property
    def center_lon(self) -> float:
        """Longitude do centro"""
        return (self.min_lon + self.max_lon) / 2
    
    @property
    def width_km(self) -> float:
        """Largura aproximada em km"""
        return (self.max_lon - self.min_lon) * 111.0
    
    @property
    def height_km(self) -> float:
        """Altura aproximada em km"""
        return (self.max_lat - self.min_lat) * 110.567
    
    def to_wgs84_string(self) -> str:
        """Retorna bbox em formato WGS84"""
        return f"{self.min_lon},{self.min_lat},{self.max_lon},{self.max_lat}"
    
    def to_geojson(self) -> Dict:
        """Retorna bbox como GeoJSON"""
        return {
            "type": "Polygon",
            "coordinates": [[
                [self.min_lon, self.min_lat],
                [self.max_lon, self.min_lat],
                [self.max_lon, self.max_lat],
                [self.min_lon, self.max_lat],
                [self.min_lon, self.min_lat]
            ]]
        }


@dataclass
class SatelliteMetadata:
    """Metadados de imagem de satélite"""
    id: str
    data_aquisicao: datetime
    sensor: str
    resolucao_m: int
    cobertura_nuvem_pct: float
    url: str
    bounding_box: BoundingBox
    propriedades: Dict


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


# ========================================================================
# SERVIÇO UNIFICADO INPE
# ========================================================================

class INPEService:
    """
    Serviço unificado para gerenciar imagens de satélite de múltiplas fontes:
    - CBERS-4A (INPE/Brazil Data Cube)
    - Sentinel-2 (Copernicus)
    - Landsat (USGS)
    - Terrabrasilis (INPE WMS)
    
    Suporta buscas por:
    - Coordenadas (raio)
    - Polígonos (WKT)
    - BBOX direto
    - Transformador
    - Subestação
    """
    
    # Endpoints públicos INPE/OGC
    INPE_OGC_WMS = "https://terrabrasilis.dpi.inpe.br/geoserver/ows"
    INPE_OGC_WMSCS = "https://terrabrasilis.dpi.inpe.br/geoserver/wms"
    
    # Endpoints Landsat (via AWS ou USGS)
    LANDSAT_SEARCHSTAC = "https://landsatlook.usgs.gov/stac-server"
    LANDSAT_API = "https://rstac.cr.usgs.gov"
    
    # Endpoints Sentinel-2 (via Copernicus)
    SENTINEL_STAC = "https://planetarycomputer.microsoft.com/api/stac/v1"
    
    # Brazil Data Cube STAC (CBERS)
    STAC_URL = "https://data.inpe.br/bdc/stac/v1"
    
    def __init__(self, engine: Engine = None, satellite_service_v2=None):
        """
        Inicializa serviço unificado INPE
        
        Args:
            engine: SQLAlchemy engine para banco de dados
            satellite_service_v2: SatelliteServiceV2 para rastreamento (opcional)
        """
        self.engine = engine
        self.satellite_service_v2 = satellite_service_v2
        self.catalog = None
        self.logger = logging.getLogger(__name__)
        
        # Coleções CBERS disponíveis
        self.colecoes = {
            "CBERS-4A-WPM": "CBERS-4A-WPM-L4-SR",  # 2m resolução (PAN)
            "CBERS-4-MUX": "CBERS4-MUX-2M-1",      # ~20m resolução
            "CBERS-4-WFI": "CBERS4-WFI-16D-2"      # ~64m resolução
        }
        
        self.colecao_padrao = self.colecoes["CBERS-4A-WPM"]
        
        # Inicializar STAC se disponível
        self._inicializar_stac()
    
    def _inicializar_stac(self):
        """Inicializa conexão com STAC do INPE"""
        try:
            if Client is None:
                self.logger.warning("⚠️ pystac-client não instalado. Instale: pip install pystac-client")
                return
            
            self.catalog = Client.open(self.STAC_URL)
            self.logger.info("✅ Conectado ao STAC INPE")
        except Exception as e:
            self.logger.error(f"❌ Erro ao conectar STAC: {e}")
            self.catalog = None
    
    # ========================================================================
    # BUSCA CBERS-4A POR COORDENADAS
    # ========================================================================
    
    def buscar_cbers4a_coordenadas(
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
        Busca imagens CBERS-4A para uma localização (raio)
        
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
        if not self.catalog:
            self.logger.error("STAC não inicializado")
            return []
        
        try:
            # Usar coleção padrão se não especificada
            if not colecao:
                colecao = self.colecao_padrao
            
            # Calcular bounding box
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
            
            self.logger.info(f"🛰️ Buscando CBERS-4A para ({latitude}, {longitude})")
            self.logger.info(f"   Período: {data_inicio} a {data_fim}")
            self.logger.info(f"   Raio: {raio_km} km")
            self.logger.info(f"   Coleção: {colecao}")
            
            # Buscar no STAC
            search = self.catalog.search(
                collections=[colecao],
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=50
            )
            
            items = list(search.get_items())
            self.logger.info(f"   Encontradas {len(items)} imagens CBERS-4A")
            
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
            
            # Ordenar por cobertura de nuvens
            resultados.sort(key=lambda x: (x.cobertura_nuvem, -x.data.timestamp()))
            
            self.logger.info(f"   ✅ {len(resultados)} imagens após filtro")
            
            return resultados
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao buscar CBERS-4A: {e}")
            return []
    
    # ========================================================================
    # BUSCA CBERS-4A POR POLÍGONO
    # ========================================================================
    
    def buscar_cbers4a_poligono(
        self,
        subestacao_id: int,
        poligono_wkt: str = None,
        data_inicio: str = None,
        data_fim: str = None,
        cobertura_nuvem_max: int = 30,
        max_imagens: int = 50
    ) -> Dict:
        """
        Busca imagens CBERS-4A usando polígono de cobertura
        
        Args:
            subestacao_id: ID da subestação
            poligono_wkt: Polígono WKT da área de cobertura (se None, busca no BD)
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            cobertura_nuvem_max: % máximo de nuvens
            max_imagens: Máximo de imagens a retornar
        
        Returns:
            {
                'fonte': 'CBERS-4A',
                'subestacao_id': int,
                'imagens_encontradas': int,
                'imagens': [ lista de imagens ],
                'bbox': (min_lat, min_lon, max_lat, max_lon),
                'status': 'sucesso' ou 'erro'
            }
        """
        if not self.catalog:
            self.logger.error("STAC não inicializado")
            return self._registrar_resultado_erro(
                subestacao_id,
                'erro',
                'STAC não inicializado'
            )
        
        self.logger.info(f"🛰️ Buscando imagens CBERS-4A por POLÍGONO para SE {subestacao_id}")
        
        # Se não tiver WKT, buscar do banco
        if not poligono_wkt:
            poligono_wkt = self._obter_poligono_cobertura(subestacao_id)
        
        if not poligono_wkt:
            self.logger.warning(f"⚠️ Nenhum polígono encontrado para SE {subestacao_id}")
            return self._registrar_resultado_erro(
                subestacao_id,
                'sem_cobertura',
                'Polígono não encontrado'
            )
        
        # Calcular bounding box do polígono
        bbox = self._extrair_bbox_poligono(poligono_wkt)
        if not bbox:
            return self._registrar_resultado_erro(
                subestacao_id,
                'erro',
                'Falha ao calcular bbox do polígono'
            )
        
        # Datas
        if not data_fim:
            data_fim = datetime.now().strftime('%Y-%m-%d')
        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        self.logger.info(f"   📍 Bbox: {bbox}")
        self.logger.info(f"   📅 Período: {data_inicio} a {data_fim}")
        
        # Buscar no STAC
        try:
            search = self.catalog.search(
                collections=['CB4A-WPM-L4-DN-1'],  # CBERS-4A WPM
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=max_imagens
            )
            
            items = list(search.items())
            self.logger.info(f"   ✅ {len(items)} imagens encontradas")
            
            # Processar e filtrar
            imagens = []
            for item in items:
                cloud_cover = item.properties.get('eo:cloud_cover')
                if cloud_cover is None:
                    cloud_cover = 0
                else:
                    cloud_cover = float(cloud_cover)
                
                if cloud_cover <= cobertura_nuvem_max:
                    # Extrair assets
                    band_red = item.assets['BAND2'].href if 'BAND2' in item.assets else None
                    band_green = item.assets['BAND1'].href if 'BAND1' in item.assets else None
                    band_blue = item.assets['BAND0'].href if 'BAND0' in item.assets else None
                    band_nir = item.assets['BAND3'].href if 'BAND3' in item.assets else None
                    thumbnail_url = item.assets['thumbnail'].href if 'thumbnail' in item.assets else None
                    
                    best_url = band_nir or band_red or band_green or band_blue or thumbnail_url
                    
                    imagem_dict = {
                        'id': item.id,
                        'data': item.properties['datetime'],
                        'cobertura_nuvem_percent': cloud_cover,
                        'resolucao_metros': 2.0,
                        'sensor': 'CBERS-4A WPM',
                        'banda_pan': band_nir or best_url,
                        'banda_red': band_red or best_url,
                        'banda_green': band_green or best_url,
                        'banda_blue': band_blue or thumbnail_url,
                        'metadata': item.assets.get('MTL', None) if 'MTL' in item.assets else None,
                    }
                    imagens.append(imagem_dict)
            
            # Ordenar por cobertura de nuvens
            imagens.sort(key=lambda x: x['cobertura_nuvem_percent'])
            
            # Registrar sucesso
            if self.satellite_service_v2 and len(imagens) > 0:
                melhor_imagem = imagens[0]
                self.satellite_service_v2.registrar_requisicao_cbers4a(
                    subestacao_id=subestacao_id,
                    tipo_requisicao='busca_poligono',
                    status='sucesso' if len(imagens) > 0 else 'sem_cobertura',
                    bbox=bbox,
                    data_imagem=melhor_imagem.get('data'),
                    cobertura_nuvem=melhor_imagem.get('cobertura_nuvem_percent'),
                    imagem_id=melhor_imagem.get('id'),
                    url_download=melhor_imagem.get('banda_pan'),
                    observacoes=f"{len(imagens)} imagens encontradas"
                )
            
            return {
                'fonte': 'CBERS-4A',
                'subestacao_id': subestacao_id,
                'imagens_encontradas': len(imagens),
                'imagens': imagens[:10],
                'bbox': bbox,
                'periodo': f"{data_inicio} a {data_fim}",
                'resolucao_metros': 2.0,
                'status': 'sucesso' if len(imagens) > 0 else 'sem_cobertura'
            }
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao buscar CBERS-4A: {e}")
            return self._registrar_resultado_erro(
                subestacao_id,
                'erro',
                f"Falha na busca: {str(e)}"
            )
    
    # ========================================================================
    # BUSCA SENTINELA-2 POR BBOX
    # ========================================================================
    
    def gerar_url_sentinel2_stac(
        self,
        bbox: BoundingBox,
        data_inicio: datetime,
        data_fim: datetime,
        cobertura_nuvem_max_pct: float = 50.0
    ) -> Dict[str, str]:
        """
        Gera URLs STAC para consultar Sentinel-2 no Planetary Computer
        
        Args:
            bbox: Bounding box da área
            data_inicio: Data inicial
            data_fim: Data final
            cobertura_nuvem_max_pct: Máximo de cobertura de nuvem (%)
        
        Returns:
            Dicionário com URLs e parâmetros STAC
        """
        stac_search_url = f"{self.SENTINEL_STAC}/search"
        
        payload = {
            "bbox": [bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat],
            "datetime": f"{data_inicio.isoformat()}Z/{data_fim.isoformat()}Z",
            "query": {
                "eo:cloud_cover": {"lte": cobertura_nuvem_max_pct}
            },
            "collections": ["sentinel-2-l2a"],
            "limit": 10
        }
        
        self.logger.debug("STAC search preparada para Sentinel-2")
        
        return {
            "url": stac_search_url,
            "payload": payload,
            "method": "POST"
        }
    
    # ========================================================================
    # BUSCA LANDSAT POR BBOX
    # ========================================================================
    
    def gerar_url_landsat_stac(
        self,
        bbox: BoundingBox,
        data_inicio: datetime,
        data_fim: datetime,
        cobertura_nuvem_max_pct: float = 50.0
    ) -> Dict[str, str]:
        """
        Gera URLs STAC para consultar Landsat 8/9
        
        Args:
            bbox: Bounding box da área
            data_inicio: Data inicial
            data_fim: Data final
            cobertura_nuvem_max_pct: Máximo de cobertura de nuvem (%)
        
        Returns:
            Dicionário com URLs e parâmetros STAC
        """
        stac_search_url = f"{self.LANDSAT_API}/collections/landsat-c2-l2/items"
        
        payload = {
            "bbox": [bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat],
            "datetime": f"{data_inicio.isoformat()}Z/{data_fim.isoformat()}Z",
            "query": {
                "landsat:cloud_cover": {"lte": cobertura_nuvem_max_pct}
            },
            "limit": 10
        }
        
        self.logger.debug("STAC Landsat search preparada")
        
        return {
            "url": stac_search_url,
            "payload": payload,
            "method": "POST"
        }
    
    # ========================================================================
    # WMS TERRABRASILIS
    # ========================================================================
    
    def construir_url_wms_terrabrasilis(
        self,
        bbox: BoundingBox,
        camada: str = "prodes",
        largura_px: int = 512,
        altura_px: int = 512,
        formato: str = "image/geotiff"
    ) -> str:
        """
        Constrói URL WMS para consultar imagem no Terrabrasilis
        
        Args:
            bbox: Bounding box da área
            camada: Nome da camada (prodes, alerts, etc.)
            largura_px: Largura da imagem em pixels
            altura_px: Altura da imagem em pixels
            formato: Formato da imagem
        
        Returns:
            URL para download via WMS
        """
        params = {
            "service": "WMS",
            "version": "1.1.0",
            "request": "GetMap",
            "layers": camada,
            "styles": "",
            "bbox": bbox.to_wgs84_string(),
            "width": largura_px,
            "height": altura_px,
            "srs": "EPSG:4326",
            "format": formato,
            "exceptions": "application/vnd.ogc.se_xml"
        }
        
        url = self.INPE_OGC_WMS + "?" + "&".join(
            f"{k}={v}" for k, v in params.items()
        )
        
        self.logger.debug(f"URL WMS construída: {url[:100]}...")
        return url
    
    # ========================================================================
    # UTILITÁRIOS - SUBESTAÇÃO
    # ========================================================================
    
    def calcular_bbox_subestacao(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5.0
    ) -> BoundingBox:
        """
        Calcula bounding box ao redor de uma subestação
        
        Args:
            latitude: Latitude da subestação
            longitude: Longitude da subestação
            raio_km: Raio de cobertura em km (default 5 km)
        
        Returns:
            BoundingBox calculada
        """
        if np is None:
            # Aproximação simplificada sem numpy
            delta_lat = raio_km / 111.0
            delta_lon = raio_km / 111.0
        else:
            delta_lat = raio_km / 111.0
            delta_lon = raio_km / (111.0 * np.cos(np.radians(latitude)))
        
        bbox = BoundingBox(
            min_lat=latitude - delta_lat,
            max_lat=latitude + delta_lat,
            min_lon=longitude - delta_lon,
            max_lon=longitude + delta_lon
        )
        
        self.logger.debug(
            f"BBox calculada para ({latitude}, {longitude}): "
            f"lat [{bbox.min_lat:.4f}, {bbox.max_lat:.4f}], "
            f"lon [{bbox.min_lon:.4f}, {bbox.max_lon:.4f}]"
        )
        
        return bbox
    
    def _obter_poligono_cobertura(self, subestacao_id: int) -> Optional[str]:
        """Obtém WKT do polígono de cobertura da subestação"""
        if not self.engine:
            return None
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT ST_AsText(area_cobertura)
                    FROM subestacoes_area_cobertura
                    WHERE subestacao_id = :subestacao_id
                """), {'subestacao_id': subestacao_id})
                
                row = result.fetchone()
                if row and row[0]:
                    self.logger.info(f"   ✅ Polígono obtido do banco")
                    return row[0]
                else:
                    self.logger.warning(f"   ⚠️ Nenhum polígono encontrado")
                    return None
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter polígono: {e}")
            return None
    
    def _extrair_bbox_poligono(self, poligono_wkt: str) -> Optional[Tuple]:
        """
        Extrai bounding box de um polígono WKT
        
        Args:
            poligono_wkt: String WKT do polígono
        
        Returns:
            (min_lon, min_lat, max_lon, max_lat) para STAC
        """
        if not self.engine:
            return None
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        ST_XMin(geom) as min_lon,
                        ST_YMin(geom) as min_lat,
                        ST_XMax(geom) as max_lon,
                        ST_YMax(geom) as max_lat
                    FROM (SELECT ST_GeomFromText(:wkt, 4326) as geom) sub
                """), {'wkt': poligono_wkt})
                
                row = result.fetchone()
                if row:
                    return (row[0], row[1], row[2], row[3])
                else:
                    return None
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao extrair bbox: {e}")
            return None
    
    def _registrar_resultado_erro(
        self,
        subestacao_id: int,
        status: str,
        observacoes: str
    ) -> Dict:
        """Registra resultado de erro"""
        if self.satellite_service_v2:
            self.satellite_service_v2.registrar_requisicao_cbers4a(
                subestacao_id=subestacao_id,
                tipo_requisicao='busca_poligono',
                status=status,
                observacoes=observacoes
            )
        
        return {
            'fonte': 'CBERS-4A',
            'subestacao_id': subestacao_id,
            'imagens_encontradas': 0,
            'imagens': [],
            'status': status,
            'observacoes': observacoes
        }
    
    # ========================================================================
    # UTILITÁRIOS - ARMAZENAMENTO
    # ========================================================================
    
    def armazenar_metadata_imagem(
        self,
        subestacao_id: int,
        metadata: SatelliteMetadata
    ) -> bool:
        """
        Armazena metadados de imagem de satélite no banco
        
        Args:
            subestacao_id: ID da subestação
            metadata: Metadados da imagem
        
        Returns:
            ID da imagem armazenada, ou None se houver erro
        """
        if not self.engine:
            return False
        
        try:
            with self.engine.begin() as conn:
                insert_query = text("""
                    INSERT INTO satelite_imagens 
                    (subestacao_id, sensor, data_aquisicao, resolucao_m, 
                     cobertura_nuvem_pct, url, bbox_json, propriedades_json)
                    VALUES (:sub_id, :sensor, :data, :res, :nuvem, :url, 
                            :bbox, :props)
                    ON CONFLICT (subestacao_id, sensor, data_aquisicao) 
                    DO UPDATE SET
                        resolucao_m = :res,
                        cobertura_nuvem_pct = :nuvem,
                        url = :url,
                        bbox_json = :bbox,
                        propriedades_json = :props
                    RETURNING id
                """)
                
                result = conn.execute(insert_query, {
                    "sub_id": subestacao_id,
                    "sensor": metadata.sensor,
                    "data": metadata.data_aquisicao,
                    "res": metadata.resolucao_m,
                    "nuvem": metadata.cobertura_nuvem_pct,
                    "url": metadata.url,
                    "bbox": json.dumps(metadata.bounding_box.to_geojson()),
                    "props": json.dumps(metadata.propriedades) if metadata.propriedades else json.dumps({})
                })
                imagem_id = result.scalar()
                self.logger.info(
                    f"✅ Imagem {metadata.id} armazenada para SE {subestacao_id}"
                )
                return imagem_id
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao armazenar metadata: {e}")
            return False
    
    def listar_imagens_subestacao(
        self,
        subestacao_id: int,
        limite: int = 50,
        ordenar_por: str = "data_aquisicao"
    ) -> List[Dict]:
        """
        Lista imagens de satélite registradas para uma subestação
        
        Args:
            subestacao_id: ID da subestação
            limite: Máximo de registros
            ordenar_por: Campo para ordenação
        
        Returns:
            Lista de dicionários com metadados
        """
        if not self.engine or not table_exists("satelite_imagens", self.engine):
            return []
        
        query = text(f"""
            SELECT id, sensor, data_aquisicao, resolucao_m, 
                   cobertura_nuvem_pct, url, propriedades_json
            FROM satelite_imagens
            WHERE subestacao_id = :sub_id
            ORDER BY {ordenar_por} DESC
            LIMIT :limite
        """)
        
        try:
            with self.engine.connect() as conn:
                resultados = conn.execute(
                    query,
                    {"sub_id": subestacao_id, "limite": limite}
                ).fetchall()
            
            imagens = []
            for row in resultados:
                imagens.append({
                    "id": row[0],
                    "sensor": row[1],
                    "data_aquisicao": row[2].isoformat() if row[2] else None,
                    "resolucao_m": row[3],
                    "cobertura_nuvem_pct": row[4],
                    "url": row[5],
                    "propriedades": row[6] or {}
                })
            
            return imagens
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao listar imagens: {e}")
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
            from PIL import Image
            from io import BytesIO
            
            if np is None:
                raise ImportError("numpy não está instalado")
            
            self.logger.info("🎨 Criando composição RGB...")
            
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
            
            self.logger.info(f"✓ Composição RGB criada: {img.size}")
            
            return buffer.getvalue()
            
        except ImportError:
            self.logger.error("rasterio não está instalado")
            self.logger.info("Instale com: pip install rasterio pillow")
            raise
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar composição RGB: {e}")
            raise


# Aliases para compatibilidade com código antigo
INPESatelliteService = INPEService
INPEServiceV2 = INPEService
CBERSService = INPEService

__all__ = [
    'INPEService',
    'INPESatelliteService',
    'INPEServiceV2',
    'CBERSService',
    'BoundingBox',
    'SatelliteMetadata',
    'ImagemCBERS',
]
