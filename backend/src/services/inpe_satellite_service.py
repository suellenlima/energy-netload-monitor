"""
Serviço para integração com INPE e obtenção de imagens de satélite de áreas de subestações.

Fornece funcionalidades para:
- Detectar coordenadas da área de uma subestação
- Calcular bounding boxes ao redor da subestação
- Consultar APIs do INPE (Open Data Cube, Landsat, Sentinel-2)
- Processar e armazenar URLs/metadados de imagens
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import table_exists


@dataclass
class BoundingBox:
    """Representa uma bounding box geográfica."""
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    
    @property
    def center_lat(self) -> float:
        """Latitude do centro."""
        return (self.min_lat + self.max_lat) / 2
    
    @property
    def center_lon(self) -> float:
        """Longitude do centro."""
        return (self.min_lon + self.max_lon) / 2
    
    @property
    def width_km(self) -> float:
        """Largura aproximada em km."""
        # Aproximação simplificada
        return (self.max_lon - self.min_lon) * 111.0
    
    @property
    def height_km(self) -> float:
        """Altura aproximada em km."""
        # Aproximação simplificada
        return (self.max_lat - self.min_lat) * 110.567
    
    def to_wgs84_string(self) -> str:
        """Retorna bbox em formato WGS84."""
        return f"{self.min_lon},{self.min_lat},{self.max_lon},{self.max_lat}"
    
    def to_geojson(self) -> Dict:
        """Retorna bbox como GeoJSON."""
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
    """Metadados de imagem de satélite."""
    id: str
    data_aquisicao: datetime
    sensor: str  # Landsat, Sentinel-2, etc.
    resolucao_m: int  # Resolução em metros
    cobertura_nuvem_pct: float
    url: str
    bounding_box: BoundingBox
    propriedades: Dict


class INPESatelliteService:
    """Serviço para gerenciar consultas e cache de imagens de satélite INPE."""
    
    # Endpoints públicos INPE/OGC
    INPE_OGC_WMS = "https://terrabrasilis.dpi.inpe.br/geoserver/ows"
    INPE_OGC_WMSCS = "https://terrabrasilis.dpi.inpe.br/geoserver/wms"
    
    # Endpoints Landsat (via AWS ou USGS)
    LANDSAT_SEARCHSTAC = "https://landsatlook.usgs.gov/stac-server"
    LANDSAT_API = "https://rstac.cr.usgs.gov"
    
    # Endpoints Sentinel-2 (via Copernicus)
    SENTINEL_STAC = "https://planetarycomputer.microsoft.com/api/stac/v1"
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Inicializa o serviço."""
        self.logger = logger or logging.getLogger(__name__)
    
    def calcular_bbox_subestacao(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5.0
    ) -> BoundingBox:
        """
        Calcula bounding box ao redor de uma subestação.
        
        Args:
            latitude: Latitude da subestação
            longitude: Longitude da subestação
            raio_km: Raio de cobertura em km (default 5 km)
        
        Returns:
            BoundingBox calculada
        """
        # Conversão aproximada: 1 grau ≈ 111 km
        delta_lat = (raio_km / 111.0)
        delta_lon = (raio_km / (111.0 * np.cos(np.radians(latitude))))
        
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
    
    def construir_url_wms_terrabrasilis(
        self,
        bbox: BoundingBox,
        camada: str = "prodes",
        largura_px: int = 512,
        altura_px: int = 512,
        formato: str = "image/geotiff"
    ) -> str:
        """
        Constrói URL WMS para consultar imagem no Terrabrasilis.
        
        Args:
            bbox: Bounding box da área
            camada: Nome da camada (prodes, alerts, etc.)
            largura_px: Largura da imagem em pixels
            altura_px: Altura da imagem em pixels
            formato: Formato da imagem (image/geotiff, image/png, etc.)
        
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
    
    def gerar_url_sentinel2_stac(
        self,
        bbox: BoundingBox,
        data_inicio: datetime,
        data_fim: datetime,
        cobertura_nuvem_max_pct: float = 50.0
    ) -> Dict[str, str]:
        """
        Gera URLs STAC para consultar Sentinel-2 no Planetary Computer.
        
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
        
        self.logger.debug(
            f"STAC search preparada para {len(payload)} coleções"
        )
        
        return {
            "url": stac_search_url,
            "payload": payload,
            "method": "POST"
        }
    
    def gerar_url_landsat_stac(
        self,
        bbox: BoundingBox,
        data_inicio: datetime,
        data_fim: datetime,
        cobertura_nuvem_max_pct: float = 50.0
    ) -> Dict[str, str]:
        """
        Gera URLs STAC para consultar Landsat 8/9.
        
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
        
        self.logger.debug(
            f"STAC Landsat search preparada para bbox"
        )
        
        return {
            "url": stac_search_url,
            "payload": payload,
            "method": "POST"
        }
    
    def consultar_subestacao_satellite_data(
        self,
        engine: Engine,
        subestacao_id: int,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        raio_km: float = 5.0,
        sensores: List[str] = None
    ) -> Dict:
        """
        Consulta dados de satélite disponíveis para uma subestação.
        
        Args:
            engine: Engine SQLAlchemy
            subestacao_id: ID da subestação
            data_inicio: Data inicial (default: últimos 30 dias)
            data_fim: Data final (default: hoje)
            raio_km: Raio de cobertura em km
            sensores: Lista de sensores (Sentinel-2, Landsat, etc.)
        
        Returns:
            Dicionário com URLs e metadados disponíveis
        """
        if sensores is None:
            sensores = ["Sentinel-2", "Landsat"]
        
        if data_fim is None:
            data_fim = datetime.now()
        if data_inicio is None:
            data_inicio = data_fim - timedelta(days=30)
        
        # Buscar subestação no banco
        query = text("""
            SELECT id, nome, latitude, longitude, distribuidora 
            FROM subestacoes_detectadas 
            WHERE id = :id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"id": subestacao_id}).fetchone()
        
        if not result:
            self.logger.warning(f"Subestação ID {subestacao_id} não encontrada")
            return {"erro": "Subestação não encontrada"}
        
        subestacao = {
            "id": result[0],
            "nome": result[1],
            "latitude": result[2],
            "longitude": result[3],
            "distribuidora": result[4]
        }
        
        # Calcular bbox
        bbox = self.calcular_bbox_subestacao(
            subestacao["latitude"],
            subestacao["longitude"],
            raio_km
        )
        
        # Preparar respostas
        resultado = {
            "subestacao": subestacao,
            "bbox": {
                "min_lat": bbox.min_lat,
                "max_lat": bbox.max_lat,
                "min_lon": bbox.min_lon,
                "max_lon": bbox.max_lon,
                "center": {
                    "latitude": bbox.center_lat,
                    "longitude": bbox.center_lon
                },
                "dimensoes": {
                    "largura_km": bbox.width_km,
                    "altura_km": bbox.height_km
                }
            },
            "periodo": {
                "data_inicio": data_inicio.isoformat(),
                "data_fim": data_fim.isoformat()
            },
            "urls_consulta": {}
        }
        
        # Adicionar URLs por sensor
        if "Sentinel-2" in sensores:
            resultado["urls_consulta"]["sentinel2"] = self.gerar_url_sentinel2_stac(
                bbox, data_inicio, data_fim
            )
        
        if "Landsat" in sensores:
            resultado["urls_consulta"]["landsat"] = self.gerar_url_landsat_stac(
                bbox, data_inicio, data_fim
            )
        
        # Terrabrasilis WMS (sempre disponível)
        resultado["urls_consulta"]["terrabrasilis_wms"] = {
            "url": self.construir_url_wms_terrabrasilis(bbox),
            "camadas_disponiveis": [
                "prodes",  # Alertas PRODES
                "deter",   # DETER
                "alerts"   # Alertas em tempo real
            ]
        }
        
        self.logger.info(
            f"Consulta satellite data preparada para subestação "
            f"{subestacao['nome']} (ID: {subestacao_id})"
        )
        
        return resultado
    
    def armazenar_metadata_imagem(
        self,
        engine: Engine,
        subestacao_id: int,
        metadata: SatelliteMetadata
    ) -> bool:
        """
        Armazena metadados de imagem de satélite no banco.
        
        NOTA: As tabelas satelite_imagens e satelite_bandas devem ser criadas previamente
        usando o schema SQL em: infrastructure/database/001_satelite_tables.sql
        
        Args:
            engine: Engine SQLAlchemy
            subestacao_id: ID da subestação
            metadata: Metadados da imagem
        
        Returns:
            ID da imagem armazenada, ou None se houver erro
        """
        try:
            import json
            
            with engine.begin() as conn:
                # Inserir registro de imagem
                print(f"[DEBUG] Preparando INSERT para imagem {metadata.id}...")
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
                
                print(f"[DEBUG] Executando INSERT...")
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
                print(f"[DEBUG] ✅ INSERT sucesso: imagem_id={imagem_id}")
            
            self.logger.info(
                f"Imagem {metadata.id} armazenada para subestação {subestacao_id} (ID: {imagem_id})"
            )
            return imagem_id
        
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            self.logger.error(f"❌ ERRO ao armazenar metadata: {e}", exc_info=True)
            print(f"\n{'='*80}")
            print(f"ERROR em armazenar_metadata_imagem:")
            print(f"{tb_str}")
            print(f"{'='*80}\n")
            return None
    
    def listar_imagens_subestacao(
        self,
        engine: Engine,
        subestacao_id: int,
        limite: int = 50,
        ordenar_por: str = "data_aquisicao"
    ) -> List[Dict]:
        """
        Lista imagens de satélite registradas para uma subestação.
        
        Args:
            engine: Engine SQLAlchemy
            subestacao_id: ID da subestação
            limite: Máximo de registros
            ordenar_por: Campo para ordenação
        
        Returns:
            Lista de dicionários com metadados
        """
        if not table_exists("satelite_imagens", engine):
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
            with engine.connect() as conn:
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
            self.logger.error(f"Erro ao listar imagens: {e}", exc_info=True)
            return []
