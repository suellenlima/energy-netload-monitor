"""
INPEServiceV2 - Buscar Imagens CBERS-4A do INPE (Consolidado no Backend)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from pystac_client import Client
except ImportError:
    Client = None

logger = logging.getLogger(__name__)


class INPEServiceV2:
    """
    Serviço INPE v2 para buscar imagens CBERS-4A com busca por polígono e transformador
    """
    
    def __init__(self, engine: Engine, satellite_service_v2=None):
        """
        Inicializa serviço INPE
        
        Args:
            engine: SQLAlchemy engine
            satellite_service_v2: SatelliteServiceV2 para tracking
        """
        self.engine = engine
        self.satellite_service_v2 = satellite_service_v2
        self.stac_url = "https://data.inpe.br/bdc/stac/v1"
        self.catalog = None
        self._inicializar_stac()
    
    def _inicializar_stac(self):
        """Inicializa conexão com STAC do INPE"""
        try:
            if Client is None:
                logger.warning("⚠️ pystac-client não instalado. Instale: pip install pystac-client")
                return
            
            self.catalog = Client.open(self.stac_url)
            logger.info("✅ Conectado ao STAC INPE")
        except Exception as e:
            logger.error(f"❌ Erro ao conectar STAC: {e}")
            self.catalog = None
    
    # ========================================================================
    # BUSCA POR POLÍGONO
    # ========================================================================
    
    def buscar_imagens_cbers4a_poligono(
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
        logger.info(f"🛰️ Buscando imagens CBERS-4A por POLÍGONO para SE {subestacao_id}")
        
        # Se não tiver WKT, buscar do banco
        if not poligono_wkt:
            poligono_wkt = self._obter_poligono_cobertura(subestacao_id)
        
        if not poligono_wkt:
            logger.warning(f"⚠️ Nenhum polígono encontrado para SE {subestacao_id}")
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
        
        logger.info(f"   📍 Bbox: {bbox}")
        logger.info(f"   📅 Período: {data_inicio} a {data_fim}")
        
        # Buscar no STAC
        try:
            if not self.catalog:
                raise Exception("STAC não inicializado")
            
            search = self.catalog.search(
                collections=['CB4A-WPM-L4-DN-1'],  # CBERS-4A WPM Level-4 Digital Number (2m resoluçao)
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=max_imagens
            )
            
            items = list(search.items())
            logger.info(f"   ✅ {len(items)} imagens encontradas")
            
            # Processar e filtrar
            imagens = []
            for item in items:
                cloud_cover = item.properties.get('eo:cloud_cover')
                # Se cloud_cover eh None, assumir 0% (sem nuvem)
                if cloud_cover is None:
                    cloud_cover = 0
                else:
                    cloud_cover = float(cloud_cover)
                
                if cloud_cover <= cobertura_nuvem_max:
                    # Para CBERS-4A WPM, usar BAND0-4 (2m resoluçao)
                    band_red = None
                    band_green = None
                    band_blue = None
                    band_nir = None
                    thumbnail_url = None
                    
                    if 'BAND2' in item.assets:
                        band_red = item.assets['BAND2'].href
                    if 'BAND1' in item.assets:
                        band_green = item.assets['BAND1'].href
                    if 'BAND0' in item.assets:
                        band_blue = item.assets['BAND0'].href
                    if 'BAND3' in item.assets:
                        band_nir = item.assets['BAND3'].href
                    if 'thumbnail' in item.assets:
                        thumbnail_url = item.assets['thumbnail'].href
                    
                    best_url = band_red or band_nir or band_green or band_blue or thumbnail_url
                    
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
            if self.satellite_service_v2:
                # Extrair informações da melhor imagem (primeira, com menor cobertura de nuvem)
                melhor_imagem_url = None
                melhor_imagem_data = None
                melhor_imagem_nuvem = None
                if len(imagens) > 0:
                    melhor_imagem_url = imagens[0].get('banda_pan')  # URL do asset PAN
                    melhor_imagem_data = imagens[0].get('data')
                    melhor_imagem_nuvem = imagens[0].get('cobertura_nuvem_percent')
                
                self.satellite_service_v2.registrar_requisicao_cbers4a(
                    subestacao_id=subestacao_id,
                    tipo_requisicao='busca_poligono',
                    status='sucesso' if len(imagens) > 0 else 'sem_cobertura',
                    bbox=bbox,
                    data_imagem=melhor_imagem_data,
                    cobertura_nuvem=melhor_imagem_nuvem,
                    imagem_id=imagens[0].get('id') if len(imagens) > 0 else f"CBERS-4A_SE{subestacao_id}_0",
                    url_download=melhor_imagem_url,
                    observacoes=f"{len(imagens)} imagens encontradas, nuvem<=30%"
                )
            
            return {
                'fonte': 'CBERS-4A',
                'subestacao_id': subestacao_id,
                'imagens_encontradas': len(imagens),
                'imagens': imagens[:10],  # Top 10 melhores
                'bbox': bbox,
                'periodo': f"{data_inicio} a {data_fim}",
                'resolucao_metros': 56.0,
                'status': 'sucesso' if len(imagens) > 0 else 'sem_cobertura'
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar CBERS-4A: {e}")
            return self._registrar_resultado_erro(
                subestacao_id,
                'erro',
                f"Falha na busca: {str(e)}"
            )
    
    # ========================================================================
    # UTILIDADES - TRANSFORMADOR
    # ========================================================================
    
    def _obter_coordenadas_transformador(self, transformador_id: int) -> Optional[Tuple[float, float]]:
        """Obtém coordenadas (lat, lon) do transformador"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT latitude, longitude
                    FROM transformadores
                    WHERE id = :transformador_id
                    AND latitude IS NOT NULL AND longitude IS NOT NULL
                """), {'transformador_id': transformador_id})
                
                row = result.fetchone()
                if row:
                    logger.info(f"   ✅ Coordenadas obtidas do banco")
                    return (float(row[0]), float(row[1]))
                else:
                    logger.warning(f"   ⚠️ Transformador não encontrado ou sem coordenadas")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Erro ao obter coordenadas: {e}")
            return None
    
    def _calcular_bbox_raio(self, latitude: float, longitude: float, raio_km: float) -> Optional[Tuple]:
        """
        Calcula bbox (bounding box) ao redor de um ponto com raio
        
        Args:
            latitude: Latitude do ponto central
            longitude: Longitude do ponto central
            raio_km: Raio em km
        
        Returns:
            (min_lon, min_lat, max_lon, max_lat) para STAC
        """
        try:
            # 1 grau ≈ 111 km na linha do equador
            # Mais precisamente: varia com latitude
            delta = raio_km / 111.0
            
            min_lon = longitude - delta
            max_lon = longitude + delta
            min_lat = latitude - delta
            max_lat = latitude + delta
            
            logger.info(f"   📐 Delta: {delta:.4f}°")
            return (min_lon, min_lat, max_lon, max_lat)
            
        except Exception as e:
            logger.error(f"❌ Erro ao calcular bbox: {e}")
            return None
    
    def _obter_area_poligonal_transformador(self, transformador_id: int) -> float:
        """
        Obtém a área poligonal do transformador do banco de dados
        
        Prioridade:
        1. transformadores.area_poligonal_km
        2. transformadores_area_cobertura.area_poligonal_km
        3. Default: 1.0 km
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Área poligonal em km (float)
        """
        try:
            with self.engine.begin() as conn:
                # Tentativa 1: transformadores
                result = conn.execute(text("""
                    SELECT area_poligonal_km
                    FROM transformadores
                    WHERE id = :transformador_id
                    AND area_poligonal_km IS NOT NULL
                """), {'transformador_id': transformador_id})
                
                row = result.fetchone()
                if row and row[0]:
                    area_km = float(row[0])
                    logger.info(f"   ✅ Área poligonal obtida de transformadores: {area_km}km")
                    return area_km
                
                # Tentativa 2: transformadores_area_cobertura
                result = conn.execute(text("""
                    SELECT area_poligonal_km
                    FROM transformadores_area_cobertura
                    WHERE transformador_id = :transformador_id
                    AND area_poligonal_km IS NOT NULL
                """), {'transformador_id': transformador_id})
                
                row = result.fetchone()
                if row and row[0]:
                    area_km = float(row[0])
                    logger.info(f"   ✅ Área poligonal obtida de transformadores_area_cobertura: {area_km}km")
                    return area_km
                
                # Tentativa 3: Default
                logger.info(f"   ⚠️ Usando valor padrão de área poligonal: 2.0km")
                return 2.0
                
        except Exception as e:
            logger.error(f"❌ Erro ao obter área poligonal: {e}")
            return 2.0
    
    def _registrar_resultado_erro_transformador(
        self,
        transformador_id: int,
        status: str,
        observacoes: str
    ) -> Dict:
        """Registra resultado de erro para transformador"""
        # Obter a área poligonal atual
        area_poligonal = self._obter_area_poligonal_transformador(transformador_id)
        
        if self.satellite_service_v2:
            self.satellite_service_v2.registrar_requisicao_cbers4a(
                transformador_id=transformador_id,
                tipo_requisicao=f'busca_transformador_{transformador_id}',
                status=status,
                observacoes=observacoes
            )
        
        return {
            'fonte': 'CBERS-4A',
            'transformador_id': transformador_id,
            'imagens_encontradas': 0,
            'imagens': [],
            'bbox': (0.0, 0.0, 0.0, 0.0),
            'area_poligonal_km': area_poligonal,
            'periodo': 'N/A',
            'resolucao_metros': 2.0,
            'status': status,
            'observacoes': observacoes
        }
    
    # ========================================================================
    # UTILIDADES - SUBESTAÇÃO
    # ========================================================================
    
    def _obter_poligono_cobertura(self, subestacao_id: int) -> Optional[str]:
        """Obtém WKT do polígono de cobertura da subestação"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT ST_AsText(area_cobertura)
                    FROM subestacoes_area_cobertura
                    WHERE subestacao_id = :subestacao_id
                """), {'subestacao_id': subestacao_id})
                
                row = result.fetchone()
                if row and row[0]:
                    logger.info(f"   ✅ Polígono obtido do banco")
                    return row[0]
                else:
                    logger.warning(f"   ⚠️ Nenhum polígono encontrado no banco")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Erro ao obter polígono: {e}")
            return None
    
    def _extrair_bbox_poligono(self, poligono_wkt: str) -> Optional[Tuple]:
        """
        Extrai bounding box de um polígono WKT
        
        Args:
            poligono_wkt: String WKT do polígono
        
        Returns:
            (min_lon, min_lat, max_lon, max_lat) para STAC
        """
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
            logger.error(f"❌ Erro ao extrair bbox: {e}")
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
