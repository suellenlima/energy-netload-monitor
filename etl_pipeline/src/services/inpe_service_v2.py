"""
INPEService v2 - Buscar Imagens CBERS-4A do INPE

Melhorias:
- Busca por POLÍGONO (área de cobertura) em vez de raio
- Integração com SatelliteSourceService para tracking
- Prioridade: CBERS-4A (gratuito, sem limite)
- Resolução: 2 metros (banda PAN)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy.engine import Engine

try:
    from pystac_client import Client
except ImportError:
    Client = None

logger = logging.getLogger(__name__)


class INPEServiceV2:
    """
    Serviço INPE v2 para buscar imagens CBERS-4A com busca por polígono
    """
    
    def __init__(self, engine: Engine, satellite_source_service=None):
        """
        Inicializa serviço INPE
        
        Args:
            engine: SQLAlchemy engine
            satellite_source_service: SatelliteSourceService para tracking
        """
        self.engine = engine
        self.satellite_source_service = satellite_source_service
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
    # BUSCA POR TRANSFORMADOR (NOVO!)
    # ========================================================================
    
    def buscar_imagens_cbers4a_transformador(
        self,
        transformador_id: int,
        data_inicio: str = None,
        data_fim: str = None,
        cobertura_nuvem_max: int = 30,
        raio_km: float = 2.0,
        max_imagens: int = 50
    ) -> Dict:
        """
        Busca imagens CBERS-4A usando coordenadas do transformador
        
        Args:
            transformador_id: ID do transformador
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            cobertura_nuvem_max: % máximo de nuvens
            raio_km: Raio de busca em km (default: 2km = ~220m)
            max_imagens: Máximo de imagens a retornar
        
        Returns:
            {
                'fonte': 'CBERS-4A',
                'transformador_id': int,
                'imagens_encontradas': int,
                'imagens': [ lista ],
                'bbox': (min_lat, min_lon, max_lat, max_lon),
                'status': 'sucesso' ou 'erro'
            }
        """
        logger.info(f"🛰️ Buscando imagens CBERS-4A por TRANSFORMADOR ID {transformador_id}")
        
        # 1. Obter coordenadas do transformador
        trans_coords = self._obter_coordenadas_transformador(transformador_id)
        if not trans_coords:
            logger.warning(f"⚠️ Transformador {transformador_id} não encontrado")
            return self._registrar_resultado_erro_transformador(
                transformador_id,
                'sem_cobertura',
                'Transformador não encontrado'
            )
        
        latitude, longitude = trans_coords
        logger.info(f"   📍 Coordenadas: ({latitude}, {longitude})")
        
        # 2. Calcular bbox ao redor do transformador
        bbox = self._calcular_bbox_raio(latitude, longitude, raio_km)
        if not bbox:
            return self._registrar_resultado_erro_transformador(
                transformador_id,
                'erro',
                'Falha ao calcular bbox'
            )
        
        logger.info(f"   📏 Raio: {raio_km}km | Bbox: {bbox}")
        
        # 3. Datas
        if not data_fim:
            data_fim = datetime.now().strftime('%Y-%m-%d')
        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        logger.info(f"   📅 Período: {data_inicio} a {data_fim}")
        
        # 4. Buscar no STAC
        try:
            if not self.catalog:
                raise Exception("STAC não inicializado")
            
            search = self.catalog.search(
                collections=['CBERS-4A-WPM-L4-SR'],
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=max_imagens
            )
            
            items = list(search.get_items())
            logger.info(f"   ✅ {len(items)} imagens encontradas")
            
            # 5. Processar e filtrar
            imagens = []
            for item in items:
                cloud_cover = item.properties.get('eo:cloud_cover', 100)
                
                if cloud_cover <= cobertura_nuvem_max:
                    imagem_dict = {
                        'id': item.id,
                        'data': item.properties['datetime'],
                        'cobertura_nuvem_percent': cloud_cover,
                        'resolucao_metros': 2.0,
                        'sensor': 'CBERS-4A WPM',
                        'banda_pan': item.assets.get('pan', {}).get('href'),
                        'banda_red': item.assets.get('red', {}).get('href'),
                        'banda_green': item.assets.get('green', {}).get('href'),
                        'banda_blue': item.assets.get('blue', {}).get('href'),
                    }
                    imagens.append(imagem_dict)
            
            imagens.sort(key=lambda x: x['cobertura_nuvem_percent'])
            
            # 6. Registrar sucesso
            if self.satellite_source_service:
                self.satellite_source_service.registrar_requisicao_cbers4a(
                    subestacao_id=None,
                    tipo_requisicao=f'busca_transformador_{transformador_id}',
                    status='sucesso' if len(imagens) > 0 else 'sem_cobertura',
                    bbox=bbox,
                    imagem_id=f"CBERS-4A_T{transformador_id}_{len(imagens)}",
                    observacoes=f"Transformador {transformador_id}: {len(imagens)} imagens"
                )
            
            return {
                'fonte': 'CBERS-4A',
                'transformador_id': transformador_id,
                'imagens_encontradas': len(imagens),
                'imagens': imagens[:10],
                'bbox': bbox,
                'raio_km': raio_km,
                'periodo': f"{data_inicio} a {data_fim}",
                'resolucao_metros': 2.0,
                'status': 'sucesso' if len(imagens) > 0 else 'sem_cobertura'
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar CBERS-4A: {e}")
            return self._registrar_resultado_erro_transformador(
                transformador_id,
                'erro',
                f"Falha na busca: {str(e)}"
            )
    
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
                collections=['CBERS-4A-WPM-L4-SR'],  # CBERS-4A com resolução 2m
                bbox=bbox,
                datetime=f"{data_inicio}/{data_fim}",
                max_items=max_imagens
            )
            
            items = list(search.get_items())
            logger.info(f"   ✅ {len(items)} imagens encontradas")
            
            # Processar e filtrar
            imagens = []
            for item in items:
                cloud_cover = item.properties.get('eo:cloud_cover', 100)
                
                if cloud_cover <= cobertura_nuvem_max:
                    imagem_dict = {
                        'id': item.id,
                        'data': item.properties['datetime'],
                        'cobertura_nuvem_percent': cloud_cover,
                        'resolucao_metros': 2.0,
                        'sensor': 'CBERS-4A WPM',
                        'banda_pan': item.assets.get('pan', {}).get('href'),
                        'banda_red': item.assets.get('red', {}).get('href'),
                        'banda_green': item.assets.get('green', {}).get('href'),
                        'banda_blue': item.assets.get('blue', {}).get('href'),
                        'metadata': item.assets.get('MTL', {}).get('href'),
                    }
                    imagens.append(imagem_dict)
            
            # Ordenar por cobertura de nuvens
            imagens.sort(key=lambda x: x['cobertura_nuvem_percent'])
            
            # Registrar sucesso
            if self.satellite_source_service:
                self.satellite_source_service.registrar_requisicao_cbers4a(
                    subestacao_id=subestacao_id,
                    tipo_requisicao='busca_poligono',
                    status='sucesso' if len(imagens) > 0 else 'sem_cobertura',
                    bbox=bbox,
                    imagem_id=f"CBERS-4A_{len(imagens)}_imagens",
                    observacoes=f"{len(imagens)} imagens encontradas, nuvem<=30%"
                )
            
            return {
                'fonte': 'CBERS-4A',
                'subestacao_id': subestacao_id,
                'imagens_encontradas': len(imagens),
                'imagens': imagens[:10],  # Top 10 melhores
                'bbox': bbox,
                'periodo': f"{data_inicio} a {data_fim}",
                'resolucao_metros': 2.0,
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
            from sqlalchemy import text
            
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
    
    def _registrar_resultado_erro_transformador(
        self,
        transformador_id: int,
        status: str,
        observacoes: str
    ) -> Dict:
        """Registra resultado de erro para transformador"""
        if self.satellite_source_service:
            self.satellite_source_service.registrar_requisicao_cbers4a(
                subestacao_id=None,
                tipo_requisicao=f'busca_transformador_{transformador_id}',
                status=status,
                observacoes=observacoes
            )
        
        return {
            'fonte': 'CBERS-4A',
            'transformador_id': transformador_id,
            'imagens_encontradas': 0,
            'imagens': [],
            'status': status,
            'observacoes': observacoes
        }
    
    # ========================================================================
    # UTILIDADES - SUBESTAÇÃO
    # ========================================================================
    
    def _obter_poligono_cobertura(self, subestacao_id: int) -> Optional[str]:
        """Obtém WKT do polígono de cobertura da subestação"""
        try:
            from sqlalchemy import text
            
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
            from sqlalchemy import text
            
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
        if self.satellite_source_service:
            self.satellite_source_service.registrar_requisicao_cbers4a(
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
    # DOWNLOAD
    # ========================================================================
    
    def download_imagem_cbers4a(
        self,
        subestacao_id: int,
        imagem_id: str,
        banda: str = 'pan',
        destino: str = '/tmp'
    ) -> Optional[str]:
        """
        Download de banda específica da imagem CBERS-4A
        
        Args:
            subestacao_id: ID da subestação
            imagem_id: ID da imagem
            banda: 'pan' (2m), 'red', 'green', 'blue'
            destino: Diretório de destino
        
        Returns:
            Caminho do arquivo baixado
        """
        logger.info(f"📥 Baixando banda {banda} de {imagem_id}")
        
        try:
            import requests
            from pathlib import Path
            
            # Buscar URL da imagem (simplificado)
            # Em produção, buscaria do STAC
            logger.warning("⚠️ Download não implementado nesta versão")
            return None
            
        except Exception as e:
            logger.error(f"❌ Erro ao fazer download: {e}")
            return None
