"""
Google Maps Static API Service for Satellite Image Search
Busca de imagens estáticas do Google Maps para transformadores
Com rastreamento de requisições, gerenciamento de quota e suporte a área poligonal
"""

import json
import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
import requests
from urllib.parse import urlencode
from .google_maps_quota_service import GoogleMapsQuotaService

logger = logging.getLogger(__name__)


class GoogleMapsServiceV2:
    """
    Serviço para buscar imagens estáticas do Google Maps
    Especializado em busca por transformador com suporte a área poligonal customizável
    """
    
    def __init__(self, engine: Engine, api_key: str = None):
        """
        Inicializa serviço Google Maps
        
        Args:
            engine: SQLAlchemy engine para banco de dados
            api_key: Chave da API do Google Maps (pode vir de env ou config)
        """
        self.engine = engine
        self.api_key = api_key
        self.BASE_URL = "https://maps.googleapis.com/maps/api/staticmap"
        self.ZOOM_DEFAULT = 18
        self.SIZE_DEFAULT = "640x640"
        self.LIMIT_MENSAL = 25000
        self.quota_service = GoogleMapsQuotaService(engine)  # Inicializa serviço de quota
    
    # ========================================================================
    # CÁLCULO AUTOMÁTICO DE ZOOM
    # ========================================================================
    
    def calcular_zoom_area_poligonal(self, area_km: float) -> int:
        """
        Calcula automaticamente o nível de zoom baseado na área poligonal.
        
        Quanto maior a área, menor o zoom (mostra mais área).
        Quanto menor a área, maior o zoom (mostra mais detalhe).
        
        Tabela de referência (aproximada):
        - 0.1 km²  → zoom 19 (muito detalhado)
        - 1 km²    → zoom 17 (detalhado)
        - 5 km²    → zoom 16 (geral)
        - 10 km²   → zoom 15 (mais geral)
        - 50 km²   → zoom 13 (visão ampla)
        - 100 km²  → zoom 12 (visão bem ampla)
        
        Args:
            area_km: Área em km²
        
        Returns:
            Nível de zoom entre 10 e 20
        """
        import math
        
        if area_km <= 0:
            logger.warning(f"⚠️ Área inválida: {area_km} km², usando zoom padrão {self.ZOOM_DEFAULT}")
            return self.ZOOM_DEFAULT
        
        # Fórmula para calcular zoom baseado em área
        # Baseia-se em: zoom = 16 - log2(area_km / 0.3)
        # Ajustamos a constante 0.3 para calibrar os níveis desejados
        
        zoom = 16 - math.log2(area_km / 0.3)
        
        # Limitar zoom entre 10 e 20
        zoom = max(10, min(20, int(round(zoom))))
        
        logger.info(f"📊 Área poligonal: {area_km} km² → Zoom calculado: {zoom}")
        return zoom
    
    # ========================================================================
    # BUSCA DE IMAGENS - TRANSFORMADOR
    # ========================================================================
    
    def buscar_imagens_transformador(
        self,
        transformador_id: int,
        zoom: int = 18,
        tamanho: str = "640x640",
        auto_zoom: bool = True
    ) -> Dict:
        """
        Busca imagens de satélite do Google Maps para um transformador
        
        A área poligonal é obtida automaticamente do banco de dados.
        
        Args:
            transformador_id: ID do transformador
            zoom: Nível de zoom (10-20, default 18)
            tamanho: Tamanho da imagem em pixels (WIDTHxHEIGHT)
            auto_zoom: Se True, calcula zoom automaticamente pela área poligonal
        
        Returns:
            Dict com resultado da busca:
            {
                'sucesso': bool,
                'transformador_id': int,
                'imagens': [
                    {
                        'url': str,
                        'zoom': int,
                        'tipo': 'satellite',
                        'data_obtencao': datetime,
                        'fonte': 'GOOGLE_MAPS',
                        'tamanho_pixels': str,
                        'area_poligonal_km': float
                    }
                ],
                'motivo': str
            }
        """
        logger.info(f"🗺️  Buscando imagens Google Maps para TRANSFORMADOR {transformador_id}")
        
        # Validar API key
        if not self.api_key:
            logger.warning(f"   ⚠️ API key do Google Maps não configurada")
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'imagens': [],
                'motivo': 'API key não configurada'
            }
        
        # Buscar coordenadas e área poligonal do transformador
        coords = self._obter_coordenadas_transformador(transformador_id)
        if not coords:
            logger.error(f"   ❌ Transformador {transformador_id} não encontrado")
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'imagens': [],
                'motivo': f'Transformador {transformador_id} não encontrado'
            }
        
        logger.info(f"   📍 Nome: {coords.get('nome')}, Lat: {coords['latitude']}, Lon: {coords['longitude']}")
        
        # Obter área poligonal do banco de dados
        area_poligonal_km = self._obter_area_poligonal_transformador(transformador_id)
        logger.info(f"   📏 Área poligonal: {area_poligonal_km} km")
        
        # Gerenciar zoom automático
        if auto_zoom:
            # Calcular zoom automaticamente baseado na área poligonal
            zoom_calculado = self.calcular_zoom_area_poligonal(area_poligonal_km)
            zoom = zoom_calculado
            logger.info(f"   🔍 Usando zoom calculado automaticamente: {zoom}")
        else:
            # Validar zoom passado
            zoom = max(10, min(20, zoom))
            logger.info(f"   🔍 Usando zoom do request: {zoom}")
        
        
        
        try:
            # Construir URL
            url = self._construir_url_staticmap(
                lat=coords['latitude'],
                lon=coords['longitude'],
                zoom=zoom,
                tamanho=tamanho,
                tipos=['satellite', 'hybrid']
            )
            
            # Registrar requisição
            self._registrar_requisicao_google_maps(
                transformador_id=transformador_id,
                url=url,
                tipo='transformador'
            )
            
            # Contabilizar na quota do Google Maps (cada busca = 2 requisições: satellite + hybrid)
            subestacao_id = coords.get('subestacao_id', 1)
            self.quota_service.registrar_requisicao(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                tipo_requisicao='satellite',
                zoom=zoom,
                largura=int(tamanho.split('x')[0]) if 'x' in tamanho else 640,
                altura=int(tamanho.split('x')[1]) if 'x' in tamanho else 640,
                status='sucesso',
                url=url.get('satellite'),
                tempo_ms=0,
                codigo_resposta=200,
                custo_unidades=1.0,
                notas=f"Busca transformador ID={transformador_id}"
            )
            
            # Registrar também a requisição hybrid
            self.quota_service.registrar_requisicao(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                tipo_requisicao='hybrid',
                zoom=zoom,
                largura=int(tamanho.split('x')[0]) if 'x' in tamanho else 640,
                altura=int(tamanho.split('x')[1]) if 'x' in tamanho else 640,
                status='sucesso',
                url=url.get('hybrid'),
                tempo_ms=0,
                codigo_resposta=200,
                custo_unidades=1.0,
                notas=f"Busca transformador ID={transformador_id}"
            )
            
            logger.info(f"   ✅ URL gerada para transformador {transformador_id} e quota contabilizada")
            
            return {
                'sucesso': True,
                'transformador_id': transformador_id,
                'nome': coords.get('nome'),
                'latitude': coords['latitude'],
                'longitude': coords['longitude'],
                'imagens': [
                    {
                        'url': url['satellite'],
                        'zoom': zoom,
                        'tipo': 'satellite',
                        'data_obtencao': datetime.utcnow().isoformat(),
                        'fonte': 'GOOGLE_MAPS',
                        'tamanho_pixels': tamanho,
                        'area_poligonal_km': area_poligonal_km
                    },
                    {
                        'url': url['hybrid'],
                        'zoom': zoom,
                        'tipo': 'hybrid',
                        'data_obtencao': datetime.utcnow().isoformat(),
                        'fonte': 'GOOGLE_MAPS',
                        'tamanho_pixels': tamanho,
                        'area_poligonal_km': area_poligonal_km
                    }
                ],
                'motivo': 'Sucesso'
            }
            
        except Exception as exc:
            logger.error(f"   ❌ Erro ao buscar imagens Google Maps: {exc}", exc_info=True)
            self._registrar_resultado_erro_transformador(
                transformador_id=transformador_id,
                erro=str(exc),
                tipo='google_maps'
            )
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'imagens': [],
                'motivo': f'Erro ao gerar URLs: {str(exc)}'
            }
    
    # ========================================================================
    # BUSCA EM GRID - MÚLTIPLAS IMAGENS COM ZOOM MÁXIMO
    # ========================================================================
    
    def _calcular_grid_coordenadas(
        self,
        lat_centro: float,
        lon_centro: float,
        area_km: float,
        zoom_grid: int = 20,
        tamanho_imagem: str = "640x640"
    ) -> List[Dict]:
        """
        Calcula uma grade (grid) de coordenadas para cobrir uma área poligonal
        com múltiplas imagens em zoom máximo.
        
        A cobertura é calculada baseada no zoom e tamanho da imagem.
        
        Args:
            lat_centro: Latitude do centro
            lon_centro: Longitude do centro
            area_km: Área em km² para cobrir
            zoom_grid: Nível de zoom para o grid (default 20 - máximo)
            tamanho_imagem: Tamanho de cada imagem em pixels
        
        Returns:
            Lista de dicts com coordenadas de cada célula do grid
        """
        
        # Extrair dimensões da imagem
        largura_px, altura_px = map(int, tamanho_imagem.split('x'))
        
        # Calcular raio aproximado da área em km
        # area = π * r² → r = √(area/π)
        raio_km = math.sqrt(area_km / math.pi)
        
        logger.info(f"📐 Calculando grid: área={area_km} km², raio≈{raio_km:.2f} km, zoom={zoom_grid}")
        
        # Cobertura por imagem em km (aproximado para zoom dado)
        # Em zoom 20, cada pixel ≈ 1.4 metros
        # A cobertura varia com a latitude
        metros_por_pixel = self._metros_por_pixel_zoom(zoom_grid)
        cobertura_m = metros_por_pixel * largura_px
        cobertura_km = cobertura_m / 1000
        
        logger.info(f"   - Cobertura por imagem: {cobertura_km:.2f} km (zoom {zoom_grid})")
        
        # Calcular quantas imagens precisam em cada dimensão
        # Usando a fórmula: n_imagens = ceil(2*raio / cobertura)
        n_imagens_lado = max(1, math.ceil(2 * raio_km / cobertura_km))
        
        logger.info(f"   - Grid: {n_imagens_lado}x{n_imagens_lado} = {n_imagens_lado * n_imagens_lado} imagens")
        
        # Gerar coordenadas do grid
        grid = []
        espacamento = raio_km * 2 / n_imagens_lado
        
        for i in range(n_imagens_lado):
            for j in range(n_imagens_lado):
                # Calcular offset do centro
                offset_lat = (i - n_imagens_lado/2 + 0.5) * espacamento
                offset_lon = (j - n_imagens_lado/2 + 0.5) * espacamento
                
                # Ajustar longitude pela latitude (correção de projeção)
                lat = lat_centro + offset_lat / 111  # 111 km por grau de latitude
                lon = lon_centro + offset_lon / (111 * math.cos(math.radians(lat_centro)))
                
                grid.append({
                    'linha': i,
                    'coluna': j,
                    'latitude': lat,
                    'longitude': lon,
                    'offset_lat_km': offset_lat,
                    'offset_lon_km': offset_lon
                })
        
        logger.info(f"   ✅ Grid com {len(grid)} células calculado")
        return grid
    
    def _metros_por_pixel_zoom(self, zoom: int) -> float:
        """
        Calcula quantos metros por pixel em um determinado nível de zoom.
        Baseado na projeção Web Mercator do Google Maps.
        
        Args:
            zoom: Nível de zoom (0-21)
        
        Returns:
            Metros por pixel
        """
        # Earth's circumference in meters
        earth_circumference = 40075016.686
        # At zoom 0, image is 256 pixels
        pixels_at_zoom_0 = 256
        return earth_circumference / (pixels_at_zoom_0 * (2 ** zoom))
    
    def buscar_imagens_grid_transformador(
        self,
        transformador_id: int,
        tamanho: str = "640x640",
        zoom_grid: int = 20
    ) -> Dict:
        """
        Busca múltiplas imagens em grade para cobrir toda a área poligonal
        com zoom máximo, permitindo visão de alta resolução de toda a região.
        
        Args:
            transformador_id: ID do transformador
            tamanho: Tamanho de cada imagem em pixels
            zoom_grid: Nível de zoom para cada célula do grid (default 20)
        
        Returns:
            Dict com grid de imagens
        """
        logger.info(f"📊 Buscando imagens em GRID para transformador {transformador_id}")
        
        # Validar API key
        if not self.api_key:
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'grid': [],
                'motivo': 'API key não configurada'
            }
        
        # Buscar coordenadas do transformador
        coords = self._obter_coordenadas_transformador(transformador_id)
        if not coords:
            logger.error(f"   ❌ Transformador {transformador_id} não encontrado")
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'grid': [],
                'motivo': f'Transformador {transformador_id} não encontrado'
            }
        
        # Obter área poligonal
        area_poligonal_km = self._obter_area_poligonal_transformador(transformador_id)
        logger.info(f"   📏 Área poligonal: {area_poligonal_km} km")
        
        # Validar zoom
        zoom_grid = max(10, min(21, zoom_grid))
        
        try:
            # Calcular grid de coordenadas
            grid_coords = self._calcular_grid_coordenadas(
                lat_centro=coords['latitude'],
                lon_centro=coords['longitude'],
                area_km=area_poligonal_km,
                zoom_grid=zoom_grid,
                tamanho_imagem=tamanho
            )
            
            # Gerar URLs para cada célula
            imagens_grid = []
            for cell in grid_coords:
                urls = self._construir_url_staticmap(
                    lat=cell['latitude'],
                    lon=cell['longitude'],
                    zoom=zoom_grid,
                    tamanho=tamanho,
                    tipos=['satellite']
                )
                
                imagem = {
                    'linha': cell['linha'],
                    'coluna': cell['coluna'],
                    'latitude': cell['latitude'],
                    'longitude': cell['longitude'],
                    'offset_lat_km': cell['offset_lat_km'],
                    'offset_lon_km': cell['offset_lon_km'],
                    'url': urls['satellite'],
                    'zoom': zoom_grid,
                    'tamanho_pixels': tamanho,
                    'tipo': 'grid',
                    'fonte': 'GOOGLE_MAPS'
                }
                imagens_grid.append(imagem)
            
            # Registrar requisições na quota
            subestacao_id = coords.get('subestacao_id', 1)
            for imagem in imagens_grid:
                # Registra 1 requisição por célula (apenas satellite)
                self.quota_service.registrar_requisicao(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    tipo_requisicao='grid_satellite',
                    zoom=zoom_grid,
                    largura=int(tamanho.split('x')[0]),
                    altura=int(tamanho.split('x')[1]),
                    status='sucesso',
                    url=imagem['url'],
                    tempo_ms=0,
                    codigo_resposta=200,
                    custo_unidades=1.0,
                    notas=f"Grid [{imagem['linha']},{imagem['coluna']}] ID={transformador_id}"
                )
            
            logger.info(f"   ✅ Grid com {len(imagens_grid)} imagens gerado e quota contabilizada")
            
            return {
                'sucesso': True,
                'transformador_id': transformador_id,
                'nome': coords.get('nome'),
                'latitude_centro': coords['latitude'],
                'longitude_centro': coords['longitude'],
                'area_poligonal_km': area_poligonal_km,
                'zoom_grid': zoom_grid,
                'tamanho_imagem': tamanho,
                'dimensoes_grid': {
                    'linhas': max([c['linha'] for c in grid_coords]) + 1,
                    'colunas': max([c['coluna'] for c in grid_coords]) + 1
                },
                'total_imagens': len(imagens_grid),
                'imagens': imagens_grid,
                'motivo': 'Sucesso'
            }
            
        except Exception as exc:
            logger.error(f"   ❌ Erro ao gerar grid: {exc}", exc_info=True)
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'grid': [],
                'motivo': f'Erro ao gerar grid: {str(exc)}'
            }
    
    # ========================================================================
    # BUSCA EM LOTE - TRANSFORMADORES
    # ========================================================================
    
    def buscar_imagens_multiplos_transformadores(
        self,
        transformador_ids: List[int],
        zoom: int = 18,
        tamanho: str = "640x640"
    ) -> Dict:
        """
        Busca imagens para múltiplos transformadores em lote
        
        Args:
            transformador_ids: Lista de IDs de transformadores (máx 100)
            zoom: Nível de zoom
            tamanho: Tamanho da imagem
        
        Returns:
            Dict com resultados agregados
        """
        logger.info(f"🗺️  Buscando {len(transformador_ids)} transformadores no Google Maps")
        
        # Limitar a 100 transformadores
        transformador_ids = transformador_ids[:100]
        
        resultados = []
        sucessos = 0
        erros = 0
        
        for trans_id in transformador_ids:
            resultado = self.buscar_imagens_transformador(
                transformador_id=trans_id,
                zoom=zoom,
                tamanho=tamanho
            )
            
            if resultado['sucesso']:
                sucessos += 1
            else:
                erros += 1
            
            resultados.append(resultado)
        
        return {
            'total_solicitados': len(transformador_ids),
            'sucessos': sucessos,
            'erros': erros,
            'percentual_sucesso': round(100.0 * sucessos / len(transformador_ids)) if transformador_ids else 0,
            'resultados': resultados
        }
    
    # ========================================================================
    # CONSTRUÇÃO DE URLs
    # ========================================================================
    
    def _construir_url_staticmap(
        self,
        lat: float,
        lon: float,
        zoom: int,
        tamanho: str,
        tipos: List[str] = None
    ) -> Dict[str, str]:
        """
        Constrói URLs para Google Maps Static API
        
        Args:
            lat, lon: Coordenadas do transformador
            zoom: Nível de zoom
            tamanho: Tamanho em pixels
            tipos: Tipos de mapa desejados (satellite, hybrid, terrain, roadmap)
        
        Returns:
            Dict com URLs por tipo
        """
        tipos = tipos or ['satellite', 'hybrid']
        urls = {}
        
        logger.info(f"🗺️ Construindo URLs para coordenadas: lat={lat}, lon={lon}, zoom={zoom}")
        
        for map_type in tipos:
            params = {
                'center': f"{lat},{lon}",
                'zoom': zoom,
                'size': tamanho,
                'maptype': map_type,
                'key': self.api_key,
                'style': 'feature:all|element:labels|visibility:off'  # Hide labels for clarity
            }
            
            url = f"{self.BASE_URL}?{urlencode(params)}"
            urls[map_type] = url
            logger.debug(f"   URL {map_type}: {url[:100]}...")
        
        return urls
    
    # ========================================================================
    # AUXILIARES
    # ========================================================================
    
    def _obter_coordenadas_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca coordenadas de um transformador no banco de dados"""
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id,
                        nome,
                        latitude,
                        longitude,
                        potencia_kva,
                        codigo
                    FROM transformadores
                    WHERE id = :trans_id
                """), {'trans_id': transformador_id})
                
                row = result.fetchone()
                if not row:
                    logger.warning(f"⚠️ Transformador {transformador_id} não encontrado na tabela transformadores")
                    return None
                
                coords = {
                    'id': row[0],
                    'nome': row[1],
                    'latitude': float(row[2]),
                    'longitude': float(row[3]),
                    'potencia_kva': float(row[4]),
                    'codigo': row[5]
                }
                logger.info(f"✅ Coordenadas obtidas para transformador {transformador_id}: lat={coords['latitude']}, lon={coords['longitude']}")
                return coords
        
        except Exception as exc:
            logger.error(f"❌ Erro ao buscar coordenadas do transformador {transformador_id}: {exc}")
            return None
    
    def _obter_area_poligonal_transformador(self, transformador_id: int) -> float:
        """
        Busca a área poligonal de um transformador no banco de dados
        
        Prioridade de busca:
        1. Campo area_poligonal_km na tabela transformadores
        2. Campo area_poligonal_km na tabela transformadores_area_cobertura
        3. Valor default: 1.0 km
        """
        try:
            with self.engine.begin() as conn:
                # Tentar primeiro a tabela transformadores
                result = conn.execute(text("""
                    SELECT area_poligonal_km
                    FROM transformadores
                    WHERE id = :trans_id
                """), {'trans_id': transformador_id})
                
                row = result.fetchone()
                if row and row[0]:
                    area = float(row[0])
                    logger.info(f"   ✓ Área poligonal obtida do banco: {area} km")
                    return area
                
                # Tentar tabela transformadores_area_cobertura se não encontrar
                result = conn.execute(text("""
                    SELECT area_poligonal_km
                    FROM transformadores_area_cobertura
                    WHERE transformador_id = :trans_id
                """), {'trans_id': transformador_id})
                
                row = result.fetchone()
                if row and row[0]:
                    area = float(row[0])
                    logger.info(f"   ✓ Área poligonal obtida de área_cobertura: {area} km")
                    return area
                
                # Default
                logger.warning(f"   ⚠️ Área poligonal não encontrada, usando default: 1.0 km")
                return 1.0
        
        except Exception as exc:
            logger.warning(f"Erro ao buscar área poligonal do transformador {transformador_id}: {exc}")
            logger.warning(f"   Usando valor default: 1.0 km")
            return 1.0
    
    def _registrar_requisicao_google_maps(
        self,
        transformador_id: int,
        url: Dict,
        tipo: str = 'transformador'
    ) -> None:
        """Registra uma requisição ao Google Maps para auditoria"""
        
        try:
            with self.engine.begin() as conn:
                # Tabela: satelite_requisicoes_google_maps
                conn.execute(text("""
                    INSERT INTO satelite_requisicoes_google_maps
                    (transformador_id, tipo_requisicao, url_satellite, url_hybrid, 
                     data_requisicao, status)
                    VALUES (:trans_id, :tipo, :url_sat, :url_hyb, :data, 'registrada')
                """), {
                    'trans_id': transformador_id,
                    'tipo': tipo,
                    'url_sat': url.get('satellite'),
                    'url_hyb': url.get('hybrid'),
                    'data': datetime.utcnow()
                })
                
                logger.debug(f"✓ Requisição Google Maps registrada para transformador {transformador_id}")
        
        except Exception as exc:
            logger.warning(f"Erro ao registrar requisição Google Maps: {exc}")
    
    def _registrar_resultado_erro_transformador(
        self,
        transformador_id: int,
        erro: str,
        tipo: str
    ) -> None:
        """Registra erro em requisição de transformador"""
        
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO satelite_erros_transformador
                    (transformador_id, tipo_busca, erro_mensagem, data_erro)
                    VALUES (:trans_id, :tipo, :erro, :data)
                """), {
                    'trans_id': transformador_id,
                    'tipo': tipo,
                    'erro': erro,
                    'data': datetime.utcnow()
                })
        
        except Exception as exc:
            logger.warning(f"Erro ao registrar erro de transformador: {exc}")
    
    # ========================================================================
    # QUOTA E ESTATÍSTICAS
    # ========================================================================
    
    def obter_quota_google_maps_mes_atual(self) -> Dict:
        """
        Retorna informações de quota do mês atual
        """
        try:
            with self.engine.begin() as conn:
                # Buscar requisições do mês atual
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT transformador_id) as transformadores_unicos,
                        MAX(data_requisicao) as ultima_requisicao
                    FROM satelite_requisicoes_google_maps
                    WHERE DATE_TRUNC('month', data_requisicao) = DATE_TRUNC('month', NOW())
                """))
                
                row = result.fetchone()
                if not row:
                    row = (0, 0, None)
                
                total_usada = row[0] or 0
                disponivel = self.LIMIT_MENSAL - total_usada
                
                return {
                    'limite_mensal': self.LIMIT_MENSAL,
                    'usada_mes_atual': total_usada,
                    'disponivel': max(0, disponivel),
                    'percentual_uso': round(100.0 * total_usada / self.LIMIT_MENSAL, 2),
                    'transformadores_unicos': row[1] or 0,
                    'ultima_requisicao': row[2].isoformat() if row[2] else None
                }
        
        except Exception as exc:
            logger.error(f"Erro ao obter quota: {exc}")
            return {
                'limite_mensal': self.LIMIT_MENSAL,
                'usada_mes_atual': 0,
                'disponivel': self.LIMIT_MENSAL,
                'percentual_uso': 0,
                'transformadores_unicos': 0,
                'ultima_requisicao': None,
                'erro': str(exc)
            }
    
    def obter_estatisticas_google_maps(self) -> Dict:
        """
        Retorna estatísticas gerais de uso do Google Maps
        """
        try:
            with self.engine.begin() as conn:
                # Ultimos 30 dias
                resultado = conn.execute(text("""
                    SELECT 
                        DATE_TRUNC('day', data_requisicao)::date as dia,
                        COUNT(*) as requisicoes,
                        COUNT(DISTINCT transformador_id) as transformadores
                    FROM satelite_requisicoes_google_maps
                    WHERE data_requisicao >= NOW() - INTERVAL '30 days'
                    GROUP BY DATE_TRUNC('day', data_requisicao)
                    ORDER BY dia DESC
                """))
                
                historico_dias = []
                for row in resultado:
                    historico_dias.append({
                        'dia': row[0].isoformat() if row[0] else None,
                        'requisicoes': row[1],
                        'transformadores': row[2]
                    })
                
                # Total geral
                result_total = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_requisicoes,
                        COUNT(DISTINCT transformador_id) as transformadores_totais,
                        MIN(data_requisicao) as primeira_requisicao
                    FROM satelite_requisicoes_google_maps
                """))
                
                row_total = result_total.fetchone()
                
                return {
                    'total_requisicoes_historico': row_total[0] or 0,
                    'transformadores_buscados': row_total[1] or 0,
                    'primeira_requisicao': row_total[2].isoformat() if row_total[2] else None,
                    'historico_ultimos_30_dias': historico_dias,
                    'quota_mes_atual': self.obter_quota_google_maps_mes_atual()
                }
        
        except Exception as exc:
            logger.error(f"Erro ao obter estatísticas Google Maps: {exc}")
            return {
                'erro': str(exc),
                'quota_mes_atual': self.obter_quota_google_maps_mes_atual()
            }
