"""
Application layer for multi-source roof detection service

Extends TelhadoDetectionService with fallback strategies:
1. Try Google Maps (priority - high resolution)
2. Fallback to CBERS-4A (free - lower resolution)
3. Aggregates results from multiple sources
"""

import logging
import time
import os
from typing import Dict, Optional, List, Any

from ...domain.telhado import Telhado
from ...infrastructure.persistence.telhado_multifonte import TelhadoMultiFonteRepository
from .service import TelhadoDetectionService


logger = logging.getLogger(__name__)


class TelhadoMultiFonteApplicationService:
    """
    Application service for multi-source roof detection with fallback strategy.
    
    Orchestrates:
    - Detection with multiple image sources
    - Fallback logic when primary source fails
    - Result aggregation and persistence
    """
    
    def __init__(self, engine):
        """
        Initialize multi-source detection service
        
        Args:
            engine: SQLAlchemy engine for database access
        """
        self.engine = engine
        self.repository = TelhadoMultiFonteRepository(engine)
        self.logger = logging.getLogger(__name__)
        
        # Lazy-loaded services
        self._servico_telhados: Optional[TelhadoDetectionService] = None
    
    def _obter_servico_telhados(self) -> TelhadoDetectionService:
        """Get or create roof detection service instance (DDD)"""
        if self._servico_telhados is None:
            self.logger.info("🔧 Inicializando serviço DDD de detecção de telhados...")
            self._servico_telhados = TelhadoDetectionService(engine=self.engine, use_gpu=True)
        return self._servico_telhados
    
    def detectar_telhados_multifonte(
        self,
        transformador_id: int,
        subestacao_id: int,
        confianca_minima: float = 0.5,
        tentar_google_maps_primeiro: bool = True,
        tentar_cbers4a_fallback: bool = True,
        salvar_rois: bool = False
    ) -> Dict[str, Any]:
        """
        Detect roofs using multiple sources with fallback strategy.
        
        Flow:
        1. Validate input and retrieve transformer coordinates
        2. Try Google Maps (priority, best resolution)
        3. If not found, try CBERS-4A (fallback, free)
        4. Save detected roofs and record processing
        
        Args:
            transformador_id: Transformer ID
            subestacao_id: Substation ID
            confianca_minima: Minimum confidence score (0-1)
            tentar_google_maps_primeiro: Try Google Maps first
            tentar_cbers4a_fallback: Use CBERS-4A as fallback
            salvar_rois: Save ROIs to disk
        
        Returns:
            Dict with detection result
        """
        try:
            self.logger.info(
                f"\n{'='*80}\n"
                f"[MULTI-FONTE] Detectando telhados para transformador {transformador_id}\n"
                f"{'='*80}"
            )
            
            # ================================================================
            # 1. VALIDATE INPUT AND RETRIEVE DATA
            # ================================================================
            
            self.logger.info("[1/4] Recuperando dados do transformador...")
            
            # Validate confidence
            if confianca_minima < 0 or confianca_minima > 1:
                raise ValueError("Confiança deve estar entre 0 e 1")
            
            # Retrieve transformer
            transformador = self.repository.obter_transformador(transformador_id)
            if not transformador:
                raise ValueError(f"Transformador {transformador_id} não encontrado")
            
            lat = transformador['latitude']
            lon = transformador['longitude']
            
            if not lat or not lon:
                raise ValueError(f"Transformador {transformador_id} sem coordenadas válidas")
            
            self.logger.info(
                f"✓ Transformador encontrado: {transformador['nome']} ({lat}, {lon})"
            )
            
            # Retrieve substation
            subestacao = self.repository.obter_subestacao(subestacao_id)
            if not subestacao:
                raise ValueError(f"Subestação {subestacao_id} não encontrada")
            
            self.logger.info(f"✓ Subestação encontrada: {subestacao['nome']}")
            
            # ================================================================
            # 2. GENERATE IMAGE URLs FOR MULTIPLE SOURCES
            # ================================================================
            
            self.logger.info("[2/4] Gerando URLs de imagens...")
            
            urls_por_fonte = self._gerar_urls_multifonte(
                transformador_id, lat, lon, subestacao_id
            )
            
            self.logger.info(f"✓ URLs geradas: {list(urls_por_fonte.keys())}")
            
            # ================================================================
            # 3. TRY DETECTION WITH FALLBACK
            # ================================================================
            
            self.logger.info("[3/4] Tentando detectar telhados...")
            
            resultado_deteccao = None
            fonte_utilizada = None
            url_utilizada = None
            detalhes_tentativas = {}
            
            # Try Google Maps first
            if tentar_google_maps_primeiro and 'google_maps' in urls_por_fonte:
                self.logger.info("\n🔍 Tentativa 1: Google Maps...")
                
                resultado_google = self._tentar_google_maps(
                    url=urls_por_fonte['google_maps']['url'],
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    confianca_minima=confianca_minima,
                    distribuidora=transformador['distribuidora']
                )
                
                detalhes_tentativas['google_maps'] = resultado_google['detalhes']
                
                if resultado_google['sucesso'] and resultado_google['telhados_detectados'] > 0:
                    self.logger.info(f"✓ {resultado_google['telhados_detectados']} telhados detectados!")
                    resultado_deteccao = resultado_google
                    fonte_utilizada = 'google_maps'
                    url_utilizada = urls_por_fonte['google_maps']['url']
                else:
                    self.logger.info("⚠️  Nenhum telhado detectado em Google Maps")
            
            # Fallback to CBERS-4A
            if (not fonte_utilizada and tentar_cbers4a_fallback and 
                'cbers4a' in urls_por_fonte):
                
                self.logger.info("\n🔍 Tentativa 2: CBERS-4A (fallback)...")
                
                resultado_cbers = self._tentar_cbers4a(
                    urls_por_fonte=urls_por_fonte['cbers4a'],
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    confianca_minima=confianca_minima
                )
                
                detalhes_tentativas['cbers4a'] = resultado_cbers['detalhes']
                
                if resultado_cbers['sucesso']:
                    self.logger.info(f"✓ CBERS-4A disponível com {len(resultado_cbers.get('bandas', []))} bandas")
                    resultado_deteccao = resultado_cbers
                    fonte_utilizada = 'cbers4a'
                    url_utilizada = resultado_cbers['url_principal']
            
            # ================================================================
            # 4. SAVE RESULTS
            # ================================================================
            
            self.logger.info("[4/4] Salvando resultados...")
            
            telhados_salvos = []
            
            if resultado_deteccao and resultado_deteccao.get('telhados'):
                # Save detected roofs
                telhados_ids = self.repository.salvar_telhados_detectados(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    telhados=resultado_deteccao['telhados'],
                    fonte_imagem=fonte_utilizada or 'desconhecida',
                    url_imagem_origem=url_utilizada
                )
                telhados_salvos = telhados_ids
                self.logger.info(f"✓ {len(telhados_salvos)} telhados salvos no banco")
            
            # Record processing
            mensagem = (
                f"Detectados {len(telhados_salvos)} telhados com {fonte_utilizada or 'nenhuma'}"
                if telhados_salvos 
                else "Nenhum telhado detectado"
            )
            
            self.repository.registrar_processamento(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                distribuidora=transformador['distribuidora'],
                fonte_utilizada=fonte_utilizada or 'nenhuma',
                telhados_detectados=len(telhados_salvos),
                sucesso=len(telhados_salvos) > 0,
                url_imagem=url_utilizada or '',
                mensagem=mensagem,
                detalhes=detalhes_tentativas
            )
            
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"[CONCLUSÃO] {mensagem}")
            self.logger.info(f"{'='*80}\n")
            
            return {
                'sucesso': len(telhados_salvos) > 0,
                'fonte_utilizada': fonte_utilizada,
                'telhados_detectados': len(telhados_salvos),
                'telhados_ids': telhados_salvos,
                'url_imagem_utilizada': url_utilizada,
                'transformador': transformador,
                'subestacao': subestacao,
                'detalhes_tentativas': detalhes_tentativas,
                'telhados_dados': resultado_deteccao.get('telhados', []) if resultado_deteccao else [],
                'timestamp': resultado_deteccao.get('timestamp') if resultado_deteccao else None
            }
        
        except Exception as e:
            self.logger.error(f"Erro crítico na detecção multi-fonte: {e}", exc_info=True)
            raise
    
    def _gerar_urls_multifonte(
        self,
        transformador_id: int,
        latitude: float,
        longitude: float,
        subestacao_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Generate image URLs for multiple sources.
        
        Returns:
            Dict with URLs by source
        """
        try:
            from .image_service import ImagemMultiFonteService
            
            google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
            service_multifonte = ImagemMultiFonteService(google_maps_api_key)
            
            urls = service_multifonte.gerar_urls_todas_fontes(
                transformador_id=transformador_id,
                latitude=latitude,
                longitude=longitude,
                vertices_poligono=[]
            )
            
            return urls
        
        except Exception as e:
            self.logger.warning(f"Erro ao gerar URLs multi-fonte: {e}")
            return {}
    
    def _tentar_google_maps(
        self,
        url: str,
        transformador_id: int,
        subestacao_id: int,
        confianca_minima: float,
        distribuidora: str
    ) -> Dict[str, Any]:
        """
        Try to detect roofs in Google Maps image.
        
        Returns:
            Dict with attempt result
        """
        try:
            self.logger.info(f"URL: {url[:100]}...")
            self.logger.info(f"Zoom: 19, Resolução: ~1m/pixel")
            
            from .quota_service import GoogleMapsQuotaService
            
            # Initialize quota service
            quota_service = GoogleMapsQuotaService(self.engine)
            tempo_inicio = time.time()
            
            # Use DDD TelhadoDetectionService (Application Layer)
            servico_telhados = self._obter_servico_telhados()
            resultado_deteccao = servico_telhados.processar_telhados_lote(
                url_imagem=url,
                id_subestacao=f"trafo_{transformador_id}",
                id_imagem_satelite=f"google_maps_{transformador_id}",
                resolucao_m_por_pixel=1.0,
                confianca_minima=confianca_minima,
                diretorio_saida=None
            )
            
            tempo_resposta_ms = int((time.time() - tempo_inicio) * 1000)
            status_requisicao = 'sucesso' if resultado_deteccao.telhados_detectados > 0 else 'nenhum_resultado'
            
            # Record quota
            resultado_quota = quota_service.registrar_requisicao(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                tipo_requisicao='satellite',
                zoom=19,
                largura=640,
                altura=640,
                status=status_requisicao,
                url=url,
                tempo_ms=tempo_resposta_ms,
                codigo_resposta=200
            )
            
            if resultado_quota['sucesso']:
                self.logger.info(f"Requisição registrada: Custo=${resultado_quota['custo_usd']:.4f}")
            
            # Format detected roofs
            telhados_formatados = self._formatar_telhados_detectados(resultado_deteccao)
            
            return {
                'sucesso': resultado_deteccao.telhados_detectados > 0,
                'telhados_detectados': resultado_deteccao.telhados_detectados,
                'telhados': telhados_formatados,
                'url': url,
                'timestamp': resultado_deteccao.timestamp_processamento,
                'detalhes': {
                    'tentado': True,
                    'status': 'sucesso' if resultado_deteccao.telhados_detectados > 0 else 'nenhum_telhado_detectado',
                    'telhados_detectados': resultado_deteccao.telhados_detectados,
                    'url': url,
                    'custo_usd': resultado_quota.get('custo_usd', 0),
                    'tempo_ms': tempo_resposta_ms
                }
            }
        
        except Exception as e:
            self.logger.warning(f"✗ Erro em Google Maps: {e}")
            return {
                'sucesso': False,
                'telhados_detectados': 0,
                'telhados': [],
                'url': url,
                'detalhes': {
                    'tentado': True,
                    'status': 'erro',
                    'erro': str(e),
                    'url': url
                }
            }
    
    def _tentar_cbers4a(
        self,
        urls_por_fonte: Dict[str, str],
        transformador_id: int,
        subestacao_id: int,
        confianca_minima: float
    ) -> Dict[str, Any]:
        """
        Try to use CBERS-4A as free fallback.
        
        Returns:
            Dict with attempt result
        """
        try:
            # CBERS-4A doesn't consume quota (it's free)
            self.logger.info(f"Bandas disponíveis: {list(urls_por_fonte.keys())}")
            self.logger.info(f"ℹ️  CBERS-4A não consome quota (é gratuito)")
            self.logger.info(f"Resolução: 2m/pixel")
            
            url_principal = urls_por_fonte.get('red')
            
            return {
                'sucesso': True,
                'telhados_detectados': 0,  # CBERS-4A requires additional pipeline
                'telhados': [],
                'bandas': list(urls_por_fonte.keys()),
                'url_principal': url_principal,
                'timestamp': None,
                'detalhes': {
                    'tentado': True,
                    'status': 'url_encontrada',
                    'bandas': list(urls_por_fonte.keys()),
                    'url_principal': url_principal,
                    'custo_usd': 0.0
                }
            }
        
        except Exception as e:
            self.logger.warning(f"✗ Erro em CBERS-4A: {e}")
            return {
                'sucesso': False,
                'telhados_detectados': 0,
                'telhados': [],
                'bandas': [],
                'url_principal': None,
                'detalhes': {
                    'tentado': True,
                    'status': 'erro',
                    'erro': str(e)
                }
            }
    
    def _formatar_telhados_detectados(self, resultado_deteccao) -> List[Dict[str, Any]]:
        """
        Convert detection result to common format.
        
        Args:
            resultado_deteccao: TelhadoDetectionService result
        
        Returns:
            List of formatted roof dicts
        """
        try:
            telhados_formatados = []
            
            for telhado in resultado_deteccao.telhados:
                telhados_formatados.append({
                    'latitude': telhado.lat if hasattr(telhado, 'lat') else 0,
                    'longitude': telhado.lon if hasattr(telhado, 'lon') else 0,
                    'area_m2': telhado.area_m2 if hasattr(telhado, 'area_m2') else 0,
                    'confianca': telhado.confianca if hasattr(telhado, 'confianca') else 0,
                    'bbox': {
                        'x': telhado.bbox.get('x', 0) if hasattr(telhado, 'bbox') else 0,
                        'y': telhado.bbox.get('y', 0) if hasattr(telhado, 'bbox') else 0,
                        'w': telhado.bbox.get('w', 0) if hasattr(telhado, 'bbox') else 0,
                        'h': telhado.bbox.get('h', 0) if hasattr(telhado, 'bbox') else 0
                    },
                    'resolucao_cm': 100
                })
            
            return telhados_formatados
        
        except Exception as e:
            self.logger.warning(f"Erro ao formatar telhados: {e}")
            return []
