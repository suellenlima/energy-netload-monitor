"""
Serviço Unificado para Gerenciamento de Imagens de Satélite (CBERS-4A, Google Maps, etc)

Responsável por:
- Orquestração de buscas multi-fonte
- Decisão de qual fonte usar (Google Maps vs CBERS-4A) com preferências
- Integração com serviços existentes (GoogleMapsService, CBERSService)
- Logging e rastreamento de requisições
- Cálculo de quotas e custos
- Controle de preferências por subestação/transformador
- Rastreamento de BBOX e metadados de imagens

Author: Energy Netload Monitor
Date: 2026-02-04 (versão unificada)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..repositories.satelite_repository import SateliteRepository

logger = logging.getLogger(__name__)


class SateliteService:
    """
    Serviço principal e unificado para gerenciamento de satélites e imagens.
    Orquestra múltiplas fontes de dados geoespaciais com suporte a:
    - Decisão inteligente de fonte (Google Maps vs CBERS-4A)
    - Rastreamento detalhado de requisições
    - Gerenciamento de preferências por subestação
    - Controle de quotas mensais
    """

    def __init__(self, engine: Engine):
        """
        Inicializa service com engine SQLAlchemy.
        
        Args:
            engine: SQLAlchemy engine para banco de dados
        """
        self.engine = engine
        self.repository = SateliteRepository(engine)
        self.logger = logging.getLogger(__name__)
        self.GOOGLE_MAPS_LIMIT_MES = 25000

    # ========================================================================
    # COORDENADAS E DADOS BÁSICOS
    # ========================================================================

    def obter_coordenadas_transformador(self, transformador_id: int) -> Optional[Dict]:
        """
        Obtém coordenadas de um transformador para busca de satélite.
        
        Valida se as coordenadas são válidas (não NULL).
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Dict com {latitude, longitude, valido} ou None se não encontrado
        
        Raises:
            ValueError: Se coordenadas forem inválidas
        """
        try:
            trafo = self.repository.obter_transformador_completo(transformador_id)
            
            if not trafo:
                raise ValueError(f"Transformador {transformador_id} não encontrado")
            
            lat = trafo.get('latitude')
            lon = trafo.get('longitude')
            
            if not lat or not lon:
                raise ValueError(
                    f"Transformador {transformador_id} sem coordenadas válidas"
                )
            
            # Validar ranges
            if lat < -90 or lat > 90:
                raise ValueError(f"Latitude inválida: {lat}")
            
            if lon < -180 or lon > 180:
                raise ValueError(f"Longitude inválida: {lon}")
            
            return {
                'transformador_id': transformador_id,
                'transformador_codigo': trafo.get('codigo'),
                'transformador_nome': trafo.get('nome'),
                'distribuidora': trafo.get('distribuidora'),
                'latitude': lat,
                'longitude': lon,
                'tipo_tensao': trafo.get('tipo_tensao'),
                'valido': True
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter coordenadas: {e}")
            raise

    def obter_coordenadas_subestacao(self, subestacao_id: int) -> Optional[Dict]:
        """
        Obtém coordenadas de uma subestação para busca de satélite.
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            Dict com {latitude, longitude, valido} ou None
        
        Raises:
            ValueError: Se subestação não encontrada ou coordenadas inválidas
        """
        try:
            sub = self.repository.obter_subestacao_completa(subestacao_id)
            
            if not sub:
                raise ValueError(f"Subestação {subestacao_id} não encontrada")
            
            lat = sub.get('latitude')
            lon = sub.get('longitude')
            
            if not lat or not lon:
                raise ValueError(
                    f"Subestação {subestacao_id} sem coordenadas válidas"
                )
            
            return {
                'subestacao_id': subestacao_id,
                'subestacao_codigo': sub.get('codigo'),
                'subestacao_nome': sub.get('nome'),
                'distribuidora': sub.get('distribuidora'),
                'latitude': lat,
                'longitude': lon,
                'tensao_kv': sub.get('tensao_kv'),
                'valido': True
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter coordenadas de subestação: {e}")
            raise

    # ========================================================================
    # ÁREA POLIGONAL (COBERTURA)
    # ========================================================================

    def obter_area_cobertura_transformador(
        self, 
        transformador_id: int
    ) -> Optional[Dict]:
        """
        Obtém área de cobertura (polígono) de um transformador.
        
        A área é calculada a partir dos consumidores BT/MT/AT conectados:
        - ≥3 consumidores: ConvexHull (polígono real)
        - <3 consumidores: Buffer (raio adaptado: 500m-2km conforme tensão)
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Dict com área, método, número de consumidores
        """
        try:
            # Obter código do transformador
            trafo = self.repository.obter_transformador_completo(transformador_id)
            if not trafo:
                return None
            
            codigo = trafo.get('codigo')
            
            # Obter área de cobertura
            area = self.repository.obter_area_cobertura_transformador(codigo)
            
            if area:
                self.logger.info(
                    f"Área de cobertura: {area['area_km2']:.2f} km² "
                    f"({area['num_consumidores']} consumidores)"
                )
            
            return area
        
        except Exception as e:
            self.logger.error(f"Erro ao obter área de cobertura: {e}")
            return None

    # ========================================================================
    # HISTÓRICO DE REQUISIÇÕES
    # ========================================================================

    def obter_historico_transformador(
        self,
        transformador_id: int,
        limite: int = 50,
        offset: int = 0,
        apenas_sucesso: bool = False
    ) -> Dict:
        """
        Obtém histórico de requisições de satélite de um transformador.
        
        Args:
            transformador_id: ID do transformador
            limite: Máximo de registros
            offset: Deslocamento para paginação
            apenas_sucesso: Filtrar apenas bem-sucedidas
        
        Returns:
            Dict com historico e estatísticas
        """
        try:
            registros = self.repository.obter_historico_transformador(
                transformador_id=transformador_id,
                limite=limite,
                offset=offset,
                apenas_sucesso=apenas_sucesso
            )
            
            return {
                'transformador_id': transformador_id,
                'total_registros': len(registros),
                'registros': registros,
                'filtro_apenas_sucesso': apenas_sucesso,
                'paginacao': {
                    'limite': limite,
                    'offset': offset
                }
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter histórico: {e}")
            raise

    def registrar_requisicao(
        self,
        transformador_id: int,
        subestacao_id: int,
        fonte: str,
        status: str,
        url_imagem: Optional[str] = None,
        imagem_id: Optional[str] = None,
        data_aquisicao: Optional[str] = None,
        cobertura_nuvem_pct: Optional[float] = None,
        resolucao_metros: Optional[float] = None,
        tempo_requisicao_ms: Optional[int] = None,
        detalhes: Optional[Dict] = None,
        custo_usd: Optional[float] = None
    ) -> int:
        """
        Registra uma requisição de satélite no banco.
        
        Args:
            transformador_id: ID do transformador
            subestacao_id: ID da subestação
            fonte: Fonte (cbers4a, google_maps, etc)
            status: Status (sucesso, sem_cobertura, erro)
            ... (demais parâmetros opcionais)
        
        Returns:
            ID do registro criado
        """
        try:
            self.logger.info(
                f"Registrando requisição {fonte} para trafo {transformador_id} "
                f"(status={status})"
            )
            
            req_id = self.repository.registrar_requisicao_satelite(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                fonte=fonte,
                status=status,
                url_imagem=url_imagem,
                imagem_id=imagem_id,
                data_aquisicao=data_aquisicao,
                cobertura_nuvem_pct=cobertura_nuvem_pct,
                resolucao_metros=resolucao_metros,
                tempo_requisicao_ms=tempo_requisicao_ms,
                detalhes=detalhes,
                custo_usd=custo_usd
            )
            
            return req_id
        
        except Exception as e:
            self.logger.error(f"Erro ao registrar requisição: {e}")
            raise

    # ========================================================================
    # RASTREAMENTO DE REQUISIÇÕES (V2)
    # ========================================================================
    
    def registrar_requisicao_cbers4a(
        self,
        tipo_requisicao: str,
        status: str,
        subestacao_id: int = None,
        transformador_id: int = None,
        data_imagem: datetime = None,
        cobertura_nuvem: float = None,
        bbox: Tuple[float, float, float, float] = None,
        imagem_id: str = None,
        url_download: str = None,
        tamanho_mb: float = None,
        observacoes: str = None
    ) -> int:
        """
        Registra requisição CBERS-4A no banco com detalhes de BBOX e metadados.
        
        Args:
            tipo_requisicao: 'busca' ou 'download'
            status: 'sucesso', 'erro', 'sem_cobertura'
            subestacao_id: ID da subestação (opcional)
            transformador_id: ID do transformador (opcional)
            data_imagem: Data da imagem encontrada
            cobertura_nuvem: % de cobertura de nuvens
            bbox: (min_lat, min_lon, max_lat, max_lon)
            imagem_id: ID da imagem no INPE
            url_download: URL para download
            tamanho_mb: Tamanho em MB
            observacoes: Observações
        
        Returns:
            ID do registro inserido
        """
        try:
            bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon = (None, None, None, None)
            if bbox:
                bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon = bbox
            
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO requisicoes_satelite_cbers4a (
                        subestacao_id, transformador_id, tipo_requisicao, status, data_imagem,
                        cobertura_nuvem_percentual, bbox_min_lat, bbox_min_lon,
                        bbox_max_lat, bbox_max_lon, imagem_id, url_download,
                        tamanho_mb, observacoes, data_requisicao
                    ) VALUES (
                        :subestacao_id, :transformador_id, :tipo_requisicao, :status, :data_imagem,
                        :cobertura_nuvem, :bbox_min_lat, :bbox_min_lon,
                        :bbox_max_lat, :bbox_max_lon, :imagem_id, :url_download,
                        :tamanho_mb, :observacoes, NOW()
                    ) RETURNING id
                """), {
                    'subestacao_id': subestacao_id,
                    'transformador_id': transformador_id,
                    'tipo_requisicao': tipo_requisicao,
                    'status': status,
                    'data_imagem': data_imagem,
                    'cobertura_nuvem': cobertura_nuvem,
                    'bbox_min_lat': bbox_min_lat,
                    'bbox_min_lon': bbox_min_lon,
                    'bbox_max_lat': bbox_max_lat,
                    'bbox_max_lon': bbox_max_lon,
                    'imagem_id': imagem_id,
                    'url_download': url_download,
                    'tamanho_mb': tamanho_mb,
                    'observacoes': observacoes
                })
                
                requisicao_id = result.scalar()
                self.logger.info(f"✅ Requisição CBERS-4A registrada (ID: {requisicao_id})")
                return requisicao_id
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao registrar CBERS-4A: {e}")
            raise
    
    def registrar_requisicao_google_maps(
        self,
        subestacao_id: int,
        tipo_requisicao: str,
        status: str,
        bbox: Tuple[float, float, float, float] = None,
        observacoes: str = None
    ) -> int:
        """
        Registra requisição Google Maps no banco com BBOX e metadados.
        
        Args:
            subestacao_id: ID da subestação
            tipo_requisicao: 'static_map', 'street_view', etc.
            status: 'sucesso', 'erro', 'cancelado'
            bbox: (min_lat, min_lon, max_lat, max_lon)
            observacoes: Observações
        
        Returns:
            ID do registro inserido
        """
        try:
            bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon = (None, None, None, None)
            if bbox:
                bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon = bbox
            
            ano_mes = datetime.now().strftime('%Y-%m')
            
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO requisicoes_satelite_google (
                        subestacao_id, tipo_requisicao, status, 
                        bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon,
                        observacoes, data_requisicao, ano_mes
                    ) VALUES (
                        :subestacao_id, :tipo_requisicao, :status,
                        :bbox_min_lat, :bbox_min_lon, :bbox_max_lat, :bbox_max_lon,
                        :observacoes, NOW(), :ano_mes
                    ) RETURNING id
                """), {
                    'subestacao_id': subestacao_id,
                    'tipo_requisicao': tipo_requisicao,
                    'status': status,
                    'bbox_min_lat': bbox_min_lat,
                    'bbox_min_lon': bbox_min_lon,
                    'bbox_max_lat': bbox_max_lat,
                    'bbox_max_lon': bbox_max_lon,
                    'observacoes': observacoes,
                    'ano_mes': ano_mes
                })
                
                requisicao_id = result.scalar()
                self.logger.info(f"✅ Requisição Google Maps registrada (ID: {requisicao_id})")
                return requisicao_id
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao registrar Google Maps: {e}")
            raise

    # ========================================================================
    # PREFERÊNCIAS
    # ========================================================================
    
    def _obter_preferencia_subestacao(self, subestacao_id: int) -> Optional[str]:
        """
        Obtém preferência de satélite armazenada para subestação.
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            'CBERS-4A', 'GOOGLE_MAPS' ou None
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT satelite_preferido
                    FROM preferencia_satelite_subestacao
                    WHERE subestacao_id = :subestacao_id
                """), {'subestacao_id': subestacao_id})
                
                row = result.fetchone()
                return row[0] if row else None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao obter preferência: {e}")
            return None
    
    def definir_preferencia_subestacao(
        self,
        subestacao_id: int,
        satelite_preferido: str = 'CBERS-4A'
    ) -> bool:
        """
        Define preferência de satélite para subestação.
        
        Args:
            subestacao_id: ID da subestação
            satelite_preferido: 'CBERS-4A' ou 'GOOGLE_MAPS'
        
        Returns:
            True se sucesso
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO preferencia_satelite_subestacao (subestacao_id, satelite_preferido)
                    VALUES (:subestacao_id, :satelite_preferido)
                    ON CONFLICT (subestacao_id) DO UPDATE SET
                        satelite_preferido = :satelite_preferido,
                        data_atualizacao = NOW()
                """), {
                    'subestacao_id': subestacao_id,
                    'satelite_preferido': satelite_preferido
                })
                
                self.logger.info(f"✅ Preferência definida para SE {subestacao_id}: {satelite_preferido}")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao definir preferência: {e}")
            return False

    # ========================================================================
    # ESTATÍSTICAS E QUOTA
    # ========================================================================

    def obter_estatisticas_google_maps(self) -> Dict:
        """
        Obtém estatísticas gerais de uso do Google Maps.
        
        Retorna:
        - Total de requisições
        - Transformadores únicos
        - Custo total
        - Taxa de sucesso
        """
        try:
            stats = self.repository.obter_estatisticas_google_maps()
            
            self.logger.info(
                f"Google Maps: {stats['total_requisicoes']} requisições, "
                f"${stats['custo_total_usd']:.2f} gasto"
            )
            
            return stats
        
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            raise

    def obter_quota_mes_atual(self) -> Dict:
        """
        Obtém quota de requisições do mês atual.
        
        Retorna:
        - Requisições usadas
        - Limite mensal (25.000)
        - Disponível
        - Percentual de uso
        - Custo estimado
        - Mês/ano
        """
        try:
            quota = self.repository.obter_quota_mes_atual()
            
            self.logger.info(
                f"Quota {quota['mes_ano']}: "
                f"{quota['requisicoes_mes']}/{quota['limite_mensal']} "
                f"({quota['percentual_uso']:.1f}%) - ${quota['custo_mes_usd']:.2f}"
            )
            
            return quota
        
        except Exception as e:
            self.logger.error(f"Erro ao obter quota: {e}")
            raise

    # ========================================================================
    # DECISÃO DE FONTE (ESTRATÉGIA DE FALLBACK COM PREFERÊNCIAS)
    # ========================================================================

    def decidir_fonte_satelite(
        self,
        transformador_id: int,
        tentar_google_maps: bool = True,
        tentar_cbers4a: bool = True,
        force_cbers4a: bool = False,
        subestacao_id: int = None
    ) -> Dict:
        """
        Decide qual fonte de satélite usar (Google Maps vs CBERS-4A).
        
        Estratégia de decisão:
        1. Se force_cbers4a=True → CBERS-4A (gratuito, mas menos preciso)
        2. Se há preferência armazenada → Usar preferência
        3. Se quota Google disponível → Google Maps (1m/pixel, melhor)
        4. Se quota Google esgotada → CBERS-4A (fallback, 2m/pixel)
        5. Se nenhuma disponível → Erro
        
        Args:
            transformador_id: ID do transformador
            tentar_google_maps: Se deve tentar Google Maps
            tentar_cbers4a: Se deve tentar CBERS-4A como fallback
            force_cbers4a: Forçar uso de CBERS-4A (ignora quota)
            subestacao_id: ID da subestação (para consultar preferência)
        
        Returns:
            Dict com {
                'fonte_recomendada': 'google_maps' ou 'cbers4a',
                'razao': 'motivo da escolha',
                'resolucao_m': resolução em metros,
                'custo_estimado': custo em USD,
                'pode_usar': True/False
            }
        """
        try:
            # Se forçar CBERS-4A
            if force_cbers4a and tentar_cbers4a:
                return {
                    'fonte_recomendada': 'cbers4a',
                    'razao': 'CBERS-4A forçado (gratuito)',
                    'resolucao_m': 2.0,
                    'cobertura': 'Brasil inteiro',
                    'custo_estimado': 0.0,
                    'pode_usar': True
                }
            
            # Verificar preferência armazenada
            if subestacao_id:
                preferencia = self._obter_preferencia_subestacao(subestacao_id)
                if preferencia:
                    self.logger.info(f"Usando preferência armazenada: {preferencia}")
                    if preferencia == 'CBERS-4A':
                        return {
                            'fonte_recomendada': 'cbers4a',
                            'razao': 'Preferência armazenada: CBERS-4A',
                            'resolucao_m': 2.0,
                            'cobertura': 'Brasil inteiro',
                            'custo_estimado': 0.0,
                            'pode_usar': True
                        }
                    elif preferencia == 'GOOGLE_MAPS' and tentar_google_maps:
                        # Ainda verificar quota
                        quota = self.obter_quota_mes_atual()
                        if quota['disponivel'] > 0:
                            return {
                                'fonte_recomendada': 'google_maps',
                                'razao': f"Preferência: Google Maps (quota: {quota['disponivel']})",
                                'resolucao_m': 1.0,
                                'cobertura': 'Mundo inteiro',
                                'quota_disponivel': quota['disponivel'],
                                'custo_estimado': 0.007,
                                'pode_usar': True
                            }
            
            # Verificar quota do Google Maps
            if tentar_google_maps:
                quota = self.obter_quota_mes_atual()
                
                if quota['disponivel'] > 0:
                    return {
                        'fonte_recomendada': 'google_maps',
                        'razao': f"Quota disponível ({quota['disponivel']} requisições)",
                        'resolucao_m': 1.0,
                        'cobertura': 'Mundo inteiro',
                        'quota_disponivel': quota['disponivel'],
                        'custo_estimado': 0.007,
                        'pode_usar': True
                    }
                else:
                    self.logger.warning(
                        f"⚠️ Quota Google Maps esgotada "
                        f"({quota['requisicoes_mes']}/{quota['limite_mensal']})"
                    )
            
            # Fallback para CBERS-4A
            if tentar_cbers4a:
                return {
                    'fonte_recomendada': 'cbers4a',
                    'razao': 'Fallback CBERS-4A (Google Maps sem quota)',
                    'resolucao_m': 2.0,
                    'cobertura': 'Brasil inteiro',
                    'custo_estimado': 0.0,
                    'pode_usar': True
                }
            
            # Nenhuma fonte disponível
            return {
                'fonte_recomendada': None,
                'razao': 'Nenhuma fonte disponível (ambas desabilitadas)',
                'pode_usar': False
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao decidir fonte: {e}")
            raise

    # ========================================================================
    # SUBESTAÇÕES
    # ========================================================================

    def listar_subestacoes_distribuidora(self, distribuidora: str) -> Dict:
        """
        Lista todas as subestações de uma distribuidora com coordenadas.
        
        Útil para buscar imagens de satélite em massa por distribuidora.
        
        Args:
            distribuidora: Nome da distribuidora (ex: "CEMIG DISTRIBUICAO S.A")
        
        Returns:
            Dict com lista de subestações e estatísticas
        """
        try:
            subestacoes = self.repository.obter_subestacoes_por_distribuidor(distribuidora)
            
            self.logger.info(
                f"Encontradas {len(subestacoes)} subestações para {distribuidora}"
            )
            
            return {
                'distribuidora': distribuidora,
                'total_subestacoes': len(subestacoes),
                'subestacoes': subestacoes
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao listar subestações: {e}")
            raise


# Alias para compatibilidade com código antigo
SatelliteServiceV2 = SateliteService

__all__ = ['SateliteService', 'SatelliteServiceV2']
