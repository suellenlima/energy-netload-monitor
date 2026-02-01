"""
SatelliteServiceV2 - Controle Inteligente de Fontes de Satélites
Gerencia priorização entre CBERS-4A (Brasil) e Google Maps
Com rastreamento de requisições e controle de quota mensal
Busca por Subestação e Transformador

Prioridade:
  1. CBERS-4A (INPE) - Gratuito, sem limite, dados brasileiros
  2. Google Maps - Se CBERS-4A sem cobertura E não ultrapassa 25k/mês
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class SatelliteServiceV2:
    """
    Serviço consolidado para gerenciar fontes de satélites com priorização e quota
    Suporta busca por subestação e transformador
    """
    
    def __init__(self, engine: Engine):
        """
        Inicializa serviço de satélites
        
        Args:
            engine: SQLAlchemy engine para banco de dados
        """
        self.engine = engine
        self.GOOGLE_MAPS_LIMIT_MES = 25000
    
    # ========================================================================
    # DECISÃO DE FONTE - TRANSFORMADOR
    # ========================================================================
    
    def decidir_fonte_satelite_transformador(
        self,
        transformador_id: int,
        preferencia_armazenada: str = None
    ) -> Dict:
        """
        Decide qual fonte de satélite usar para um transformador
        
        Args:
            transformador_id: ID do transformador
            preferencia_armazenada: Preferência salva ('CBERS-4A' ou 'GOOGLE_MAPS')
        
        Returns:
            Dict com: {
                'fonte': str,
                'pode_usar': bool,
                'motivo': str,
                'transformador_id': int
            }
        """
        logger.info(f"🛰️ Decidindo fonte para TRANSFORMADOR {transformador_id}")
        
        # Para transformador, usar preferência CBERS-4A por padrão
        preferencia = preferencia_armazenada or 'CBERS-4A'
        
        if preferencia == 'CBERS-4A':
            logger.info(f"   ✅ CBERS-4A selecionado (transformador)")
            return {
                'fonte': 'CBERS-4A',
                'pode_usar': True,
                'motivo': 'CBERS-4A - busca por transformador, resolução 2m WPM',
                'transformador_id': transformador_id,
                'resolucao_metros': 2.0,
                'cobertura': 'Brasil/América do Sul'
            }
        
        # Se preferir Google Maps
        if preferencia == 'GOOGLE_MAPS':
            quota_info = self.verificar_quota_google_maps()
            
            if quota_info['pode_usar']:
                logger.info(f"   ✅ Google Maps selecionado")
                return {
                    'fonte': 'GOOGLE_MAPS',
                    'pode_usar': True,
                    'motivo': f"Google Maps - quota disponível ({quota_info['usada']}/{self.GOOGLE_MAPS_LIMIT_MES})",
                    'transformador_id': transformador_id,
                    'quota_disponivel': quota_info['disponivel'],
                    'percentual_uso': quota_info['percentual_uso'],
                    'resolucao_metros': 0.3,
                    'cobertura': 'Global'
                }
            else:
                logger.warning(f"   ⚠️ Google Maps SEM quota! Usando CBERS-4A")
                return {
                    'fonte': 'CBERS-4A',
                    'pode_usar': True,
                    'motivo': 'Fallback: Google Maps sem quota, usando CBERS-4A WPM 2m',
                    'transformador_id': transformador_id,
                    'resolucao_metros': 2.0,
                    'cobertura': 'Brasil/América do Sul'
                }
        
        # Padrão
        return {
            'fonte': 'CBERS-4A',
            'pode_usar': True,
            'motivo': 'Padrão - CBERS-4A WPM 2m',
            'transformador_id': transformador_id,
            'resolucao_metros': 2.0,
            'cobertura': 'Brasil/América do Sul'
        }
    
    # ========================================================================
    # DECISÃO DE FONTE - SUBESTAÇÃO
    # ========================================================================
    
    def decidir_fonte_satelite(
        self,
        subestacao_id: int,
        preferencia_armazenada: str = None
    ) -> Dict:
        """
        Decide qual fonte de satélite usar para uma subestação
        
        Lógica:
        1. Verificar preferência armazenada (padrão: CBERS-4A)
        2. Se CBERS-4A: usar sempre (gratuito, sem limite)
        3. Se Google Maps: verificar se tem quota disponível
        4. Se sem quota Google: tentar CBERS-4A como fallback
        
        Args:
            subestacao_id: ID da subestação
            preferencia_armazenada: Preferência salva ('CBERS-4A' ou 'GOOGLE_MAPS')
        
        Returns:
            Dict com: {
                'fonte': str ('CBERS-4A' ou 'GOOGLE_MAPS'),
                'pode_usar': bool,
                'motivo': str,
                'quota_disponivel': int (apenas Google),
                'quota_usada': int (apenas Google)
            }
        """
        # Obter preferência da subestação
        preferencia = self._obter_preferencia_subestacao(subestacao_id) or preferencia_armazenada or 'CBERS-4A'
        
        logger.info(f"🛰️ Decidindo fonte para SE {subestacao_id} (preferência: {preferencia})")
        
        # Se preferir CBERS-4A: sempre usar (sem limite)
        if preferencia == 'CBERS-4A':
            logger.info(f"   ✅ CBERS-4A selecionado (sem limite)")
            return {
                'fonte': 'CBERS-4A',
                'pode_usar': True,
                'motivo': 'CBERS-4A preferido - gratuito, sem limite',
                'resolucao_metros': 2.0,
                'cobertura': 'Brasil/América do Sul'
            }
        
        # Se preferir Google Maps: verificar quota
        if preferencia == 'GOOGLE_MAPS':
            quota_info = self.verificar_quota_google_maps()
            
            if quota_info['pode_usar']:
                logger.info(f"   ✅ Google Maps selecionado ({quota_info['percentual_uso']}% da quota)")
                return {
                    'fonte': 'GOOGLE_MAPS',
                    'pode_usar': True,
                    'motivo': f"Google Maps - quota disponível ({quota_info['usada']}/{self.GOOGLE_MAPS_LIMIT_MES})",
                    'quota_disponivel': quota_info['disponivel'],
                    'quota_usada': quota_info['usada'],
                    'percentual_uso': quota_info['percentual_uso'],
                    'resolucao_metros': 0.3,
                    'cobertura': 'Global'
                }
            else:
                logger.warning(f"   ⚠️ Google Maps SEM quota! Usando CBERS-4A como fallback")
                return {
                    'fonte': 'CBERS-4A',
                    'pode_usar': True,
                    'motivo': 'Fallback: Google Maps excedeu quota (25k/mês), usando CBERS-4A',
                    'resolucao_metros': 2.0,
                    'cobertura': 'Brasil/América do Sul'
                }
        
        # Padrão: CBERS-4A
        return {
            'fonte': 'CBERS-4A',
            'pode_usar': True,
            'motivo': 'Padrão - CBERS-4A',
            'resolucao_metros': 2.0,
            'cobertura': 'Brasil/América do Sul'
        }
    
    # ========================================================================
    # QUOTA DO GOOGLE MAPS
    # ========================================================================
    
    def verificar_quota_google_maps(self) -> Dict:
        """
        Verifica quota mensal de Google Maps (25k requisições)
        
        Returns:
            Dict com: {
                'pode_usar': bool,
                'usada': int,
                'disponivel': int,
                'percentual_uso': float,
                'limite': int
            }
        """
        ano_mes = datetime.now().strftime('%Y-%m')
        
        try:
            with self.engine.begin() as conn:
                # Contar requisições bem-sucedidas este mês
                result = conn.execute(text("""
                    SELECT COUNT(*) as total
                    FROM requisicoes_satelite_google
                    WHERE ano_mes = :ano_mes AND status = 'sucesso'
                """), {'ano_mes': ano_mes})
                
                usada = result.scalar() or 0
                disponivel = max(0, self.GOOGLE_MAPS_LIMIT_MES - usada)
                percentual = (usada / self.GOOGLE_MAPS_LIMIT_MES) * 100
                
                pode_usar = disponivel > 0
                
                logger.info(f"📊 Quota Google Maps {ano_mes}: {usada}/{self.GOOGLE_MAPS_LIMIT_MES} ({percentual:.1f}%)")
                
                return {
                    'pode_usar': pode_usar,
                    'usada': usada,
                    'disponivel': disponivel,
                    'percentual_uso': percentual,
                    'limite': self.GOOGLE_MAPS_LIMIT_MES,
                    'mes': ano_mes
                }
                
        except Exception as e:
            logger.error(f"❌ Erro ao verificar quota: {e}")
            # Retornar conservador (sem quota)
            return {
                'pode_usar': False,
                'usada': self.GOOGLE_MAPS_LIMIT_MES,
                'disponivel': 0,
                'percentual_uso': 100.0,
                'limite': self.GOOGLE_MAPS_LIMIT_MES,
                'erro': str(e)
            }
    
    # ========================================================================
    # RASTREAMENTO DE REQUISIÇÕES
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
        Registra requisição CBERS-4A no banco
        
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
                logger.info(f"✅ Requisição CBERS-4A registrada (ID: {requisicao_id})")
                return requisicao_id
                
        except Exception as e:
            logger.error(f"❌ Erro ao registrar CBERS-4A: {e}")
            return None
    
    def registrar_requisicao_google_maps(
        self,
        subestacao_id: int,
        tipo_requisicao: str,
        status: str,
        bbox: Tuple[float, float, float, float] = None,
        observacoes: str = None
    ) -> int:
        """
        Registra requisição Google Maps no banco
        
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
                logger.info(f"✅ Requisição Google Maps registrada (ID: {requisicao_id})")
                return requisicao_id
                
        except Exception as e:
            logger.error(f"❌ Erro ao registrar Google Maps: {e}")
            return None
    
    # ========================================================================
    # PREFERÊNCIAS
    # ========================================================================
    
    def _obter_preferencia_subestacao(self, subestacao_id: int) -> Optional[str]:
        """Obtém preferência de satélite armazenada para subestação"""
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
            logger.warning(f"⚠️ Erro ao obter preferência: {e}")
            return None
    
    def definir_preferencia_subestacao(
        self,
        subestacao_id: int,
        satelite_preferido: str = 'CBERS-4A'
    ) -> bool:
        """
        Define preferência de satélite para subestação
        
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
                
                logger.info(f"✅ Preferência definida para SE {subestacao_id}: {satelite_preferido}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erro ao definir preferência: {e}")
            return False
    
    # ========================================================================
    # ESTATÍSTICAS E MONITORAMENTO
    # ========================================================================
    
    def obter_estatisticas_satelite(self) -> Dict:
        """
        Obtém estatísticas completas de uso de satélites
        
        Returns:
            Dict com estatísticas por fonte
        """
        try:
            with self.engine.begin() as conn:
                # Google Maps
                result_gm = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucesso,
                        SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as erro
                    FROM requisicoes_satelite_google
                    WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM')
                """))
                row_gm = result_gm.fetchone()
                
                # CBERS-4A
                result_cb = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucesso,
                        SUM(CASE WHEN status = 'sem_cobertura' THEN 1 ELSE 0 END) as sem_cobertura,
                        AVG(cobertura_nuvem_percentual) as media_nuvem
                    FROM requisicoes_satelite_cbers4a
                    WHERE DATE_TRUNC('month', data_requisicao) = DATE_TRUNC('month', NOW())
                """))
                row_cb = result_cb.fetchone()
                
                return {
                    'google_maps': {
                        'total': row_gm[0] or 0,
                        'sucesso': row_gm[1] or 0,
                        'erro': row_gm[2] or 0,
                        'quota_limite': self.GOOGLE_MAPS_LIMIT_MES,
                        'percentual_usado': (row_gm[0] or 0) / self.GOOGLE_MAPS_LIMIT_MES * 100
                    },
                    'cbers4a': {
                        'total': row_cb[0] or 0,
                        'sucesso': row_cb[1] or 0,
                        'sem_cobertura': row_cb[2] or 0,
                        'media_cobertura_nuvem': float(row_cb[3]) if row_cb[3] else 0.0
                    },
                    'mes': datetime.now().strftime('%Y-%m')
                }
                
        except Exception as e:
            logger.error(f"❌ Erro ao obter estatísticas: {e}")
            return {}
