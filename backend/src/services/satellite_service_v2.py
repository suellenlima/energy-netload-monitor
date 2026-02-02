"""
SatelliteServiceV2 - Controle Inteligente de Fontes de Satélites
Gerencia priorização entre CBERS-4A (Brasil) e Google Maps
Com rastreamento de requisições e controle de quota mensal
Busca por Subestação e Transformador
"""

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
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
