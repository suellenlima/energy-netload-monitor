"""
Repository para Telhados Multi-Fonte
Responsável por: SELECT, INSERT de dados de telhados detectados via múltiplas fontes

Database Schema: ANEEL BDGD (infrastructure/database/schema_aneel_bdgd.sql)
Tabelas:
  - transformadores_aneel: Transformadores (READ)
  - subestacoes_aneel: Subestações (READ)
  - telhados_detectados_transformador: Telhados detectados (READ/WRITE)
  - aneel_bdgd_processamento: Log de processamento (WRITE)

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import json
import logging
from typing import Dict, Optional, List, Any, Tuple
from datetime import datetime

from sqlalchemy import text


class TelhadoMultiFonteRepository:
    """
    Repository para acesso aos dados de telhados multi-fonte.
    Responsável por: Operações SQL de leitura/escrita de detecções.
    """

    def __init__(self, engine):
        """Inicializa repository com engine SQLAlchemy."""
        self.engine = engine
        self.logger = logging.getLogger(__name__)

    # ========================================================================
    # RECUPERAR DADOS DO TRANSFORMADOR
    # ========================================================================

    def obter_transformador(self, transformador_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtém dados completos de um transformador.
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Dict com dados do transformador ou None
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, nome, distribuidora, subestacao_codigo,
                        latitude, longitude, potencia_kva, tipo_tensao,
                        ativo, data_criacao
                    FROM transformadores_aneel
                    WHERE id = :id
                """), {'id': transformador_id})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'codigo': row[1],
                    'nome': row[2],
                    'distribuidora': row[3],
                    'subestacao_codigo': row[4],
                    'latitude': float(row[5]) if row[5] else None,
                    'longitude': float(row[6]) if row[6] else None,
                    'potencia_kva': float(row[7]) if row[7] else None,
                    'tipo_tensao': row[8],
                    'ativo': row[9],
                    'data_criacao': row[10].isoformat() if row[10] else None
                }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter transformador {transformador_id}: {e}")
            raise

    # ========================================================================
    # RECUPERAR SUBESTAÇÃO ASSOCIADA
    # ========================================================================

    def obter_subestacao(self, subestacao_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtém dados de uma subestação.
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            Dict com dados da subestação ou None
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, nome, distribuidora, latitude, longitude,
                        tensao_kv, ativo
                    FROM subestacoes_aneel
                    WHERE id = :id
                """), {'id': subestacao_id})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'codigo': row[1],
                    'nome': row[2],
                    'distribuidora': row[3],
                    'latitude': float(row[4]) if row[4] else None,
                    'longitude': float(row[5]) if row[5] else None,
                    'tensao_kv': float(row[6]) if row[6] else None,
                    'ativo': row[7]
                }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter subestação {subestacao_id}: {e}")
            raise

    # ========================================================================
    # SALVAR TELHADOS DETECTADOS
    # ========================================================================

    def salvar_telhados_detectados(
        self,
        transformador_id: int,
        subestacao_id: int,
        telhados: List[Dict[str, Any]],
        fonte_imagem: str = 'google_maps',
        url_imagem_origem: Optional[str] = None
    ) -> List[int]:
        """
        Salva uma leva de telhados detectados.
        
        Args:
            transformador_id: ID do transformador
            subestacao_id: ID da subestação
            telhados: Lista de dicts com dados dos telhados
            fonte_imagem: Origem da imagem (google_maps, cbers4a, etc)
            url_imagem_origem: URL da imagem original
        
        Returns:
            Lista de IDs dos telhados inseridos
        """
        try:
            telhados_ids = []
            
            with self.engine.begin() as conn:
                for telhado in telhados:
                    result = conn.execute(text("""
                        INSERT INTO telhados_detectados_transformador
                        (transformador_id, subestacao_id, latitude, longitude,
                         area_m2, confianca, bbox_json, fonte_imagem, 
                         resolucao_cm, timestamp_deteccao, url_imagem_origem)
                        VALUES 
                        (:trans_id, :sub_id, :lat, :lon, :area, :conf,
                         :bbox, :fonte, :res, NOW(), :url)
                        RETURNING id
                    """), {
                        'trans_id': transformador_id,
                        'sub_id': subestacao_id,
                        'lat': telhado.get('latitude'),
                        'lon': telhado.get('longitude'),
                        'area': telhado.get('area_m2'),
                        'conf': telhado.get('confianca'),
                        'bbox': json.dumps(telhado.get('bbox', {})),
                        'fonte': fonte_imagem,
                        'res': telhado.get('resolucao_cm', 30.0),
                        'url': url_imagem_origem
                    })
                    
                    telhado_id = result.scalar()
                    telhados_ids.append(telhado_id)
            
            self.logger.info(f"Salvos {len(telhados_ids)} telhados para transformador {transformador_id}")
            return telhados_ids
        
        except Exception as e:
            self.logger.error(f"Erro ao salvar telhados detectados: {e}")
            raise

    # ========================================================================
    # REGISTRAR PROCESSAMENTO
    # ========================================================================

    def registrar_processamento(
        self,
        transformador_id: int,
        subestacao_id: int,
        distribuidora: str,
        fonte_utilizada: str,
        telhados_detectados: int,
        sucesso: bool,
        url_imagem: str,
        mensagem: str = "",
        detalhes: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Registra execução de detecção de telhados.
        
        Args:
            transformador_id: ID do transformador
            subestacao_id: ID da subestação
            distribuidora: Nome da distribuidora
            fonte_utilizada: Fonte de imagem utilizada
            telhados_detectados: Quantidade detectada
            sucesso: Se foi bem-sucedido
            url_imagem: URL da imagem processada
            mensagem: Mensagem descritiva
            detalhes: Dict com detalhes adicionais
        
        Returns:
            ID do registro de processamento
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO aneel_bdgd_processamento
                    (distribuidora_processada, transformadores_inseridos, status, 
                     mensagem_erro, parametros_execucao, data_fim, tempo_total_segundos)
                    VALUES 
                    (:dist, :qty, :status, :msg, :params, NOW(), 0)
                    RETURNING id
                """), {
                    'dist': distribuidora,
                    'qty': telhados_detectados,
                    'status': 'concluido' if sucesso else 'erro',
                    'msg': mensagem,
                    'params': json.dumps({
                        'transformador_id': transformador_id,
                        'subestacao_id': subestacao_id,
                        'fonte': fonte_utilizada,
                        'url': url_imagem,
                        'detalhes': detalhes or {}
                    })
                })
                
                proc_id = result.scalar()
                return proc_id
        
        except Exception as e:
            self.logger.error(f"Erro ao registrar processamento: {e}")
            raise

    # ========================================================================
    # VERIFICAR TELHADOS JÁ DETECTADOS
    # ========================================================================

    def obter_telhados_transformador(self, transformador_id: int) -> List[Dict]:
        """
        Obtém telhados já detectados para um transformador.
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Lista de telhados
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, latitude, longitude, area_m2, confianca, 
                        fonte_imagem, timestamp_deteccao
                    FROM telhados_detectados_transformador
                    WHERE transformador_id = :trans_id
                    ORDER BY timestamp_deteccao DESC
                    LIMIT 1000
                """), {'trans_id': transformador_id})
                
                telhados = []
                for row in result:
                    telhados.append({
                        'id': row[0],
                        'latitude': float(row[1]),
                        'longitude': float(row[2]),
                        'area_m2': float(row[3]),
                        'confianca': float(row[4]),
                        'fonte_imagem': row[5],
                        'timestamp_deteccao': row[6].isoformat() if row[6] else None
                    })
                
                return telhados
        
        except Exception as e:
            self.logger.error(f"Erro ao obter telhados do transformador {transformador_id}: {e}")
            raise

    # ========================================================================
    # OBTER COORDENADAS PARA GERACAO DE URLS
    # ========================================================================

    def obter_coordenadas_transformador(
        self, 
        transformador_id: int
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Obtém latitude e longitude do transformador para gerar URLs.
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Tupla (latitude, longitude) ou (None, None)
        """
        try:
            trafo = self.obter_transformador(transformador_id)
            
            if trafo and trafo['latitude'] and trafo['longitude']:
                return (trafo['latitude'], trafo['longitude'])
            
            return (None, None)
        
        except Exception as e:
            self.logger.error(f"Erro ao obter coordenadas do transformador: {e}")
            raise
