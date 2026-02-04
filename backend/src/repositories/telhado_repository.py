"""
Repository para Telhados
Responsável por: SELECT, INSERT, UPDATE, DELETE de telhados

Database Schema: ANEEL BDGD (infrastructure/database/schema_aneel_bdgd.sql)
Tabelas:
  - telhados_detectados_transformador: Telhados detectados (READ/WRITE)
  - transformadores_aneel: Transformadores (READ)
  - subestacoes_aneel: Subestações (READ)

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import json
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime

from sqlalchemy import text


class TelhadoRepository:
    """
    Repository para acesso aos dados de telhados.
    Responsável por: Operações SQL de leitura/escrita.
    """

    def __init__(self, engine):
        """Inicializa repository com engine SQLAlchemy."""
        self.engine = engine
        self.logger = logging.getLogger(__name__)

    # ========================================================================
    # LISTAR TELHADOS COM FILTROS
    # ========================================================================

    def listar_telhados_com_filtros(
        self,
        id_subestacao: Optional[str] = None,
        tipo_edificio: Optional[str] = None,
        confianca_minima: float = 0.0,
        pagina: int = 1,
        limite: int = 100
    ) -> Dict[str, Any]:
        """
        Lista telhados com filtros e paginação.
        
        Args:
            id_subestacao: Filtro por subestação (opcional)
            tipo_edificio: Filtro por tipo (opcional)
            confianca_minima: Confiança mínima (0.0-1.0)
            pagina: Número da página (padrão: 1)
            limite: Itens por página (padrão: 100)
        
        Returns:
            Dict com total, página, total_páginas e lista de telhados
        """
        try:
            with self.engine.connect() as conn:
                # Montar WHERE dinâmico
                where_clauses = ["1=1"]
                params = {}
                
                if id_subestacao:
                    where_clauses.append("th.subestacao_id = :sub_id")
                    params['sub_id'] = id_subestacao
                
                if confianca_minima > 0:
                    where_clauses.append("th.confianca >= :conf_min")
                    params['conf_min'] = confianca_minima
                
                where_clause = " AND ".join(where_clauses)
                
                # Contar total
                result_count = conn.execute(text(f"""
                    SELECT COUNT(*) as total
                    FROM telhados_detectados_transformador th
                    WHERE {where_clause}
                """), params)
                
                total = result_count.scalar() or 0
                
                # Calcular paginação
                offset = (pagina - 1) * limite
                total_paginas = (total + limite - 1) // limite
                
                # Buscar dados
                result = conn.execute(text(f"""
                    SELECT 
                        th.id, th.transformador_id, th.subestacao_id, 
                        th.latitude, th.longitude, th.area_m2, 
                        th.confianca, th.bbox_json, 
                        th.fonte_imagem, th.timestamp_deteccao,
                        t.codigo as transformador_codigo,
                        s.codigo as subestacao_codigo
                    FROM telhados_detectados_transformador th
                    LEFT JOIN transformadores_aneel t ON th.transformador_id = t.id
                    LEFT JOIN subestacoes_aneel s ON th.subestacao_id = s.id
                    WHERE {where_clause}
                    ORDER BY th.timestamp_deteccao DESC
                    LIMIT :limite OFFSET :offset
                """), {**params, 'limite': limite, 'offset': offset})
                
                telhados = []
                for row in result:
                    telhados.append({
                        'id': row[0],
                        'transformador_id': row[1],
                        'subestacao_id': row[2],
                        'latitude': float(row[3]),
                        'longitude': float(row[4]),
                        'area_m2': float(row[5]),
                        'confianca': float(row[6]),
                        'bbox_json': json.loads(row[7]) if row[7] else {},
                        'fonte_imagem': row[8],
                        'timestamp_deteccao': row[9].isoformat() if row[9] else None,
                        'transformador_codigo': row[10],
                        'subestacao_codigo': row[11]
                    })
                
                return {
                    'total': total,
                    'pagina': pagina,
                    'limite': limite,
                    'total_paginas': total_paginas,
                    'telhados': telhados
                }
                
        except Exception as e:
            self.logger.error(f"Erro ao listar telhados: {e}")
            raise

    # ========================================================================
    # OBTER TELHADOS DE UMA SUBESTAÇÃO
    # ========================================================================

    def obter_telhados_subestacao(self, subestacao_id: int) -> List[Dict]:
        """
        Obtém todos os telhados de uma subestação.
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            Lista de telhados
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        th.id, th.transformador_id, th.subestacao_id,
                        th.latitude, th.longitude, th.area_m2,
                        th.confianca, th.bbox_json, th.timestamp_deteccao,
                        t.codigo as transformador_codigo
                    FROM telhados_detectados_transformador th
                    LEFT JOIN transformadores_aneel t ON th.transformador_id = t.id
                    WHERE th.subestacao_id = :sub_id
                    ORDER BY th.timestamp_deteccao DESC
                    LIMIT 1000
                """), {'sub_id': subestacao_id})
                
                telhados = []
                for row in result:
                    telhados.append({
                        'id': row[0],
                        'transformador_id': row[1],
                        'subestacao_id': row[2],
                        'latitude': float(row[3]),
                        'longitude': float(row[4]),
                        'area_m2': float(row[5]),
                        'confianca': float(row[6]),
                        'bbox_json': json.loads(row[7]) if row[7] else {},
                        'timestamp_deteccao': row[8].isoformat() if row[8] else None,
                        'transformador_codigo': row[9]
                    })
                
                return telhados
                
        except Exception as e:
            self.logger.error(f"Erro ao obter telhados da subestação {subestacao_id}: {e}")
            raise

    # ========================================================================
    # OBTER TELHADOS DE UM TRANSFORMADOR
    # ========================================================================

    def obter_telhados_transformador(self, transformador_id: int, limite: int = 100) -> List[Dict]:
        """
        Obtém todos os telhados de um transformador.
        
        Args:
            transformador_id: ID do transformador
            limite: Máximo de resultados (padrão: 100)
        
        Returns:
            Lista de telhados
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, transformador_id, subestacao_id,
                        latitude, longitude, area_m2, confianca,
                        bbox_json, timestamp_deteccao, fonte_imagem
                    FROM telhados_detectados_transformador
                    WHERE transformador_id = :trans_id
                    ORDER BY timestamp_deteccao DESC
                    LIMIT :limite
                """), {'trans_id': transformador_id, 'limite': limite})
                
                telhados = []
                for row in result:
                    telhados.append({
                        'id': row[0],
                        'transformador_id': row[1],
                        'subestacao_id': row[2],
                        'latitude': float(row[3]),
                        'longitude': float(row[4]),
                        'area_m2': float(row[5]),
                        'confianca': float(row[6]),
                        'bbox_json': json.loads(row[7]) if row[7] else {},
                        'timestamp_deteccao': row[8].isoformat() if row[8] else None,
                        'fonte_imagem': row[9]
                    })
                
                return telhados
                
        except Exception as e:
            self.logger.error(f"Erro ao obter telhados do transformador {transformador_id}: {e}")
            raise

    # ========================================================================
    # ESTATÍSTICAS AGREGADAS
    # ========================================================================

    def obter_estatisticas_telhados(self, periodo: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtém estatísticas agregadas de telhados.
        
        Args:
            periodo: Período no formato YYYY-MM (opcional)
        
        Returns:
            Dict com estatísticas
        """
        try:
            with self.engine.connect() as conn:
                # Query principal de estatísticas
                result = conn.execute(text("""
                    SELECT 
                        COUNT(DISTINCT transformador_id) as total_transformadores,
                        COUNT(DISTINCT subestacao_id) as total_subestacoes,
                        COUNT(*) as total_telhados,
                        SUM(area_m2) as area_total_m2,
                        AVG(confianca) as confianca_media,
                        MIN(confianca) as confianca_minima,
                        MAX(confianca) as confianca_maxima,
                        AVG(area_m2) as area_media_m2,
                        COUNT(CASE WHEN confianca >= 0.95 THEN 1 END) as telhados_alta_conf,
                        COUNT(CASE WHEN confianca < 0.70 THEN 1 END) as telhados_baixa_conf
                    FROM telhados_detectados_transformador
                """))
                
                row = result.fetchone()
                
                stats = {
                    'total_transformadores': row[0] or 0,
                    'total_subestacoes': row[1] or 0,
                    'total_telhados': row[2] or 0,
                    'area_total_m2': float(row[3]) if row[3] else 0,
                    'confianca_media': float(row[4]) if row[4] else 0,
                    'confianca_minima': float(row[5]) if row[5] else 0,
                    'confianca_maxima': float(row[6]) if row[6] else 0,
                    'area_media_m2': float(row[7]) if row[7] else 0,
                    'telhados_alta_confianca': row[8] or 0,
                    'telhados_baixa_confianca': row[9] or 0,
                    'periodo': periodo or datetime.now().strftime("%Y-%m"),
                    'timestamp': datetime.now().isoformat()
                }
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            raise

    # ========================================================================
    # ESTATÍSTICAS POR SUBESTAÇÃO
    # ========================================================================

    def obter_estatisticas_subestacao(self, subestacao_id: int) -> Dict[str, Any]:
        """
        Obtém estatísticas de telhados para uma subestação.
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            Dict com estatísticas
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(DISTINCT transformador_id) as transformadores,
                        COUNT(*) as total_telhados,
                        SUM(area_m2) as area_total_m2,
                        AVG(confianca) as confianca_media,
                        MIN(confianca) as confianca_min,
                        MAX(confianca) as confianca_max
                    FROM telhados_detectados_transformador
                    WHERE subestacao_id = :sub_id
                """), {'sub_id': subestacao_id})
                
                row = result.fetchone()
                
                if not row or not row[0]:
                    return {
                        'subestacao_id': subestacao_id,
                        'transformadores': 0,
                        'total_telhados': 0,
                        'area_total_m2': 0,
                        'confianca_media': 0,
                        'timestamp': datetime.now().isoformat()
                    }
                
                return {
                    'subestacao_id': subestacao_id,
                    'transformadores': row[0] or 0,
                    'total_telhados': row[1] or 0,
                    'area_total_m2': float(row[2]) if row[2] else 0,
                    'confianca_media': float(row[3]) if row[3] else 0,
                    'confianca_min': float(row[4]) if row[4] else 0,
                    'confianca_max': float(row[5]) if row[5] else 0,
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas da subestação {subestacao_id}: {e}")
            raise

    # ========================================================================
    # OBTER DETALHES DE UM TELHADO
    # ========================================================================

    def obter_telhado_por_id(self, telhado_id: int) -> Optional[Dict]:
        """
        Obtém detalhes de um telhado específico.
        
        Args:
            telhado_id: ID do telhado
        
        Returns:
            Dict com dados do telhado ou None
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        th.id, th.transformador_id, th.subestacao_id,
                        th.latitude, th.longitude, th.area_m2,
                        th.confianca, th.bbox_json, th.timestamp_deteccao,
                        th.fonte_imagem, th.resolucao_cm,
                        t.codigo as transformador_codigo,
                        s.codigo as subestacao_codigo
                    FROM telhados_detectados_transformador th
                    LEFT JOIN transformadores_aneel t ON th.transformador_id = t.id
                    LEFT JOIN subestacoes_aneel s ON th.subestacao_id = s.id
                    WHERE th.id = :telhado_id
                """), {'telhado_id': telhado_id})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'transformador_id': row[1],
                    'subestacao_id': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4]),
                    'area_m2': float(row[5]),
                    'confianca': float(row[6]),
                    'bbox_json': json.loads(row[7]) if row[7] else {},
                    'timestamp_deteccao': row[8].isoformat() if row[8] else None,
                    'fonte_imagem': row[9],
                    'resolucao_cm': float(row[10]) if row[10] else None,
                    'transformador_codigo': row[11],
                    'subestacao_codigo': row[12]
                }
                
        except Exception as e:
            self.logger.error(f"Erro ao obter telhado {telhado_id}: {e}")
            raise

    # ========================================================================
    # SALVAR TELHADO (INSERT)
    # ========================================================================

    def salvar_telhado(self, dados_telhado: Dict) -> int:
        """
        Salva um novo telhado no banco.
        
        Args:
            dados_telhado: Dict com dados do telhado
                Campos esperados: transformador_id, subestacao_id, latitude, longitude,
                                 area_m2, confianca, bbox_json, fonte_imagem, etc.
        
        Returns:
            ID do telhado inserido
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO telhados_detectados_transformador
                    (transformador_id, subestacao_id, latitude, longitude,
                     area_m2, confianca, bbox_json, fonte_imagem, timestamp_deteccao)
                    VALUES (:trans_id, :sub_id, :lat, :lon, :area, :conf,
                            :bbox, :fonte, NOW())
                    RETURNING id
                """), {
                    'trans_id': dados_telhado.get('transformador_id'),
                    'sub_id': dados_telhado.get('subestacao_id'),
                    'lat': dados_telhado.get('latitude'),
                    'lon': dados_telhado.get('longitude'),
                    'area': dados_telhado.get('area_m2'),
                    'conf': dados_telhado.get('confianca'),
                    'bbox': json.dumps(dados_telhado.get('bbox_json', {})),
                    'fonte': dados_telhado.get('fonte_imagem', 'google_maps')
                })
                
                telhado_id = result.scalar()
                self.logger.info(f"Telhado {telhado_id} salvo com sucesso")
                return telhado_id
                
        except Exception as e:
            self.logger.error(f"Erro ao salvar telhado: {e}")
            raise

    # ========================================================================
    # DELETAR TELHADO
    # ========================================================================

    def deletar_telhado(self, telhado_id: int) -> bool:
        """
        Deleta um telhado.
        
        Args:
            telhado_id: ID do telhado
        
        Returns:
            True se deletado com sucesso
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    DELETE FROM telhados_detectados_transformador
                    WHERE id = :telhado_id
                """), {'telhado_id': telhado_id})
                
                rows_deleted = result.rowcount
                self.logger.info(f"Deletados {rows_deleted} telhado(s) com ID {telhado_id}")
                return rows_deleted > 0
                
        except Exception as e:
            self.logger.error(f"Erro ao deletar telhado {telhado_id}: {e}")
            raise
