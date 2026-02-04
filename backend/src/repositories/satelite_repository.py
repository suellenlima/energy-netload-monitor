"""
Repository para operações de banco de dados de Satélite (CBERS-4A, Google Maps, etc)

Responsável por:
- SELECT de transformadores com coordenadas
- SELECT de subestações com coordenadas
- SELECT de áreas poligonais (transformador_area_cobertura)
- INSERT de histórico de requisições de satélite
- SELECT de histórico e estatísticas
- Queries geoespaciais (PostGIS)
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from sqlalchemy import text

from .base import BaseRepository


logger = logging.getLogger(__name__)


class SateliteRepository(BaseRepository):
    """
    Repositório para acesso aos dados de satélite e imagens.
    """

    # ========================================================================
    # TRANSFORMADORES
    # ========================================================================

    def obter_transformador_completo(self, transformador_id: int) -> Optional[Dict]:
        """
        Obtém dados completos de um transformador para processamento de satélite.
        
        Retorna:
        - Dados básicos: id, código, nome, distribuidora, tipo_tensão
        - Coordenadas: latitude, longitude, geometria
        - Dados técnicos: potência, tensão primária/secundária
        - Código de subestação associada
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, nome, distribuidora, subestacao_codigo,
                        latitude, longitude, localizacao::text as geom_wkt,
                        potencia_kva, tensao_primaria_kv, tensao_secundaria_kv,
                        tipo_tensao, ativo, data_criacao
                    FROM transformadores_aneel
                    WHERE id = :trans_id AND ativo = TRUE
                """), {'trans_id': transformador_id})
                
                row = result.fetchone()
                if not row:
                    logger.debug(f"Transformador {transformador_id} não encontrado")
                    return None
                
                return {
                    'id': row[0],
                    'codigo': row[1],
                    'nome': row[2],
                    'distribuidora': row[3],
                    'subestacao_codigo': row[4],
                    'latitude': float(row[5]) if row[5] else None,
                    'longitude': float(row[6]) if row[6] else None,
                    'geom_wkt': row[7],
                    'potencia_kva': float(row[8]) if row[8] else None,
                    'tensao_primaria_kv': float(row[9]) if row[9] else None,
                    'tensao_secundaria_kv': float(row[10]) if row[10] else None,
                    'tipo_tensao': row[11],
                    'ativo': row[12],
                    'data_criacao': row[13].isoformat() if row[13] else None
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter transformador {transformador_id}: {e}")
            raise

    def obter_coordenadas_transformador(self, transformador_id: int) -> Optional[Tuple[float, float]]:
        """
        Obtém apenas coordenadas de um transformador.
        
        Returns:
            (latitude, longitude) ou (None, None)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT latitude, longitude
                    FROM transformadores_aneel
                    WHERE id = :trans_id AND ativo = TRUE
                """), {'trans_id': transformador_id})
                
                row = result.fetchone()
                if not row or not row[0] or not row[1]:
                    return (None, None)
                
                return (float(row[0]), float(row[1]))
        
        except Exception as e:
            logger.error(f"Erro ao obter coordenadas do transformador {transformador_id}: {e}")
            raise

    # ========================================================================
    # SUBESTAÇÕES
    # ========================================================================

    def obter_subestacao_completa(self, subestacao_id: int) -> Optional[Dict]:
        """
        Obtém dados completos de uma subestação para processamento de satélite.
        
        Retorna:
        - Dados básicos: id, código, nome, distribuidora
        - Coordenadas: latitude, longitude, geometria
        - Dados técnicos: tensão
        - Código ONS (se disponível)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, nome, distribuidora,
                        latitude, longitude, localizacao::text as geom_wkt,
                        tensao_kv, tensao_operacao_kv,
                        codigo_ons, ativo, data_criacao
                    FROM subestacoes_aneel
                    WHERE id = :sub_id AND ativo = TRUE
                """), {'sub_id': subestacao_id})
                
                row = result.fetchone()
                if not row:
                    logger.debug(f"Subestação {subestacao_id} não encontrada")
                    return None
                
                return {
                    'id': row[0],
                    'codigo': row[1],
                    'nome': row[2],
                    'distribuidora': row[3],
                    'latitude': float(row[4]) if row[4] else None,
                    'longitude': float(row[5]) if row[5] else None,
                    'geom_wkt': row[6],
                    'tensao_kv': float(row[7]) if row[7] else None,
                    'tensao_operacao_kv': float(row[8]) if row[8] else None,
                    'codigo_ons': row[9],
                    'ativo': row[10],
                    'data_criacao': row[11].isoformat() if row[11] else None
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter subestação {subestacao_id}: {e}")
            raise

    def obter_coordenadas_subestacao(self, subestacao_id: int) -> Optional[Tuple[float, float]]:
        """
        Obtém apenas coordenadas de uma subestação.
        
        Returns:
            (latitude, longitude) ou (None, None)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT latitude, longitude
                    FROM subestacoes_aneel
                    WHERE id = :sub_id AND ativo = TRUE
                """), {'sub_id': subestacao_id})
                
                row = result.fetchone()
                if not row or not row[0] or not row[1]:
                    return (None, None)
                
                return (float(row[0]), float(row[1]))
        
        except Exception as e:
            logger.error(f"Erro ao obter coordenadas da subestação {subestacao_id}: {e}")
            raise

    # ========================================================================
    # ÁREAS POLIGONAIS
    # ========================================================================

    def obter_area_cobertura_transformador(self, transformador_codigo: str) -> Optional[Dict]:
        """
        Obtém área de cobertura (polígono) de um transformador.
        
        Retorna:
        - Area em km² e m²
        - Método de cálculo (convex_hull ou buffer)
        - Número de consumidores que formam a área
        - Geometria WKT
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, transformador_codigo, tipo_tensao,
                        metodo_calculo, area_m2, area_km2,
                        num_consumidores, num_vertices,
                        geom::text as geom_wkt, data_calculo
                    FROM transformador_area_cobertura
                    WHERE transformador_codigo = :codigo AND ativo = TRUE
                """), {'codigo': transformador_codigo})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'transformador_codigo': row[1],
                    'tipo_tensao': row[2],
                    'metodo_calculo': row[3],
                    'area_m2': float(row[4]) if row[4] else None,
                    'area_km2': float(row[5]) if row[5] else None,
                    'num_consumidores': row[6],
                    'num_vertices': row[7],
                    'geom_wkt': row[8],
                    'data_calculo': row[9].isoformat() if row[9] else None
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter área de cobertura: {e}")
            raise

    # ========================================================================
    # HISTÓRICO DE REQUISIÇÕES DE SATÉLITE
    # ========================================================================

    def registrar_requisicao_satelite(
        self,
        transformador_id: int,
        subestacao_id: int,
        fonte: str,  # 'cbers4a', 'google_maps', 'sentinel2', etc
        status: str,  # 'sucesso', 'sem_cobertura', 'erro', etc
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
        Registra uma requisição de satélite no banco de dados.
        
        Cria registro em: requisicoes_satelite_cbers4a (ou tabela genérica se existir)
        
        Args:
            transformador_id: ID do transformador
            subestacao_id: ID da subestação
            fonte: Fonte de satélite (cbers4a, google_maps, etc)
            status: Status da requisição
            url_imagem: URL da imagem (se obtida)
            imagem_id: ID da imagem na fonte
            data_aquisicao: Data de aquisição da imagem
            cobertura_nuvem_pct: Cobertura de nuvens (%)
            resolucao_metros: Resolução em metros
            tempo_requisicao_ms: Tempo da requisição em ms
            detalhes: Dict com detalhes adicionais
            custo_usd: Custo em USD (se aplicável)
        
        Returns:
            ID do registro criado
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO requisicoes_satelite_cbers4a
                    (transformador_id, subestacao_id, fonte_satelite, status,
                     imagem_id, url_download, data_imagem, 
                     cobertura_nuvem_percentual, resolucao_metros,
                     tempo_requisicao_ms, detalhes_json, custo_usd_estimado, 
                     data_requisicao)
                    VALUES 
                    (:trans_id, :sub_id, :fonte, :status,
                     :img_id, :url, :data_acq,
                     :cobertura, :resolucao,
                     :tempo, :detalhes, :custo,
                     NOW())
                    RETURNING id
                """), {
                    'trans_id': transformador_id,
                    'sub_id': subestacao_id,
                    'fonte': fonte,
                    'status': status,
                    'img_id': imagem_id,
                    'url': url_imagem,
                    'data_acq': data_aquisicao,
                    'cobertura': cobertura_nuvem_pct,
                    'resolucao': resolucao_metros,
                    'tempo': tempo_requisicao_ms,
                    'detalhes': json.dumps(detalhes) if detalhes else None,
                    'custo': custo_usd
                })
                
                req_id = result.scalar()
                logger.info(f"✓ Requisição {fonte} registrada: ID={req_id}, status={status}")
                return req_id
        
        except Exception as e:
            logger.error(f"Erro ao registrar requisição de satélite: {e}")
            raise

    def obter_historico_transformador(
        self,
        transformador_id: int,
        limite: int = 50,
        offset: int = 0,
        apenas_sucesso: bool = False
    ) -> List[Dict]:
        """
        Obtém histórico de requisições de satélite de um transformador.
        
        Args:
            transformador_id: ID do transformador
            limite: Máximo de registros
            offset: Deslocamento para paginação
            apenas_sucesso: Filtrar apenas requisições bem-sucedidas
        
        Returns:
            Lista de dicts com histórico
        """
        try:
            where_clause = "WHERE transformador_id = :trans_id"
            if apenas_sucesso:
                where_clause += " AND status = 'sucesso'"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT 
                        id, transformador_id, subestacao_id,
                        fonte_satelite, status, imagem_id,
                        url_download, data_imagem,
                        cobertura_nuvem_percentual, resolucao_metros,
                        tempo_requisicao_ms, custo_usd_estimado,
                        data_requisicao
                    FROM requisicoes_satelite_cbers4a
                    {where_clause}
                    ORDER BY data_requisicao DESC
                    LIMIT :limite OFFSET :offset
                """), {
                    'trans_id': transformador_id,
                    'limite': limite,
                    'offset': offset
                })
                
                registros = []
                for row in result:
                    registros.append({
                        'id': row[0],
                        'transformador_id': row[1],
                        'subestacao_id': row[2],
                        'fonte_satelite': row[3],
                        'status': row[4],
                        'imagem_id': row[5],
                        'url_download': row[6],
                        'data_imagem': row[7].isoformat() if row[7] else None,
                        'cobertura_nuvem_percentual': float(row[8]) if row[8] else None,
                        'resolucao_metros': float(row[9]) if row[9] else None,
                        'tempo_requisicao_ms': row[10],
                        'custo_usd_estimado': float(row[11]) if row[11] else None,
                        'data_requisicao': row[12].isoformat() if row[12] else None
                    })
                
                return registros
        
        except Exception as e:
            logger.error(f"Erro ao obter histórico do transformador: {e}")
            raise

    def obter_estatisticas_google_maps(self) -> Dict:
        """
        Obtém estatísticas de uso do Google Maps.
        
        Retorna:
        - Total de requisições
        - Total de transformadores únicos
        - Custo total
        - Requisições nos últimos 30 dias
        """
        try:
            with self.engine.connect() as conn:
                # Estatísticas gerais
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_requisicoes,
                        COUNT(DISTINCT transformador_id) as transformadores_unicos,
                        SUM(COALESCE(custo_usd_estimado, 0)) as custo_total,
                        COUNT(CASE WHEN status = 'sucesso' THEN 1 END) as sucesso,
                        COUNT(CASE WHEN status = 'erro' THEN 1 END) as erro
                    FROM requisicoes_satelite_cbers4a
                    WHERE fonte_satelite = 'google_maps'
                """))
                
                row = result.fetchone()
                
                return {
                    'total_requisicoes': row[0] or 0,
                    'transformadores_unicos': row[1] or 0,
                    'custo_total_usd': float(row[2]) if row[2] else 0.0,
                    'sucesso': row[3] or 0,
                    'erro': row[4] or 0,
                    'taxa_sucesso': (
                        (row[3] or 0) / (row[0] or 1) * 100
                    ) if row[0] else 0
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            raise

    def obter_quota_mes_atual(self) -> Dict:
        """
        Obtém quota de requisições do mês atual (Google Maps).
        
        Retorna:
        - Requisições usadas neste mês
        - Limite mensal
        - Percentual de uso
        - Custo estimado
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as requisicoes_mes,
                        SUM(COALESCE(custo_usd_estimado, 0)) as custo_mes,
                        EXTRACT(MONTH FROM NOW()) as mes,
                        EXTRACT(YEAR FROM NOW()) as ano
                    FROM requisicoes_satelite_cbers4a
                    WHERE fonte_satelite = 'google_maps'
                      AND DATE_TRUNC('month', data_requisicao) = DATE_TRUNC('month', NOW())
                """))
                
                row = result.fetchone()
                req_mes = row[0] or 0
                custo_mes = float(row[1]) if row[1] else 0.0
                
                limite_mensal = 25000
                percentual_uso = (req_mes / limite_mensal * 100) if limite_mensal > 0 else 0
                
                return {
                    'requisicoes_mes': req_mes,
                    'limite_mensal': limite_mensal,
                    'disponivel': limite_mensal - req_mes,
                    'percentual_uso': round(percentual_uso, 2),
                    'custo_mes_usd': round(custo_mes, 4),
                    'mes_ano': f"{int(row[3])}-{int(row[2]):02d}"
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter quota do mês: {e}")
            raise

    # ========================================================================
    # SUBESTAÇÕES COM DADOS DE SATÉLITE
    # ========================================================================

    def obter_subestacoes_por_distribuidor(self, distribuidora: str) -> List[Dict]:
        """
        Obtém todas as subestações de uma distribuidora com coordenadas.
        
        Returns:
            Lista de subestações com id, código, nome, lat, lon
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, nome, distribuidora,
                        latitude, longitude,
                        tensao_kv, codigo_ons
                    FROM subestacoes_aneel
                    WHERE distribuidora = :dist AND ativo = TRUE
                      AND latitude IS NOT NULL AND longitude IS NOT NULL
                    ORDER BY nome
                """), {'dist': distribuidora})
                
                subestacoes = []
                for row in result:
                    subestacoes.append({
                        'id': row[0],
                        'codigo': row[1],
                        'nome': row[2],
                        'distribuidora': row[3],
                        'latitude': float(row[4]),
                        'longitude': float(row[5]),
                        'tensao_kv': float(row[6]) if row[6] else None,
                        'codigo_ons': row[7]
                    })
                
                return subestacoes
        
        except Exception as e:
            logger.error(f"Erro ao obter subestações por distribuidora: {e}")
            raise


__all__ = ['SateliteRepository']
