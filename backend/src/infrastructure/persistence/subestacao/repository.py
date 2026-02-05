"""Infrastructure Layer - Subestacao SQLAlchemy Repository"""

from typing import List, Optional, Dict, Any
from sqlalchemy import text
from src.domain.subestacao import (
    ISubestacaoRepository,
    Subestacao,
    CodigoSubestacao,
    SubestacaoNotFoundError,
)
from src.infrastructure.persistence.subestacao.mapper import SubestacaoMapper
from src.core.database import get_db_connection


class SQLAlchemySubestacaoRepository(ISubestacaoRepository):
    """SQLAlchemy implementation of Subestacao repository using real ANEEL data"""
    
    def obter_por_codigo(self, codigo: str) -> Optional[Subestacao]:
        """Obtém subestação por código - usa dados reais ANEEL"""
        try:
            conn = get_db_connection()
            # Convert CodigoSubestacao to string if needed
            codigo_str = str(codigo) if hasattr(codigo, 'valor') else str(codigo)
            result = conn.execute(text("""
                SELECT 
                    id,
                    codigo,
                    nome,
                    tensao_kv as tensao_nominal_kv,
                    NULL as potencia_nominal_mva,
                    NULL as area_cobertura_km2,
                    latitude,
                    longitude,
                    dist_codigo as distribuidora_codigo,
                    distribuidora as distribuidora_nome,
                    ativo,
                    data_criacao as timestamp_criacao,
                    data_atualizacao as timestamp_atualizacao
                FROM subestacoes_aneel
                WHERE codigo = :codigo AND ativo = true
                LIMIT 1
            """), {"codigo": codigo_str})
            
            row = result.fetchone()
            if not row:
                return None
            
            row_dict = dict(row._mapping)
            return SubestacaoMapper.to_domain(row_dict)
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao buscar subestação: {str(e)}")
    
    def listar_paginados(self, offset: int, limite: int) -> List[Subestacao]:
        """Lista subestações paginadas - usa dados reais ANEEL"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    id,
                    codigo,
                    nome,
                    tensao_kv as tensao_nominal_kv,
                    NULL as potencia_nominal_mva,
                    NULL as area_cobertura_km2,
                    latitude,
                    longitude,
                    dist_codigo as distribuidora_codigo,
                    distribuidora as distribuidora_nome,
                    ativo,
                    data_criacao as timestamp_criacao,
                    data_atualizacao as timestamp_atualizacao
                FROM subestacoes_aneel
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY codigo ASC
                LIMIT :limite OFFSET :offset
            """), {"limite": limite, "offset": offset})
            
            rows = result.fetchall()
            result_list = []
            for row in rows:
                row_dict = dict(row._mapping)
                result_list.append(SubestacaoMapper.to_domain(row_dict))
            return result_list
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao listar subestações: {str(e)}")
    
    def listar_por_tensao(
        self,
        tensao_nominal_kv: float,
        offset: int,
        limite: int
    ) -> List[Subestacao]:
        """Lista subestações por tensão nominal - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    id,
                    codigo,
                    nome,
                    tensao_kv as tensao_nominal_kv,
                    NULL as potencia_nominal_mva,
                    NULL as area_cobertura_km2,
                    latitude,
                    longitude,
                    dist_codigo as distribuidora_codigo,
                    distribuidora as distribuidora_nome,
                    ativo,
                    data_criacao as timestamp_criacao,
                    data_atualizacao as timestamp_atualizacao
                FROM subestacoes_aneel
                WHERE tensao_kv = :tensao_nominal_kv AND ativo = true 
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY codigo ASC
                LIMIT :limite OFFSET :offset
            """), {"tensao_nominal_kv": tensao_nominal_kv, "limite": limite, "offset": offset})
            
            rows = result.fetchall()
            result_list = []
            for row in rows:
                row_dict = dict(row._mapping)
                result_list.append(SubestacaoMapper.to_domain(row_dict))
            return result_list
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao listar por tensão: {str(e)}")
    
    def listar_por_distribuidora(
        self,
        distribuidora_codigo: str,
        offset: int,
        limite: int
    ) -> List[Subestacao]:
        """Lista subestações por distribuidora - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    id,
                    codigo,
                    nome,
                    tensao_kv as tensao_nominal_kv,
                    NULL as potencia_nominal_mva,
                    NULL as area_cobertura_km2,
                    latitude,
                    longitude,
                    dist_codigo as distribuidora_codigo,
                    distribuidora as distribuidora_nome,
                    ativo,
                    data_criacao as timestamp_criacao,
                    data_atualizacao as timestamp_atualizacao
                FROM subestacoes_aneel
                WHERE dist_codigo = :distribuidora_codigo AND ativo = true 
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY codigo ASC
                LIMIT :limite OFFSET :offset
            """), {"distribuidora_codigo": distribuidora_codigo, "limite": limite, "offset": offset})
            
            rows = result.fetchall()
            result_list = []
            for row in rows:
                row_dict = dict(row._mapping)
                result_list.append(SubestacaoMapper.to_domain(row_dict))
            return result_list
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao listar por distribuidora: {str(e)}")
    
    def contar_total(self) -> int:
        """Conta total de subestações - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT COUNT(*) as total 
                FROM subestacoes_aneel 
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
            """))
            row = result.fetchone()
            return row[0] if row else 0
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao contar subestações: {str(e)}")
    
    def contar_por_distribuidora(self, distribuidora_codigo: str) -> int:
        """Conta subestações por distribuidora - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT COUNT(*) as total 
                FROM subestacoes_aneel 
                WHERE dist_codigo = :distribuidora_codigo AND ativo = true 
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
            """), {"distribuidora_codigo": distribuidora_codigo})
            
            row = result.fetchone()
            return row[0] if row else 0
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao contar por distribuidora: {str(e)}")
    
    def obter_estatisticas_gerais(self) -> Dict[str, Any]:
        """Obtém estatísticas gerais das subestações - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            
            # Total de subestações - dados reais ANEEL
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM subestacoes_aneel 
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
            """))
            total = result.fetchone()[0]
            
            # Contagem por tipo de tensão
            result = conn.execute(text("""
                SELECT 
                    CASE
                        WHEN tensao_kv >= 230 THEN 'AT'
                        WHEN tensao_kv >= 69 THEN 'MT'
                        ELSE 'BT'
                    END as tipo_tensao,
                    COUNT(*) as quantidade
                FROM subestacoes_aneel
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY tipo_tensao
            """))
            tipo_tensao_data = result.fetchall()
            por_tipo_tensao = {row[0]: row[1] for row in tipo_tensao_data}
            
            # Contagem por distribuidora
            result = conn.execute(text("""
                SELECT 
                    dist_codigo,
                    COUNT(*) as quantidade
                FROM subestacoes_aneel
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY dist_codigo
                ORDER BY quantidade DESC
            """))
            distribuidoras_data = result.fetchall()
            por_distribuidora = {row[0]: row[1] for row in distribuidoras_data}
            
            # Estatísticas de tensão
            result = conn.execute(text("""
                SELECT 
                    MIN(tensao_kv) as min_tensao,
                    MAX(tensao_kv) as max_tensao,
                    AVG(tensao_kv) as media_tensao,
                    COUNT(DISTINCT tensao_kv) as diferentes_tensoes
                FROM subestacoes_aneel
                WHERE ativo = true AND latitude IS NOT NULL AND longitude IS NOT NULL
            """))
            tensao_data = result.fetchone()
            
            return {
                'total_subestacoes': total,
                'por_tipo_tensao': por_tipo_tensao,
                'por_distribuidora': por_distribuidora,
                'tensao_nominal': {
                    'minima': tensao_data[0],
                    'maxima': tensao_data[1],
                    'media': tensao_data[2],
                    'diferentes_niveis': tensao_data[3],
                }
            }
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao obter estatísticas: {str(e)}")
