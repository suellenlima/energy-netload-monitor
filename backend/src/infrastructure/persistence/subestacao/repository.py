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
    
    def obter_mix_consumidores(self, subestacao_id: int) -> Dict[str, Any]:
        """Obtém mix de consumidores por subestação - dados reais ANEEL"""
        try:
            conn = get_db_connection()
            
            # Primeiro, buscar o código da subestação pelo ID
            result = conn.execute(text("""
                SELECT codigo FROM subestacoes_aneel WHERE id = :subestacao_id
            """), {"subestacao_id": subestacao_id})
            row = result.fetchone()
            if not row:
                return {
                    'subestacao_id': subestacao_id,
                    'total_ucs': 0,
                    'total_instalacoes': 0,
                    'potencia_total_mw': 0.0,
                    'mix_por_classe': {}
                }
            
            subestacao_codigo = row[0]
            
            # Buscar consumidores de AT
            result_at = conn.execute(text("""
                SELECT 
                    classe_subclasse_codigo,
                    COUNT(*) as qtd_ucs,
                    SUM(COALESCE(carga_instalada_kw, 0)) as potencia_kw
                FROM consumidores_at_aneel
                WHERE subestacao_codigo = :subestacao_codigo
                GROUP BY classe_subclasse_codigo
            """), {"subestacao_codigo": subestacao_codigo})
            
            # Buscar consumidores de MT
            result_mt = conn.execute(text("""
                SELECT 
                    classe_subclasse_codigo,
                    COUNT(*) as qtd_ucs,
                    SUM(COALESCE(carga_instalada_kw, 0)) as potencia_kw
                FROM consumidores_mt_aneel
                WHERE subestacao_codigo = :subestacao_codigo
                GROUP BY classe_subclasse_codigo
            """), {"subestacao_codigo": subestacao_codigo})
            
            # Buscar consumidores de BT
            result_bt = conn.execute(text("""
                SELECT 
                    classe_subclasse_codigo,
                    COUNT(*) as qtd_ucs,
                    SUM(COALESCE(carga_instalada_kw, 0)) as potencia_kw
                FROM consumidores_bt_aneel
                WHERE subestacao_codigo = :subestacao_codigo
                GROUP BY classe_subclasse_codigo
            """), {"subestacao_codigo": subestacao_codigo})
            
            # Consolidar dados
            mix_por_classe = {}
            total_ucs = 0
            total_instalacoes = 0
            potencia_total_kw = 0.0
            
            # Mapear códigos de classe e subclasse para nomes amigáveis
            classe_map = {
                'R': 'Residencial',
                'I': 'Industrial',
                'C': 'Comercial',
                'RU': 'Rural',
                'PP': 'Poder Público',
                'IP': 'Iluminação Pública',
                'SP': 'Serviço Público',
                'CP': 'Consumo Próprio',
                '1': 'Residencial',
                '2': 'Industrial',
                '3': 'Comercial',
                '4': 'Rural',
                '5': 'Poder Público',
                '6': 'Iluminação Pública',
                '7': 'Serviço Público',
                '8': 'Consumo Próprio'
            }
            
            # Mapear subclasses para tipos de estabelecimento
            tipo_map = {
                'R1': 'Residencial Baixa Renda',
                'R2': 'Residencial',
                'R3': 'Residencial Alta Renda',
                'I1': 'Industrial',
                'I2': 'Industrial Grande Porte',
                'C1': 'Comercial',
                'C2': 'Comercial Serviços',
                'C3': 'Comercial Outros',
                'C9': 'Comercial Diversos',
                'CO1': 'Comercial',
                'CO2': 'Comercial Serviços',
                'CO3': 'Comercial Outros',
                'CO9': 'Comercial Diversos',
                'RU1': 'Rural Agropecuária',
                'RU2': 'Rural Cooperativa',
                'RU3': 'Rural Irrigação',
                'PP1': 'Poder Público',
                'PP2': 'Serviço Público',
                'IP': 'Iluminação Pública',
            }
            
            for result in [result_at, result_mt, result_bt]:
                for row in result:
                    classe_codigo = row[0] if row[0] else ''
                    qtd_ucs = int(row[1])
                    potencia_kw = float(row[2]) if row[2] else 0.0
                    
                    # Extrair prefixo da classe (primeiras letras ou primeiro dígito)
                    if classe_codigo:
                        # Tentar extrair letras primeiro (ex: CO, PP, RU)
                        prefixo = ''.join([c for c in classe_codigo if c.isalpha()])
                        if not prefixo:
                            # Se não houver letras, usar primeiro dígito
                            prefixo = classe_codigo[0]
                    else:
                        prefixo = '0'
                    
                    classe_nome = classe_map.get(prefixo, 'Outros')
                    tipo_estabelecimento = tipo_map.get(classe_codigo, classe_codigo or 'Não especificado')
                    
                    # Inicializar classe se não existir
                    if classe_nome not in mix_por_classe:
                        mix_por_classe[classe_nome] = {
                            'qtd_unidades_consumidoras': 0,
                            'qtd_instalacoes': 0,
                            'potencia_total_mw': 0.0,
                            'por_tipo': {}
                        }
                    
                    # Inicializar tipo se não existir
                    if tipo_estabelecimento not in mix_por_classe[classe_nome]['por_tipo']:
                        mix_por_classe[classe_nome]['por_tipo'][tipo_estabelecimento] = {
                            'qtd_unidades_consumidoras': 0,
                            'qtd_instalacoes': 0,
                            'potencia_total_mw': 0.0
                        }
                    
                    # Atualizar totais da classe
                    mix_por_classe[classe_nome]['qtd_unidades_consumidoras'] += qtd_ucs
                    mix_por_classe[classe_nome]['qtd_instalacoes'] += qtd_ucs
                    mix_por_classe[classe_nome]['potencia_total_mw'] += potencia_kw / 1000.0
                    
                    # Atualizar totais do tipo
                    mix_por_classe[classe_nome]['por_tipo'][tipo_estabelecimento]['qtd_unidades_consumidoras'] += qtd_ucs
                    mix_por_classe[classe_nome]['por_tipo'][tipo_estabelecimento]['qtd_instalacoes'] += qtd_ucs
                    mix_por_classe[classe_nome]['por_tipo'][tipo_estabelecimento]['potencia_total_mw'] += potencia_kw / 1000.0
                    
                    total_ucs += qtd_ucs
                    total_instalacoes += qtd_ucs
                    potencia_total_kw += potencia_kw
            
            return {
                'subestacao_id': subestacao_id,
                'total_ucs': total_ucs,
                'total_instalacoes': total_instalacoes,
                'potencia_total_mw': potencia_total_kw / 1000.0,
                'mix_por_classe': mix_por_classe
            }
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao obter mix de consumidores: {str(e)}")
