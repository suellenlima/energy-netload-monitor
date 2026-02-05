"""Infrastructure Layer - Subestacao Mapper (DTO Converter)"""

from typing import Dict, Any
from src.domain.subestacao import (
    Subestacao,
    CodigoSubestacao,
    NomeSubestacao,
    TensaoNominal,
    AreaCobertura,
)


class SubestacaoMapper:
    """Converte entre modelo ORM, domínio e DTOs"""
    
    @staticmethod
    def to_domain(row: Dict[str, Any]) -> Subestacao:
        """Converte row do banco para entidade de domínio - suporta dados ANEEL com campos NULL"""
        # Handle NULL values from ANEEL data - use defaults se valores não existem ou são inválidos
        tensao_kv = row.get('tensao_nominal_kv')
        tensao = TensaoNominal(float(tensao_kv)) if tensao_kv and float(tensao_kv) > 0 else TensaoNominal(138.0)
        
        potencia = row.get('potencia_nominal_mva')
        # Use default 100 MVA (padrão de distribuição) se NULL ou inválido
        potencia_nominal = float(potencia) if potencia and float(potencia) > 0 else 100.0
        
        area = row.get('area_cobertura_km2')
        area_cobertura = AreaCobertura(float(area)) if area and float(area) > 0 else AreaCobertura(1.0)
        
        return Subestacao(
            id=row['id'],
            codigo=CodigoSubestacao(row['codigo']),
            nome=NomeSubestacao(row['nome']),
            tensao_nominal_kv=tensao,
            potencia_nominal_mva=potencia_nominal,
            area_cobertura_km2=area_cobertura,
            latitude=float(row.get('latitude', 0)),
            longitude=float(row.get('longitude', 0)),
            distribuidora_codigo=row.get('distribuidora_codigo'),
            distribuidora_nome=row.get('distribuidora_nome'),
            ativo=bool(row.get('ativo', True)),
            timestamp_criacao=row.get('timestamp_criacao'),
            timestamp_atualizacao=row.get('timestamp_atualizacao'),
        )
    
    @staticmethod
    def to_detail_response(subestacao: Subestacao) -> Dict[str, Any]:
        """Converte para resposta de detalhe"""
        return {
            'id': subestacao.id,
            'codigo': str(subestacao.codigo),
            'nome': str(subestacao.nome),
            'tensao_nominal_kv': subestacao.tensao_nominal_kv.valor,
            'potencia_nominal_mva': subestacao.potencia_nominal_mva,
            'area_cobertura_km2': subestacao.area_cobertura_km2.valor,
            'tipo_tensao': subestacao.obter_tipo_tensao(),
            'latitude': subestacao.latitude,
            'longitude': subestacao.longitude,
            'distribuidora_codigo': subestacao.distribuidora_codigo,
            'distribuidora_nome': subestacao.distribuidora_nome,
            'ativo': subestacao.ativo,
            'timestamp_criacao': subestacao.timestamp_criacao.isoformat() if subestacao.timestamp_criacao else None,
            'timestamp_atualizacao': subestacao.timestamp_atualizacao.isoformat() if subestacao.timestamp_atualizacao else None,
        }
    
    @staticmethod
    def to_list_response(subestacao: Subestacao) -> Dict[str, Any]:
        """Converte para resposta de listagem (resumida)"""
        return {
            'id': subestacao.id,
            'codigo': str(subestacao.codigo),
            'nome': str(subestacao.nome),
            'tensao_nominal_kv': subestacao.tensao_nominal_kv.valor,
            'tipo_tensao': subestacao.obter_tipo_tensao(),
            'potencia_nominal_mva': subestacao.potencia_nominal_mva,
            'distribuidora_codigo': subestacao.distribuidora_codigo,
            'ativo': subestacao.ativo,
        }
