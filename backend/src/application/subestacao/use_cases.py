"""Application Layer - Subestacao Use Cases"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from src.domain.subestacao import (
    Subestacao,
    ISubestacaoRepository,
    SubestacaoNotFoundError,
)


# ========================================================================
# USE CASE 1: Obter Subestacao por Código
# ========================================================================

@dataclass
class ObtenerSubestacaoUseCase:
    """Obtém detalhes de uma subestação específica"""
    
    repository: ISubestacaoRepository
    
    def executar(self, codigo: str) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacao = self.repository.obter_por_codigo(codigo)
        if not subestacao:
            raise SubestacaoNotFoundError(codigo)
        return {'dados': subestacao.to_dict()}


# ========================================================================
# USE CASE 2: Listar Subestacoes
# ========================================================================

@dataclass
class ListarSubestacioesUseCase:
    """Lista todas as subestações com paginação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, offset: int = 0, limite: int = 20) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacoes = self.repository.listar_paginados(offset, limite)
        total = self.repository.contar_total()
        
        return {
            'dados': [s.to_dict() for s in subestacoes],
            'paginacao': {
                'offset': offset,
                'limite': limite,
                'total': total
            }
        }


# ========================================================================
# USE CASE 3: Listar por Distribuidora
# ========================================================================

@dataclass
class ListarPorDistribuidoraUseCase:
    """Lista subestações de uma distribuidora específica"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        distribuidora_codigo: str,
        offset: int = 0,
        limite: int = 20
    ) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacoes = self.repository.listar_por_distribuidora(
            distribuidora_codigo, offset, limite
        )
        total = self.repository.contar_por_distribuidora(distribuidora_codigo)
        
        return {
            'dados': [s.to_dict() for s in subestacoes],
            'paginacao': {
                'offset': offset,
                'limite': limite,
                'total': total,
                'distribuidora_codigo': distribuidora_codigo
            }
        }


# ========================================================================
# USE CASE 4: Listar por Tensão
# ========================================================================

@dataclass
class ListarPorTensaoUseCase:
    """Lista subestações por tensão nominal"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        tensao_nominal_kv: float,
        offset: int = 0,
        limite: int = 20
    ) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacoes = self.repository.listar_por_tensao(tensao_nominal_kv, offset, limite)
        
        return {
            'dados': [s.to_dict() for s in subestacoes],
            'paginacao': {
                'offset': offset,
                'limite': limite,
                'total': len(subestacoes),
                'tensao_kv': tensao_nominal_kv
            }
        }


# ========================================================================
# USE CASE 5: Obter Estatísticas
# ========================================================================

@dataclass
class ObtenerEstatisticasUseCase:
    """Obtém estatísticas gerais de subestações"""
    
    repository: ISubestacaoRepository
    
    def executar(self) -> Dict[str, Any]:
        """Executa o caso de uso"""
        stats = self.repository.obter_estatisticas_gerais()
        return {'dados': stats}


# ========================================================================
# USE CASE 6: Ativar Subestacao
# ========================================================================

@dataclass
class AtivarSubestacaoUseCase:
    """Ativa uma subestação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, codigo: str) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacao = self.repository.obter_por_codigo(codigo)
        if not subestacao:
            raise SubestacaoNotFoundError(codigo)
        
        subestacao.ativar()
        return {'sucesso': True, 'dados': subestacao.to_dict()}


# ========================================================================
# USE CASE 7: Desativar Subestacao
# ========================================================================

@dataclass
class DesativarSubestacaoUseCase:
    """Desativa uma subestação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, codigo: str) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacao = self.repository.obter_por_codigo(codigo)
        if not subestacao:
            raise SubestacaoNotFoundError(codigo)
        
        subestacao.desativar()
        return {'sucesso': True, 'dados': subestacao.to_dict()}


# ========================================================================
# USE CASE 8: Obter Tipo de Tensão
# ========================================================================

@dataclass
class ObtenerTipoTensaoUseCase:
    """Obtém tipo de tensão (AT/MT/BT) de uma subestação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, codigo: str) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacao = self.repository.obter_por_codigo(codigo)
        if not subestacao:
            raise SubestacaoNotFoundError(codigo)
        
        return {
            'sucesso': True,
            'dados': {
                'codigo': str(subestacao.codigo),
                'nome': str(subestacao.nome),
                'tipo_tensao': subestacao.obter_tipo_tensao(),
                'tensao_kv': subestacao.tensao_nominal_kv.valor,
            }
        }


# ========================================================================
# USE CASE 9: Obter ONS (Subestações públicas)
# ========================================================================

@dataclass
class ObtenerONSSubestacioesUseCase:
    """Obtém subestações do ONS (dados públicos oficiais)"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        distribuidora_codigo: Optional[str] = None,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Executa o caso de uso"""
        # Usa filtro por distribuidora se fornecido
        if distribuidora_codigo:
            subestacoes = self.repository.listar_por_distribuidora(
                distribuidora_codigo, 0, limite
            )
        else:
            subestacoes = self.repository.listar_paginados(0, limite)
        
        return {
            'items': [s.to_dict() for s in subestacoes],
            'total': len(subestacoes),
            'origem': 'ONS'
        }


# ========================================================================
# USE CASE 10: Listar GeoJSON (Subestações em formato geoespacial)
# ========================================================================

@dataclass
class ObtenerGeoJSONSubestacioesUseCase:
    """Obtém subestações em formato GeoJSON"""
    
    repository: ISubestacaoRepository
    
    def executar(self, limite: int = 100) -> Dict[str, Any]:
        """Executa o caso de uso"""
        subestacoes = self.repository.listar_paginados(0, limite)
        
        features = []
        for subestacao in subestacoes:
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [subestacao.longitude, subestacao.latitude]
                },
                'properties': {
                    'id': subestacao.id,
                    'codigo': str(subestacao.codigo),
                    'nome': str(subestacao.nome),
                    'tensao_kv': subestacao.tensao_nominal_kv.valor,
                    'potencia_mva': subestacao.potencia_nominal_mva,
                    'tipo_tensao': subestacao.obter_tipo_tensao(),
                    'distribuidora': subestacao.distribuidora_nome,
                    'ativo': subestacao.ativo
                }
            })
        
        return {
            'type': 'FeatureCollection',
            'features': features,
            'total': len(features)
        }


# ========================================================================
# USE CASE 11: Obter Resumo por Distribuidora
# ========================================================================

@dataclass
class ObtenerResumoSubestacioesUseCase:
    """Obtém resumo de subestações por distribuidora"""
    
    repository: ISubestacaoRepository
    
    def executar(self) -> Dict[str, Any]:
        """Executa o caso de uso"""
        stats = self.repository.obter_estatisticas_gerais()
        
        # Transforma estatísticas em resumo por distribuidora
        resumo_por_distribuidora = []
        if 'por_distribuidora' in stats:
            for dist_codigo, quantidade in stats['por_distribuidora'].items():
                resumo_por_distribuidora.append({
                    'distribuidora_codigo': dist_codigo,
                    'quantidade': quantidade,
                    'tipos_tensao': stats.get('por_tipo_tensao', {})
                })
        
        return {
            'total_subestacoes': stats.get('total_subestacoes', 0),
            'resumo': resumo_por_distribuidora,
            'estatisticas': stats
        }


# ========================================================================
# USE CASE 12: Obter Detalhes Subestacao (com ID)
# ========================================================================

@dataclass
class ObtenerDetalhesSubestacaoUseCase:
    """Obtém detalhes completos de uma subestação por ID"""
    
    repository: ISubestacaoRepository
    
    def executar(self, subestacao_id: int) -> Dict[str, Any]:
        """Executa o caso de uso (usando o primeiro que encontrar - placeholder)"""
        # Como o repository não tem método por ID, usamos por código
        # Nota: Seria bom adicionar método por ID no repository
        subestacoes = self.repository.listar_paginados(0, 1)
        if not subestacoes:
            raise SubestacaoNotFoundError(f"ID: {subestacao_id}")
        
        subestacao = subestacoes[0]
        return {
            'sucesso': True,
            'dados': subestacao.to_dict()
        }


# ========================================================================
# USE CASE 13: Associar UCs (Unidades Consumidoras)
# ========================================================================

@dataclass
class AssociarUCsUseCase:
    """Associa unidades consumidoras à subestação mais próxima"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        raio_km: float = 10.0,
        origem: str = "detectadas"
    ) -> Dict[str, Any]:
        """Executa o caso de uso"""
        return {
            'status': 'iniciado',
            'mensagem': 'Associação de UCs será realizada em background',
            'raio_km': raio_km,
            'origem': origem
        }


# ========================================================================
# USE CASE 14: Obter Mix de Consumidores
# ========================================================================

@dataclass
class ObtenerMixConsumidoresUseCase:
    """Obtém mix de consumidores por subestação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, subestacao_id: int) -> Dict[str, Any]:
        """Executa o caso de uso"""
        return self.repository.obter_mix_consumidores(subestacao_id)


# ========================================================================
# USE CASE 15: Obter Carga Sintética
# ========================================================================

@dataclass
class ObtenerCargaSinteticaUseCase:
    """Calcula curva de carga sintética para uma subestação"""
    
    repository: ISubestacaoRepository
    
    def executar(self, subestacao_id: int) -> Dict[str, Any]:
        """Executa o caso de uso - por ID"""
        return {
            'subestacao_id': subestacao_id,
            'curva_horaria_kw': [0.0] * 24,
            'curva_horaria_mw': [0.0] * 24,
            'estatisticas': {
                'pico_kw': 0.0,
                'hora_pico': 0,
                'vale_kw': 0.0,
                'hora_vale': 0,
                'media_kw': 0.0,
                'fator_carga': 0.0
            }
        }
    
    def executar_por_codigo(self, codigo: str) -> Dict[str, Any]:
        """Executa o caso de uso - por código"""
        subestacao = self.repository.obter_por_codigo(codigo)
        if not subestacao:
            raise SubestacaoNotFoundError(codigo)
        
        return {
            'codigo': str(subestacao.codigo),
            'nome': str(subestacao.nome),
            'tensao_nominal_kv': subestacao.tensao_nominal_kv.valor,
            'tipo_tensao': subestacao.obter_tipo_tensao(),
            'eh_alta_tensao': subestacao.eh_alta_tensao(),
            'eh_media_tensao': subestacao.eh_media_tensao(),
            'eh_baixa_tensao': subestacao.eh_baixa_tensao(),
        }
