"""
Use cases for Satellite module.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from ...domain.satelite import (
    SateliteRepository,
    TransformadorNotFoundError,
    CoordenadasInvalidasError,
    AreaCoberturaNaoCalculadaError,
    FonteNaoDisponibleError,
    Coordenadas,
)


@dataclass
class ObtenerCoordenadasTransformadorUseCase:
    """Get coordinates for transformador"""
    repository: SateliteRepository
    
    def executar(self, transformador_id: int) -> Dict[str, Any]:
        """
        Execute use case
        
        Args:
            transformador_id: ID of transformador
        
        Returns:
            Dict with coordinate data
        
        Raises:
            TransformadorNotFoundError: If transformador doesn't exist
            CoordenadasInvalidasError: If coordinates are invalid
        """
        trafo = self.repository.obter_transformador(transformador_id)
        
        if not trafo:
            raise TransformadorNotFoundError(
                f"Transformador {transformador_id} não encontrado"
            )
        
        if not trafo.validar_coordenadas():
            raise CoordenadasInvalidasError(
                f"Transformador {transformador_id} sem coordenadas válidas"
            )
        
        return {
            'dados': {
                'transformador_id': trafo.transformador_id,
                'transformador_codigo': trafo.transformador_codigo,
                'transformador_nome': trafo.transformador_nome,
                'distribuidora': trafo.distribuidora,
                'latitude': trafo.coordenadas.latitude,
                'longitude': trafo.coordenadas.longitude,
                'tipo_tensao': trafo.tipo_tensao,
                'valido': True,
            },
            'sucesso': True,
        }


@dataclass
class ObtenerAreaCoberturaTelhadoUseCase:
    """Get coverage area for transformador"""
    repository: SateliteRepository
    
    def executar(self, transformador_id: int) -> Dict[str, Any]:
        """
        Execute use case
        
        Args:
            transformador_id: ID of transformador
        
        Returns:
            Dict with area data or None if not calculated
        """
        trafo = self.repository.obter_transformador(transformador_id)
        
        if not trafo:
            raise TransformadorNotFoundError(
                f"Transformador {transformador_id} não encontrado"
            )
        
        if not trafo.tem_area_cobertura():
            return {'dados': None, 'sucesso': True}
        
        area = trafo.area_cobertura
        return {
            'dados': {
                'transformador_codigo': trafo.transformador_codigo,
                'tipo_tensao': trafo.tipo_tensao,
                'metodo_calculo': area.metodo_calculo,
                'area_km2': area.area_km2,
                'area_m2': area.area_m2,
                'num_consumidores': area.num_consumidores,
                'num_vertices': area.num_vertices,
                'data_calculo': '2026-02-04T10:30:00',  # Would come from DB
            },
            'sucesso': True,
        }


@dataclass
class ListarImagensHistoricoTransformadorUseCase:
    """List satellite request history for transformador"""
    repository: SateliteRepository
    
    def executar(
        self,
        transformador_id: int,
        limite: int = 50,
        offset: int = 0,
        apenas_sucesso: bool = True
    ) -> Dict[str, Any]:
        """
        Execute use case
        
        Args:
            transformador_id: ID of transformador
            limite: Max records
            offset: Pagination offset
            apenas_sucesso: Filter by success
        
        Returns:
            Dict with history data
        """
        trafo = self.repository.obter_transformador(transformador_id)
        
        if not trafo:
            raise TransformadorNotFoundError(
                f"Transformador {transformador_id} não encontrado"
            )
        
        requisicoes, total = self.repository.listar_historico_requisicoes(
            transformador_id=transformador_id,
            limite=limite,
            offset=offset,
            apenas_sucesso=apenas_sucesso
        )
        
        return {
            'dados': {
                'transformador_id': transformador_id,
                'total_requisicoes': total,
                'registros': [
                    {
                        'id': r.id,
                        'transformador_id': r.transformador_id,
                        'subestacao_id': r.subestacao_id,
                        'fonte_satelite': r.fonte_satelite,
                        'status': r.status,
                        'imagem_id': r.imagem_id,
                        'url_download': r.url_download,
                        'data_imagem': r.data_imagem,
                        'cobertura_nuvem_percentual': r.cobertura_nuvem_percentual,
                        'resolucao_metros': r.resolucao_metros,
                        'tempo_requisicao_ms': r.tempo_requisicao_ms,
                        'custo_usd_estimado': r.custo_usd_estimado,
                        'data_requisicao': r.data_requisicao,
                    }
                    for r in requisicoes
                ]
            },
            'sucesso': True,
        }


@dataclass
class DecidirFonteSateliteUseCase:
    """Decide which satellite source to use"""
    repository: SateliteRepository
    
    def executar(
        self,
        transformador_id: int,
        tentar_google_maps: bool = True,
        tentar_cbers4a: bool = True,
        force_cbers4a: bool = False
    ) -> Dict[str, Any]:
        """
        Execute use case
        
        Args:
            transformador_id: ID of transformador
            tentar_google_maps: Try Google Maps
            tentar_cbers4a: Try CBERS-4A
            force_cbers4a: Force CBERS-4A
        
        Returns:
            Dict with source decision data
        """
        trafo = self.repository.obter_transformador(transformador_id)
        
        if not trafo:
            raise TransformadorNotFoundError(
                f"Transformador {transformador_id} não encontrado"
            )
        
        if not (tentar_google_maps or tentar_cbers4a):
            raise FonteNaoDisponibleError(
                "Nenhuma fonte habilitada (ambas desabilitadas)"
            )
        
        decisao = self.repository.decidir_fonte_melhor(
            transformador_id=transformador_id,
            tentar_google=tentar_google_maps,
            tentar_cbers=tentar_cbers4a,
            forcar_cbers=force_cbers4a
        )
        
        return {
            'dados': decisao,
            'sucesso': decisao.get('pode_usar', False),
        }


@dataclass
class ObtenerQuotaMesAtualUseCase:
    """Get current month Google Maps quota"""
    repository: SateliteRepository
    
    def executar(self) -> Dict[str, Any]:
        """
        Execute use case
        
        Returns:
            Dict with quota data
        """
        quota = self.repository.obter_quota_mensal_atual()
        
        from datetime import datetime
        mes_ano = datetime.now().strftime("%Y-%m")
        
        return {
            'dados': {
                'requisicoes_mes': quota.requisicoes_mes,
                'limite_mensal': quota.limite_mensal,
                'disponivel': quota.disponivel,
                'percentual_uso': quota.percentual_uso,
                'custo_mes_usd': quota.requisicoes_mes * 0.007,
                'mes_ano': mes_ano,
            },
            'sucesso': True,
        }


@dataclass
class ObtenerEstatisticasGoogleMapsUseCase:
    """Get statistics for Google Maps usage"""
    repository: SateliteRepository
    
    def executar(self) -> Dict[str, Any]:
        """
        Execute use case
        
        Returns:
            Dict with statistics data
        """
        stats = self.repository.obter_estatisticas_google_maps()
        
        return {
            'dados': stats,
            'sucesso': True,
        }
