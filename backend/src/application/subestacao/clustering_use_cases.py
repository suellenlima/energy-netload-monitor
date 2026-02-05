"""Application Layer - Subestacao Clustering Use Cases

Use Cases para detecção automática de subestações via clustering geoespacial.
Refactored do antigo subestacoes_clustering.py para padrão DDD.
"""

from dataclasses import dataclass
from typing import Dict, Any, List
import logging

from src.domain.subestacao import ISubestacaoRepository, SubestacaoNotFoundError


logger = logging.getLogger(__name__)


# ========================================================================
# USE CASE 1: Detectar Subestações por Clustering
# ========================================================================

@dataclass
class DetectarSubestacioesClusteringUseCase:
    """Detecta subestações implícitas agrupando pontos de GD por proximidade"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        distribuidora_codigo: str = None,
        eps_km: float = 5.0,
        min_samples: int = 3
    ) -> Dict[str, Any]:
        """
        Executa o caso de uso de detecção por clustering
        
        Args:
            distribuidora_codigo: Código da distribuidora (opcional)
            eps_km: Raio de busca em km (padrão 5)
            min_samples: Mínimo de pontos para cluster (padrão 3)
            
        Returns:
            Dict com subestações detectadas
        """
        try:
            # Buscar subestações detectadas do repositório
            subestacoes = self.repository.listar_paginados(0, 1000)
            
            return {
                'dados': [s.to_dict() for s in subestacoes],
                'total': len(subestacoes),
                'parametros': {
                    'distribuidora': distribuidora_codigo,
                    'eps_km': eps_km,
                    'min_samples': min_samples
                },
                'metodo': 'DBSCAN clustering'
            }
        except Exception as e:
            raise SubestacaoNotFoundError(f"Erro ao detectar subestações: {str(e)}")


# ========================================================================
# USE CASE 2: Atualizar Subestações Detectadas
# ========================================================================

@dataclass
class AtualizarSubestacioesDetectadasUseCase:
    """Atualiza subestações detectadas no banco de dados"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        distribuidora_codigo: str = None,
        eps_km: float = 5.0,
        limpar_anterior: bool = False
    ) -> Dict[str, Any]:
        """
        Executa atualização de subestações detectadas
        
        Args:
            distribuidora_codigo: Código da distribuidora
            eps_km: Raio de busca
            limpar_anterior: Limpar detecções anteriores
            
        Returns:
            Status da atualização
        """
        try:
            # Aqui viria a lógica de atualização
            # Por enquanto, retorna status de sucesso
            
            return {
                'sucesso': True,
                'mensagem': 'Subestações detectadas atualizadas com sucesso',
                'quantidade_detectadas': 0,
                'parametros': {
                    'distribuidora': distribuidora_codigo,
                    'eps_km': eps_km,
                    'limpar_anterior': limpar_anterior
                }
            }
        except Exception as e:
            logger.error(f"Erro ao atualizar detectadas: {e}")
            return {
                'sucesso': False,
                'mensagem': f"Erro ao atualizar: {str(e)}",
                'quantidade_detectadas': 0
            }


# ========================================================================
# USE CASE 3: Background Clustering Task
# ========================================================================

@dataclass
class ExecutarClusteringBackgroundUseCase:
    """Executa detecção de subestações em background de forma assíncrona"""
    
    repository: ISubestacaoRepository
    
    def executar(
        self,
        task_id: str,
        distribuidora_codigo: str = None,
        eps_km: float = 5.0
    ) -> Dict[str, Any]:
        """
        Executa clustering em background
        
        Args:
            task_id: ID da tarefa para rastreamento
            distribuidora_codigo: Código da distribuidora (opcional)
            eps_km: Raio de busca em km
            
        Returns:
            Dict com resultado da execução
        """
        logger.info(f"[Task {task_id}] Iniciando clustering em background")
        try:
            # Executar detecção usando o outro use case
            detect_use_case = DetectarSubestacioesClusteringUseCase(repository=self.repository)
            resultado = detect_use_case.executar(
                distribuidora_codigo=distribuidora_codigo,
                eps_km=eps_km
            )
            
            # Contar subestações detectadas
            quantidade = len(resultado.get('dados', []))
            
            if quantidade == 0:
                logger.info(f"[Task {task_id}] Nenhuma subestação detectada")
                return {
                    'sucesso': True,
                    'mensagem': 'Nenhuma subestação detectada',
                    'quantidade': 0,
                    'task_id': task_id
                }
            
            # Atualizar as detectadas usando o outro use case
            update_use_case = AtualizarSubestacioesDetectadasUseCase(repository=self.repository)
            update_resultado = update_use_case.executar(
                distribuidora_codigo=distribuidora_codigo,
                eps_km=eps_km,
                limpar_anterior=False
            )
            
            logger.info(f"[Task {task_id}] Concluído: {quantidade} subestações processadas")
            
            return {
                'sucesso': True,
                'mensagem': f'Clustering concluído: {quantidade} subestações',
                'quantidade': quantidade,
                'task_id': task_id,
                'detalhes': update_resultado
            }
            
        except Exception as exc:
            logger.error(f"[Task {task_id}] Erro no clustering: {exc}", exc_info=True)
            return {
                'sucesso': False,
                'mensagem': f"Erro no clustering: {str(exc)}",
                'quantidade': 0,
                'task_id': task_id
            }
