"""
Serviço de Integração: Google Maps + Detecção de Telhados por Transformador

Pipeline completo:
1. Buscar transformadores de uma subestação
2. Obter imagens via Google Maps (0.3m resolução)
3. Detectar telhados com YOLOv8
4. Armazenar resultados no banco
5. Retornar estatísticas agregadas

Author: Energy Netload Monitor
Date: 2026-01-31
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy.engine import Engine

from .google_maps_service_v2 import GoogleMapsServiceV2
from .telhado_transformador_service import TelhadoTransformadorService

logger = logging.getLogger(__name__)


class GoogleMapsTelhadoIntegrationService:
    """
    Integra Google Maps com detecção de telhados
    """
    
    def __init__(self, engine: Engine, google_maps_api_key: str = None, modelo_yolo_path: str = None):
        """
        Inicializa o serviço
        
        Args:
            engine: SQLAlchemy engine
            google_maps_api_key: Chave da API Google Maps
            modelo_yolo_path: Caminho do modelo YOLO
        """
        self.engine = engine
        self.google_maps_service = GoogleMapsServiceV2(engine, google_maps_api_key)
        self.telhado_service = TelhadoTransformadorService(engine, modelo_yolo_path)
    
    def processar_subestacao_completo(
        self,
        subestacao_id: int,
        zoom: int = 18,
        tamanho_imagem: str = "640x640",
        salvar_resultados: bool = True
    ) -> Dict:
        """
        Processa detecção de telhados para TODOS os transformadores de uma subestação.
        
        Pipeline completo:
        1. Listar transformadores
        2. Obter imagens Google Maps
        3. Detectar telhados
        4. Salvar no banco
        5. Retornar estatísticas
        
        Args:
            subestacao_id: ID da subestação
            zoom: Nível de zoom Google Maps
            tamanho_imagem: Tamanho em pixels
            salvar_resultados: Se deve salvar no banco
        
        Returns:
            Dict com resultado completo
        """
        tempo_inicio = datetime.now()
        logger.info(f"🔄 Iniciando processamento completo para SE {subestacao_id}")
        
        resultado_geral = {
            'subestacao_id': subestacao_id,
            'sucesso': False,
            'transformadores_processados': 0,
            'transformadores_com_telhados': 0,
            'total_telhados': 0,
            'area_total_m2': 0,
            'detalhes': [],
            'motivo': '',
            'tempo_processamento_ms': 0
        }
        
        try:
            # 1. Listar transformadores
            transformadores = self.telhado_service.listar_transformadores_subestacao(subestacao_id)
            
            if not transformadores:
                resultado_geral['motivo'] = f'Nenhum transformador encontrado na SE {subestacao_id}'
                logger.warning(resultado_geral['motivo'])
                return resultado_geral
            
            logger.info(f"✅ {len(transformadores)} transformadores encontrados")
            
            # 2. Processar cada transformador
            for trans in transformadores:
                try:
                    logger.info(f"🔍 Processando transformador {trans['id']}: {trans['nome']}")
                    
                    # 2.1 Obter imagem do Google Maps
                    img_resultado = self.google_maps_service.buscar_imagens_transformador(
                        transformador_id=trans['id'],
                        zoom=zoom,
                        tamanho=tamanho_imagem
                    )
                    
                    if not img_resultado['sucesso']:
                        logger.warning(f"⚠️ Não foi possível obter imagem para transformador {trans['id']}")
                        resultado_geral['detalhes'].append({
                            'transformador_id': trans['id'],
                            'sucesso': False,
                            'motivo': 'Não foi possível obter imagem'
                        })
                        continue
                    
                    # Usar primeira URL (satellite)
                    url_imagem = img_resultado['imagens'][0]['url'] if img_resultado['imagens'] else None
                    
                    if not url_imagem:
                        logger.warning(f"⚠️ Nenhuma URL de imagem retornada")
                        continue
                    
                    # 2.2 Detectar telhados
                    deteccao_resultado = self.telhado_service.detectar_telhados_transformador(
                        transformador_id=trans['id'],
                        imagem_path=url_imagem,
                        fonte_imagem='google_maps'
                    )
                    
                    resultado_geral['transformadores_processados'] += 1
                    
                    # 2.3 Agregar resultados
                    if deteccao_resultado.sucesso:
                        resultado_geral['transformadores_com_telhados'] += 1
                        resultado_geral['total_telhados'] += deteccao_resultado.total_telhados
                        resultado_geral['area_total_m2'] += deteccao_resultado.area_total_m2
                        
                        logger.info(f"✅ {deteccao_resultado.total_telhados} telhados detectados, "
                                   f"{deteccao_resultado.area_total_m2:.0f} m²")
                        
                        # 2.4 Salvar no banco
                        if salvar_resultados:
                            self.telhado_service.salvar_deteccoes(deteccao_resultado)
                        
                        resultado_geral['detalhes'].append({
                            'transformador_id': trans['id'],
                            'transformador_nome': trans['nome'],
                            'sucesso': True,
                            'total_telhados': deteccao_resultado.total_telhados,
                            'area_m2': deteccao_resultado.area_total_m2,
                            'confianca_media': deteccao_resultado.confianca_media
                        })
                    else:
                        logger.warning(f"⚠️ Detecção sem sucesso: {deteccao_resultado.motivo}")
                        resultado_geral['detalhes'].append({
                            'transformador_id': trans['id'],
                            'sucesso': False,
                            'motivo': deteccao_resultado.motivo
                        })
                
                except Exception as e:
                    logger.error(f"❌ Erro ao processar transformador {trans['id']}: {e}")
                    resultado_geral['detalhes'].append({
                        'transformador_id': trans['id'],
                        'sucesso': False,
                        'motivo': str(e)
                    })
            
            # 3. Calcular tempo total
            tempo_total = (datetime.now() - tempo_inicio).total_seconds() * 1000
            resultado_geral['tempo_processamento_ms'] = tempo_total
            
            # 4. Status final
            if resultado_geral['transformadores_processados'] > 0:
                resultado_geral['sucesso'] = True
                resultado_geral['motivo'] = 'Processamento concluído com sucesso'
                logger.info(f"✅ Processamento concluído: {resultado_geral['transformadores_com_telhados']} "
                           f"transformadores com telhados, {resultado_geral['total_telhados']} telhados "
                           f"em {resultado_geral['area_total_m2']:.0f} m²")
            else:
                resultado_geral['motivo'] = 'Nenhum transformador foi processado com sucesso'
            
            return resultado_geral
        
        except Exception as e:
            logger.error(f"❌ Erro geral no processamento: {e}", exc_info=True)
            resultado_geral['motivo'] = str(e)
            resultado_geral['tempo_processamento_ms'] = (datetime.now() - tempo_inicio).total_seconds() * 1000
            return resultado_geral
    
    def processar_transformador_completo(
        self,
        transformador_id: int,
        subestacao_id: int,
        zoom: int = 18,
        tamanho_imagem: str = "640x640",
        salvar_resultados: bool = True
    ) -> Dict:
        """
        Processa detecção de telhados para um transformador específico.
        
        Args:
            transformador_id: ID do transformador
            subestacao_id: ID da subestação
            zoom: Nível de zoom
            tamanho_imagem: Tamanho da imagem
            salvar_resultados: Se deve salvar
        
        Returns:
            Dict com resultado
        """
        tempo_inicio = datetime.now()
        logger.info(f"🔄 Processando transformador {transformador_id}")
        
        try:
            # 1. Obter imagem Google Maps
            img_resultado = self.google_maps_service.buscar_imagens_transformador(
                transformador_id=transformador_id,
                zoom=zoom,
                tamanho=tamanho_imagem
            )
            
            if not img_resultado['sucesso']:
                return {
                    'transformador_id': transformador_id,
                    'sucesso': False,
                    'motivo': img_resultado['motivo'],
                    'tempo_ms': (datetime.now() - tempo_inicio).total_seconds() * 1000
                }
            
            url_imagem = img_resultado['imagens'][0]['url']
            
            # 2. Detectar telhados
            deteccao = self.telhado_service.detectar_telhados_transformador(
                transformador_id=transformador_id,
                imagem_path=url_imagem,
                fonte_imagem='google_maps'
            )
            
            # 3. Salvar se sucesso
            if deteccao.sucesso and salvar_resultados:
                self.telhado_service.salvar_deteccoes(deteccao)
            
            return {
                'transformador_id': transformador_id,
                'subestacao_id': subestacao_id,
                'sucesso': deteccao.sucesso,
                'total_telhados': deteccao.total_telhados,
                'area_m2': deteccao.area_total_m2,
                'confianca_media': deteccao.confianca_media,
                'motivo': deteccao.motivo,
                'tempo_ms': (datetime.now() - tempo_inicio).total_seconds() * 1000,
                'telhados': [t.to_dict() for t in deteccao.telhados]
            }
        
        except Exception as e:
            logger.error(f"❌ Erro ao processar: {e}")
            return {
                'transformador_id': transformador_id,
                'sucesso': False,
                'motivo': str(e),
                'tempo_ms': (datetime.now() - tempo_inicio).total_seconds() * 1000
            }


def criar_servico_integracao(
    engine: Engine,
    google_maps_api_key: str = None,
    modelo_yolo_path: str = None
) -> GoogleMapsTelhadoIntegrationService:
    """Factory function"""
    return GoogleMapsTelhadoIntegrationService(engine, google_maps_api_key, modelo_yolo_path)
