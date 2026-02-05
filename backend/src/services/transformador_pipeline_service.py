"""
Serviço de Pipeline de Transformadores
Lógica de orquestração: Baixa imagens, detecta telhados e painéis

ARQUITETURA (refatoração 2026-02-04):
- RoofDetectionService: Camada de infraestrutura (apenas ML - detecção/segmentação)
- RoofService: Camada de aplicação (wrapper/orquestração com RoofDetectionService)
- TransformadorPipelineService: Lógica de negócio (este arquivo)

Database Schema: ANEEL BDGD (infrastructure/database/schema_aneel_bdgd.sql)
Workflow:
  1. Busca transformador em transformadores_aneel (ativo = TRUE)
  2. Gera grid 3x3 de imagens de satélite
  3. Para cada imagem:
     - Baixa e caches em data/cache/imagens_grid/
     - Detecta telhados (RoofService → RoofDetectionService)
     - Registra em telhados_detectados_transformador
     - Para cada telhado:
       - Detecta painéis (PainelSolarDetectionService)
       - Salva em paineis_solares_detectados
       - Calcula potência e salva em potencia_telhados
  4. Acumula totais e retorna resultado consolidado

Tabelas utilizadas:
  - transformadores_aneel: Leitura (lat/lon/status)
  - telhados_detectados_transformador: Escrita
  - paineis_solares_detectados: Escrita
  - potencia_telhados: Escrita
  - satelite_requisicoes_google_maps: Escrita (rastreamento)

Author: Energy Netload Monitor
Date: 2026-02-04 (Refactored with RoofDetectionService separation)
"""

import json
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
from io import BytesIO

import requests
import cv2
from PIL import Image

from ..infrastructure.persistence.transformador_pipeline import TransformadorPipelineRepository
from ..application.telhado_detection.service import TelhadoDetectionService
from ..application.painel_solar import PainelSolarApplicationService
from ..schemas.painel_solar import (
    PainelSolarResponse,
    EstimativaPotenciaResponse,
    TelhadorComPaineis,
)


class TransformadorPipelineService:
    """
    Serviço de pipeline de transformadores.
    Responsável por: Orquestração, cache de imagens, processamento de telhados e painéis.
    """

    def __init__(self, engine):
        """
        Inicializa o serviço.
        
        Args:
            engine: SQLAlchemy Engine
        """
        self.engine = engine
        self.repository = TransformadorPipelineRepository(engine)
        self.logger = logging.getLogger(__name__)
        
        # Serviços de detecção (lazy loading)
        self._servico_telhados: Optional[TelhadoDetectionService] = None
        self._servico_paineis: Optional[PainelSolarApplicationService] = None
        
        # Criar diretório de cache
        self.cache_dir = self._criar_dir_cache()

    # ========================================================================
    # LAZY LOADING DE SERVIÇOS
    # ========================================================================

    def _obter_servico_telhados(self) -> TelhadoDetectionService:
        """Obtém ou cria instância do serviço de detecção de telhados (DDD)."""
        if self._servico_telhados is None:
            self.logger.info("🔧 Inicializando serviço DDD de detecção de telhados...")
            self._servico_telhados = TelhadoDetectionService(engine=self.engine)
        return self._servico_telhados

    def _obter_servico_paineis(self) -> PainelSolarApplicationService:
        """Obtém ou cria instância do serviço DDD de painéis solares."""
        if self._servico_paineis is None:
            self.logger.info("🔧 Inicializando serviço DDD de detecção de painéis solares...")
            self._servico_paineis = PainelSolarApplicationService()
        return self._servico_paineis

    # ========================================================================
    # GERENCIAMENTO DE CACHE DE IMAGENS
    # ========================================================================

    def _criar_dir_cache(self) -> Path:
        """Cria diretório de cache se não existir."""
        cache_dir = Path("data/cache/imagens_grid")
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _salvar_imagem_em_cache(self, url_imagem: str, transformador_id: int, indice: int) -> Optional[str]:
        """
        Baixa e salva imagem em cache.
        
        Args:
            url_imagem: URL da imagem
            transformador_id: ID do transformador
            indice: Índice da imagem no grid
            
        Returns:
            Caminho da imagem salva ou None se erro
        """
        try:
            filename = f"trafo_{transformador_id}_img_{indice:03d}.png"
            filepath = self.cache_dir / filename
            
            # Se já existe, usar
            if filepath.exists():
                self.logger.info(f"[CACHE] 💾 Imagem encontrada em cache: {filepath}")
                return str(filepath)
            
            # Baixar imagem
            self.logger.info(f"[CACHE] 📥 Baixando imagem {indice}...")
            response = requests.get(url_imagem, timeout=30)
            response.raise_for_status()
            
            # Salvar em disco
            imagem = Image.open(BytesIO(response.content))
            if imagem.mode != 'RGB':
                imagem = imagem.convert('RGB')
            
            imagem.save(filepath)
            self.logger.info(f"[CACHE] ✅ Imagem salva em: {filepath}")
            
            return str(filepath)
        
        except Exception as e:
            self.logger.error(f"[CACHE] ❌ Erro ao salvar imagem em cache: {e}")
            return None

    def _carregar_imagem_do_cache(self, caminho_imagem: str):
        """
        Carrega imagem do disco como numpy array.
        
        Returns:
            numpy array ou None se erro
        """
        try:
            img = cv2.imread(caminho_imagem)
            if img is None:
                self.logger.error(f"[CACHE] ❌ Não foi possível carregar: {caminho_imagem}")
                return None
            return img
        except Exception as e:
            self.logger.error(f"[CACHE] ❌ Erro ao carregar imagem: {e}")
            return None

    # ========================================================================
    # PROCESSAMENTO DE PAINÉIS
    # ========================================================================

    def _processar_telhado_para_paineis(self, 
                                        telhado_id: int,
                                        caminho_imagem: str,
                                        bbox: Dict,
                                        transformador_id: int,
                                        confianca_minima: float,
                                        potencia_por_m2: float) -> Dict:
        """
        Detecta painéis em um telhado específico.
        
        Returns:
            Dict com resultado do processamento
        """
        try:
            servico_paineis = self._obter_servico_paineis()
            
            # Processar telhado para painéis (DDD)
            resultado_paineis = servico_paineis.processar_telhado_completo(
                url_imagem=caminho_imagem,
                bbox=bbox,
                confianca_minima=confianca_minima,
                potencia_por_m2=potencia_por_m2
            )
            
            if not resultado_paineis.sucesso or not resultado_paineis.paineis:
                return {'sucesso': False, 'num_paineis': 0}
            
            # Formatar painéis para resposta (converter DTOs para schemas)
            paineis_response = [
                PainelSolarResponse(
                    id_painel=p.id_painel,
                    bbox=p.bbox,
                    centroide=p.centroide,
                    area_pixeis=p.area_pixeis,
                    area_m2=p.area_m2,
                    confianca=p.confianca,
                    tipo_painel=p.tipo_painel,
                    timestamp_deteccao=p.timestamp_deteccao
                )
                for p in resultado_paineis.paineis
            ]
            
            potencia_response = EstimativaPotenciaResponse(
                total_area_m2=resultado_paineis.estimativa_potencia.total_area_m2,
                num_paineis=resultado_paineis.estimativa_potencia.num_paineis,
                potencia_instalada_kw=resultado_paineis.estimativa_potencia.potencia_instalada_kw,
                producao_diaria_kwh=resultado_paineis.estimativa_potencia.producao_diaria_kwh,
                producao_anual_kwh=resultado_paineis.estimativa_potencia.producao_anual_kwh,
                economia_anual_brl=resultado_paineis.estimativa_potencia.economia_anual_brl
            ) if resultado_paineis.estimativa_potencia else None
            
            return {
                'sucesso': True,
                'num_paineis': len(paineis_response),
                'paineis': paineis_response,
                'potencia': potencia_response,
                'potencia_dict': resultado_paineis.estimativa_potencia.to_dict() if resultado_paineis.estimativa_potencia else None
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao processar telhado para painéis: {e}")
            return {'sucesso': False, 'num_paineis': 0}

    def _salvar_paineis_do_telhado(self,
                                   telhado_id: int,
                                   transformador_id: int,
                                   paineis_response: List[PainelSolarResponse],
                                   potencia_response: Optional[EstimativaPotenciaResponse]) -> bool:
        """
        Salva painéis e potência do telhado no banco.
        
        Returns:
            True se sucesso
        """
        try:
            # Buscar dados do telhado
            telhado_data = self.repository.obter_telhado_por_id(telhado_id)
            if not telhado_data:
                self.logger.error(f"Telhado {telhado_id} não encontrado")
                return False
            
            trans_id = telhado_data['transformador_id']
            sub_id = telhado_data['subestacao_id']
            
            # Limpar painéis antigos
            self.repository.limpar_paineis_do_telhado(telhado_id)
            
            # Preparar lista de painéis para inserção
            paineis_data = []
            for painel in paineis_response:
                potencia_w = painel.area_m2 * 150.0
                
                paineis_data.append({
                    'telhado_id': telhado_id,
                    'trans_id': trans_id,
                    'sub_id': sub_id,
                    'bbox': json.dumps(painel.bbox),
                    'centroide': json.dumps(painel.centroide),
                    'area_px': painel.area_pixeis,
                    'area_m2': painel.area_m2,
                    'conf': painel.confianca,
                    'tipo': painel.tipo_painel,
                    'potencia': potencia_w,
                    'timestamp': painel.timestamp_deteccao
                })
            
            # Salvar painéis em lote
            if paineis_data:
                self.repository.salvar_paineis_lote(paineis_data)
            
            # Salvar potência
            if potencia_response:
                self.repository.limpar_potencia_do_telhado(telhado_id)
                
                potencia_data = {
                    'telhado_id': telhado_id,
                    'trans_id': trans_id,
                    'num': len(paineis_response),
                    'area': potencia_response.total_area_m2,
                    'pot_kw': potencia_response.potencia_instalada_kw,
                    'prod_dia': potencia_response.producao_diaria_kwh,
                    'prod_ano': potencia_response.producao_anual_kwh,
                    'economia': potencia_response.economia_anual_brl,
                    'pot_m2': 150.0
                }
                
                self.repository.salvar_potencia_telhado(potencia_data)
            
            self.logger.info(f"✅ {len(paineis_response)} painéis salvos para telhado {telhado_id}")
            return True
        
        except Exception as e:
            self.logger.error(f"Erro ao salvar painéis do telhado: {e}")
            return False

    # ========================================================================
    # PIPELINE PRINCIPAL
    # ========================================================================

    def processar_transformador_completo(self,
                                         transformador_id: int,
                                         confianca_minima_telhados: float = 0.5,
                                         confianca_minima_paineis: float = 0.5) -> Dict:
        """
        Pipeline completo: Baixa imagens, detecta telhados e painéis.
        
        Args:
            transformador_id: ID do transformador
            confianca_minima_telhados: Confiança mínima para telhados
            confianca_minima_paineis: Confiança mínima para painéis
            
        Returns:
            Dict com resultado do pipeline
        """
        tempo_inicio = time.time()
        
        try:
            self.logger.info(f"🚀 Iniciando pipeline para transformador {transformador_id}")
            
            # Obter serviços
            servico_telhados = self._obter_servico_telhados()
            
            # Buscar transformador
            trafo_data = self.repository.obter_transformador(transformador_id)
            if not trafo_data:
                self.logger.error(f"Transformador {transformador_id} não encontrado")
                return {
                    'sucesso': False,
                    'erro': f'Transformador {transformador_id} não encontrado'
                }
            
            lat_trafo = trafo_data['latitude']
            lon_trafo = trafo_data['longitude']
            subestacao_id = trafo_data['subestacao_id']
            
            self.logger.info(f"📍 Transformador: lat={lat_trafo}, lon={lon_trafo}")
            
            # Gerar grid de imagens
            self.logger.info("🗺️ Gerando grid de imagens...")
            imagens_grid = servico_telhados.gerar_imagens_grid(lat_trafo, lon_trafo)
            self.logger.info(f"✓ {len(imagens_grid)} imagens encontradas")
            
            # Limpar dados antigos
            self.repository.limpar_paineis_do_transformador(transformador_id)
            self.repository.limpar_potencia_do_transformador(transformador_id)
            
            # Variáveis de acumulação
            total_telhados = 0
            total_paineis = 0
            todos_telhados_com_paineis = []
            potencia_total = None
            erros = []
            
            # Processar cada imagem do grid
            for idx, imagem in enumerate(imagens_grid, 1):
                try:
                    self.logger.info(f"⏳ Processando imagem {idx}/{len(imagens_grid)}")
                    
                    # ====== ETAPA 1: BAIXAR E CACHEAR IMAGEM ======
                    caminho_imagem = self._salvar_imagem_em_cache(
                        imagem['url'], 
                        transformador_id, 
                        idx
                    )
                    
                    if not caminho_imagem:
                        erros.append(f"Imagem {idx}: Erro ao salvar em cache")
                        continue
                    
                    # Salvar referência no banco
                    self.repository.salvar_referencia_imagem(
                        transformador_id, 
                        idx, 
                        imagem['url'], 
                        caminho_imagem
                    )
                    
                    # ====== ETAPA 2: DETECTAR TELHADOS ======
                    self.logger.info(f"🏠 Detectando telhados...")
                    
                    resultado_telhados = servico_telhados.processar_telhados_lote(
                        caminho_imagem,
                        latitude_centro=lat_trafo,
                        longitude_centro=lon_trafo,
                        transformador_id=transformador_id,
                        subestacao_id=subestacao_id,
                        confianca_minima=confianca_minima_telhados,
                        sem_autenticacao=False
                    )
                    
                    if resultado_telhados['sucesso']:
                        num_telhados = resultado_telhados['total_telhados_segmentados']
                        self.logger.info(f"✅ {num_telhados} telhados detectados")
                        total_telhados += num_telhados
                    
                    # ====== ETAPA 3: DETECTAR PAINÉIS NOS TELHADOS ======
                    if resultado_telhados['sucesso'] and resultado_telhados['telhados_segmentados']:
                        self.logger.info(f"☀️ Detectando painéis nos telhados...")
                        
                        for telhado in resultado_telhados['telhados_segmentados']:
                            try:
                                telhado_id = telhado['id']
                                bbox = telhado['bbox']
                                
                                # Processar painéis
                                resultado_paineis = self._processar_telhado_para_paineis(
                                    telhado_id=telhado_id,
                                    caminho_imagem=caminho_imagem,
                                    bbox=bbox,
                                    transformador_id=transformador_id,
                                    confianca_minima=confianca_minima_paineis,
                                    potencia_por_m2=150.0
                                )
                                
                                if resultado_paineis['sucesso']:
                                    num_paineis = resultado_paineis['num_paineis']
                                    self.logger.info(f"✅ {num_paineis} painéis detectados no telhado {telhado_id}")
                                    total_paineis += num_paineis
                                    
                                    # Salvar painéis no banco
                                    self._salvar_paineis_do_telhado(
                                        telhado_id=telhado_id,
                                        transformador_id=transformador_id,
                                        paineis_response=resultado_paineis['paineis'],
                                        potencia_response=resultado_paineis['potencia']
                                    )
                                    
                                    # Acumular potência total
                                    if resultado_paineis['potencia_dict']:
                                        if potencia_total is None:
                                            potencia_total = resultado_paineis['potencia_dict'].copy()
                                        else:
                                            potencia_total['total_area_m2'] += resultado_paineis['potencia_dict']['total_area_m2']
                                            potencia_total['num_paineis'] += resultado_paineis['potencia_dict']['num_paineis']
                                            potencia_total['potencia_instalada_kw'] += resultado_paineis['potencia_dict']['potencia_instalada_kw']
                                            potencia_total['producao_diaria_kwh'] += resultado_paineis['potencia_dict']['producao_diaria_kwh']
                                            potencia_total['producao_anual_kwh'] += resultado_paineis['potencia_dict']['producao_anual_kwh']
                                            potencia_total['economia_anual_brl'] += resultado_paineis['potencia_dict']['economia_anual_brl']
                                    
                                    # Construir resposta de telhado
                                    todos_telhados_com_paineis.append(
                                        TelhadorComPaineis(
                                            telhado_id=telhado_id,
                                            num_paineis=num_paineis,
                                            area_total_m2=resultado_paineis['potencia'].total_area_m2 if resultado_paineis['potencia'] else 0,
                                            potencia_instalada_kw=resultado_paineis['potencia'].potencia_instalada_kw if resultado_paineis['potencia'] else 0,
                                            producao_anual_kwh=resultado_paineis['potencia'].producao_anual_kwh if resultado_paineis['potencia'] else 0,
                                            economia_anual_brl=resultado_paineis['potencia'].economia_anual_brl if resultado_paineis['potencia'] else 0,
                                            paineis=resultado_paineis['paineis']
                                        )
                                    )
                                else:
                                    self.logger.info(f"⚪ Nenhum painel no telhado {telhado_id}")
                            
                            except Exception as e:
                                self.logger.error(f"Erro ao processar telhado: {e}")
                                erros.append(f"Telhado: {str(e)}")
                
                except Exception as e:
                    self.logger.error(f"Erro na imagem {idx}: {e}")
                    erros.append(f"Imagem {idx}: {str(e)}")
            
            # Compilar resposta final
            potencia_response = None
            if potencia_total:
                potencia_response = EstimativaPotenciaResponse(**potencia_total)
            
            tempo_decorrido = time.time() - tempo_inicio
            
            self.logger.info(f"✅ Pipeline concluído em {tempo_decorrido:.2f}s")
            self.logger.info(f"📊 Total: {total_telhados} telhados, {total_paineis} painéis")
            
            return {
                'sucesso': len(erros) == 0,
                'transformador_id': transformador_id,
                'num_imagens_processadas': len(imagens_grid),
                'total_telhados_detectados': total_telhados,
                'total_paineis_detectados': total_paineis,
                'telhados_com_paineis': todos_telhados_com_paineis,
                'potencia_total': potencia_response,
                'erros': erros,
                'tempo_processamento_s': tempo_decorrido,
                'timestamp': datetime.now()
            }
        
        except Exception as e:
            self.logger.error(f"❌ Erro no pipeline: {e}", exc_info=True)
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'erro': str(e),
                'tempo_processamento_s': time.time() - tempo_inicio,
                'timestamp': datetime.now()
            }
