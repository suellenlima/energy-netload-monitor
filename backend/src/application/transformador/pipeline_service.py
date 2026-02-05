"""
Transformador Pipeline Application Service (DDD)

Orchestrates the complete pipeline for transformer processing:
1. Image grid generation and caching
2. Roof detection across images
3. Solar panel detection on roofs
4. Power estimation and persistence

This service acts as the Application Layer coordinator, orchestrating
domain entities (Transformador) and infrastructure services (detection, 
caching, persistence).

Architecture:
- Domain Layer: Transformador entity, value objects, repository interface
- Application Layer: This service + use cases
- Infrastructure Layer: TelhadoDetectionService, PainelSolarApplicationService, 
  image caching, database persistence
- API Layer: Endpoints that consume this service

Author: Energy Netload Monitor
Date: 2026-02-05 (DDD Migration)
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

from ...infrastructure.persistence.transformador_pipeline import TransformadorPipelineRepository
from ...application.telhado_detection.service import TelhadoDetectionService
from ...application.painel_solar import PainelSolarApplicationService
from ...schemas.painel_solar import (
    PainelSolarResponse,
    EstimativaPotenciaResponse,
    TelhadorComPaineis,
)


class TransformadorPipelineApplicationService:
    """
    Application service for transformer pipeline processing.
    
    Responsibilities:
    - Orchestrate complete pipeline workflow
    - Manage image caching
    - Coordinate roof and panel detection
    - Persist results to database
    - Provide domain-level abstractions to API layer
    
    Architecture Layer: APPLICATION LAYER (DDD)
    Dependencies:
    - Infrastructure: TransformadorPipelineRepository, image caching
    - Domain: TelhadoDetectionService, PainelSolarApplicationService
    - Schemas: Response DTOs
    """

    def __init__(self, engine):
        """
        Initialize the pipeline application service.
        
        Args:
            engine: SQLAlchemy Engine for database access
        """
        self.engine = engine
        self.repository = TransformadorPipelineRepository(engine)
        self.logger = logging.getLogger(__name__)
        
        # Lazy-loaded DDD services
        self._servico_telhados: Optional[TelhadoDetectionService] = None
        self._servico_paineis: Optional[PainelSolarApplicationService] = None
        
        # Initialize cache directory
        self.cache_dir = self._criar_dir_cache()

    # ========================================================================
    # SERVICE LAZY LOADING (DDD COORDINATION)
    # ========================================================================

    def _obter_servico_telhados(self) -> TelhadoDetectionService:
        """
        Lazy-load roof detection DDD service.
        
        Returns:
            Initialized TelhadoDetectionService
        """
        if self._servico_telhados is None:
            self.logger.info("🔧 Initializing DDD roof detection service...")
            self._servico_telhados = TelhadoDetectionService(engine=self.engine)
        return self._servico_telhados

    def _obter_servico_paineis(self) -> PainelSolarApplicationService:
        """
        Lazy-load solar panel detection DDD service.
        
        Returns:
            Initialized PainelSolarApplicationService
        """
        if self._servico_paineis is None:
            self.logger.info("🔧 Initializing DDD solar panel detection service...")
            self._servico_paineis = PainelSolarApplicationService()
        return self._servico_paineis

    # ========================================================================
    # IMAGE CACHE MANAGEMENT
    # ========================================================================

    def _criar_dir_cache(self) -> Path:
        """
        Create cache directory for satellite images if it doesn't exist.
        
        Returns:
            Path to cache directory
        """
        cache_dir = Path("data/cache/imagens_grid")
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _salvar_imagem_em_cache(self, 
                                url_imagem: str, 
                                transformador_id: int, 
                                indice: int) -> Optional[str]:
        """
        Download and cache satellite image locally.
        
        Implements lazy caching: returns existing cache before downloading new image.
        
        Args:
            url_imagem: Image URL (Google Maps or CBERS-4A)
            transformador_id: Transformer ID
            indice: Image index in grid
            
        Returns:
            Path to cached image or None on error
        """
        try:
            filename = f"trafo_{transformador_id}_img_{indice:03d}.png"
            filepath = self.cache_dir / filename
            
            # Check cache first
            if filepath.exists():
                self.logger.info(f"[CACHE] 💾 Image found in cache: {filepath}")
                return str(filepath)
            
            # Download image
            self.logger.info(f"[CACHE] 📥 Downloading image {indice}...")
            response = requests.get(url_imagem, timeout=30)
            response.raise_for_status()
            
            # Save to disk
            imagem = Image.open(BytesIO(response.content))
            if imagem.mode != 'RGB':
                imagem = imagem.convert('RGB')
            
            imagem.save(filepath)
            self.logger.info(f"[CACHE] ✅ Image saved: {filepath}")
            
            return str(filepath)
        
        except Exception as e:
            self.logger.error(f"[CACHE] ❌ Error saving image to cache: {e}")
            return None

    def _carregar_imagem_do_cache(self, caminho_imagem: str):
        """
        Load cached image as numpy array.
        
        Args:
            caminho_imagem: Path to cached image
            
        Returns:
            Numpy array or None on error
        """
        try:
            img = cv2.imread(caminho_imagem)
            if img is None:
                self.logger.error(f"[CACHE] ❌ Failed to load: {caminho_imagem}")
                return None
            return img
        except Exception as e:
            self.logger.error(f"[CACHE] ❌ Error loading image: {e}")
            return None

    # ========================================================================
    # PANEL PROCESSING
    # ========================================================================

    def _processar_telhado_para_paineis(self, 
                                        telhado_id: int,
                                        caminho_imagem: str,
                                        bbox: Dict,
                                        transformador_id: int,
                                        confianca_minima: float,
                                        potencia_por_m2: float) -> Dict:
        """
        Detect solar panels on a specific roof.
        
        Coordinates with PainelSolarApplicationService (DDD layer) to:
        1. Detect panels in roof bounding box
        2. Calculate power estimation
        3. Format results as response DTOs
        
        Args:
            telhado_id: Roof ID
            caminho_imagem: Path to cached image
            bbox: Roof bounding box
            transformador_id: Transformer ID
            confianca_minima: Minimum confidence threshold
            potencia_por_m2: Power per square meter (W)
            
        Returns:
            Dict with 'sucesso', 'num_paineis', 'paineis', 'potencia', 'potencia_dict'
        """
        try:
            servico_paineis = self._obter_servico_paineis()
            
            # Process roof for panels (DDD service)
            resultado_paineis = servico_paineis.processar_telhado_completo(
                url_imagem=caminho_imagem,
                bbox=bbox,
                confianca_minima=confianca_minima,
                potencia_por_m2=potencia_por_m2
            )
            
            if not resultado_paineis.sucesso or not resultado_paineis.paineis:
                return {'sucesso': False, 'num_paineis': 0}
            
            # Format panels as response DTOs
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
            self.logger.error(f"Error processing roof for panels: {e}")
            return {'sucesso': False, 'num_paineis': 0}

    def _salvar_paineis_do_telhado(self,
                                   telhado_id: int,
                                   transformador_id: int,
                                   paineis_response: List[PainelSolarResponse],
                                   potencia_response: Optional[EstimativaPotenciaResponse]) -> bool:
        """
        Save detected panels and power estimates to database.
        
        Persists:
        - Individual panels with bounding boxes and confidence
        - Power estimates per roof
        - Aggregated power estimates
        
        Args:
            telhado_id: Roof ID
            transformador_id: Transformer ID
            paineis_response: List of detected panels (DTOs)
            potencia_response: Power estimate DTO
            
        Returns:
            True if successful, False on error
        """
        try:
            # Fetch roof data
            telhado_data = self.repository.obter_telhado_por_id(telhado_id)
            if not telhado_data:
                self.logger.error(f"Roof {telhado_id} not found")
                return False
            
            trans_id = telhado_data['transformador_id']
            sub_id = telhado_data['subestacao_id']
            
            # Clear old panels
            self.repository.limpar_paineis_do_telhado(telhado_id)
            
            # Prepare panel data for batch insertion
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
            
            # Save panels in batch
            if paineis_data:
                self.repository.salvar_paineis_lote(paineis_data)
            
            # Save power estimates
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
            
            self.logger.info(f"✅ {len(paineis_response)} panels saved for roof {telhado_id}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error saving roof panels: {e}")
            return False

    # ========================================================================
    # MAIN PIPELINE ORCHESTRATION
    # ========================================================================

    def processar_transformador_completo(self,
                                         transformador_id: int,
                                         confianca_minima_telhados: float = 0.5,
                                         confianca_minima_paineis: float = 0.5) -> Dict:
        """
        Execute complete pipeline: Download images → Detect roofs → Detect panels.
        
        Workflow:
        1. Retrieve transformer coordinates and metadata
        2. Generate 3x3 grid of satellite images
        3. For each image:
           a. Download and cache satellite image
           b. Detect roofs using TelhadoDetectionService (DDD)
           c. For each roof, detect panels using PainelSolarApplicationService (DDD)
           d. Save results to database
        4. Aggregate and return consolidated results
        
        Args:
            transformador_id: Transformer ID to process
            confianca_minima_telhados: Minimum confidence for roof detection (0-1)
            confianca_minima_paineis: Minimum confidence for panel detection (0-1)
            
        Returns:
            Dict with pipeline results including:
            - sucesso: Pipeline success status
            - total_telhados_detectados: Total roofs detected
            - total_paineis_detectados: Total panels detected
            - telhados_com_paineis: List of roofs with panels
            - potencia_total: Aggregated power estimate
            - erros: List of errors encountered
            - tempo_processamento_s: Processing time in seconds
        """
        tempo_inicio = time.time()
        
        try:
            self.logger.info(f"🚀 Starting pipeline for transformer {transformador_id}")
            
            # Get DDD services
            servico_telhados = self._obter_servico_telhados()
            
            # Fetch transformer
            trafo_data = self.repository.obter_transformador(transformador_id)
            if not trafo_data:
                self.logger.error(f"Transformer {transformador_id} not found")
                return {
                    'sucesso': False,
                    'erro': f'Transformer {transformador_id} not found'
                }
            
            lat_trafo = trafo_data['latitude']
            lon_trafo = trafo_data['longitude']
            subestacao_id = trafo_data['subestacao_id']
            
            self.logger.info(f"📍 Transformer: lat={lat_trafo}, lon={lon_trafo}")
            
            # Generate image grid
            self.logger.info("🗺️ Generating image grid...")
            imagens_grid = servico_telhados.gerar_imagens_grid(lat_trafo, lon_trafo)
            self.logger.info(f"✓ {len(imagens_grid)} images found")
            
            # Clear old data
            self.repository.limpar_paineis_do_transformador(transformador_id)
            self.repository.limpar_potencia_do_transformador(transformador_id)
            
            # Accumulation variables
            total_telhados = 0
            total_paineis = 0
            todos_telhados_com_paineis = []
            potencia_total = None
            erros = []
            
            # Process each image in grid
            for idx, imagem in enumerate(imagens_grid, 1):
                try:
                    self.logger.info(f"⏳ Processing image {idx}/{len(imagens_grid)}")
                    
                    # ====== STAGE 1: DOWNLOAD AND CACHE IMAGE ======
                    caminho_imagem = self._salvar_imagem_em_cache(
                        imagem['url'], 
                        transformador_id, 
                        idx
                    )
                    
                    if not caminho_imagem:
                        erros.append(f"Image {idx}: Error saving to cache")
                        continue
                    
                    # Save image reference to database
                    self.repository.salvar_referencia_imagem(
                        transformador_id, 
                        idx, 
                        imagem['url'], 
                        caminho_imagem
                    )
                    
                    # ====== STAGE 2: DETECT ROOFS ======
                    self.logger.info(f"🏠 Detecting roofs...")
                    
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
                        self.logger.info(f"✅ {num_telhados} roofs detected")
                        total_telhados += num_telhados
                    
                    # ====== STAGE 3: DETECT PANELS ON ROOFS ======
                    if resultado_telhados['sucesso'] and resultado_telhados['telhados_segmentados']:
                        self.logger.info(f"☀️ Detecting panels on roofs...")
                        
                        for telhado in resultado_telhados['telhados_segmentados']:
                            try:
                                telhado_id = telhado['id']
                                bbox = telhado['bbox']
                                
                                # Process panels on roof
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
                                    self.logger.info(f"✅ {num_paineis} panels detected on roof {telhado_id}")
                                    total_paineis += num_paineis
                                    
                                    # Save panels to database
                                    self._salvar_paineis_do_telhado(
                                        telhado_id=telhado_id,
                                        transformador_id=transformador_id,
                                        paineis_response=resultado_paineis['paineis'],
                                        potencia_response=resultado_paineis['potencia']
                                    )
                                    
                                    # Accumulate total power
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
                                    
                                    # Build roof response
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
                                    self.logger.info(f"⚪ No panels on roof {telhado_id}")
                            
                            except Exception as e:
                                self.logger.error(f"Error processing roof: {e}")
                                erros.append(f"Roof: {str(e)}")
                
                except Exception as e:
                    self.logger.error(f"Error on image {idx}: {e}")
                    erros.append(f"Image {idx}: {str(e)}")
            
            # Compile final response
            potencia_response = None
            if potencia_total:
                potencia_response = EstimativaPotenciaResponse(**potencia_total)
            
            tempo_decorrido = time.time() - tempo_inicio
            
            self.logger.info(f"✅ Pipeline completed in {tempo_decorrido:.2f}s")
            self.logger.info(f"📊 Total: {total_telhados} roofs, {total_paineis} panels")
            
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
            self.logger.error(f"❌ Pipeline error: {e}", exc_info=True)
            return {
                'sucesso': False,
                'transformador_id': transformador_id,
                'erro': str(e),
                'tempo_processamento_s': time.time() - tempo_inicio,
                'timestamp': datetime.now()
            }
