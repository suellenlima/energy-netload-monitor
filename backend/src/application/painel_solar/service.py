"""Application layer for solar panel detection and analysis services"""

import logging
import time
from typing import List, Dict, Optional

from ...domain.painel_solar import (
    PainelSolar,
    EstimativaPotencia,
    PropertyType,
    PropertyClassification,
    PainelSolarDTO,
    EstimativaPotenciaDTO,
    PropertyClassificationDTO,
    DetectionResultDTO,
)
from ...infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PropertyClassifier,
    PowerEstimator,
)

logger = logging.getLogger(__name__)


class PainelSolarApplicationService:
    """Application service orchestrating solar panel detection and analysis"""
    
    def __init__(self, modelo_yolo_path: Optional[str] = None):
        """
        Initialize application service
        
        Args:
            modelo_yolo_path: Path to trained YOLOv8 model
        """
        self.detection_service = SolarPanelDetectionService(modelo_yolo_path)
        self.classifier = PropertyClassifier()
        self.estimator = PowerEstimator()
    
    def detectar_paineis_em_url(
        self,
        url_imagem: str,
        bbox: Dict[str, float],
        confianca_minima: float = 0.5,
    ) -> List[PainelSolarDTO]:
        """
        Detect solar panels from URL image
        
        Args:
            url_imagem: Google Maps image URL
            bbox: Roof bounding box
            confianca_minima: Minimum confidence threshold
        
        Returns:
            List of detected panels as DTOs
        """
        try:
            # Download and crop ROI
            roi = self.detection_service.baixar_roi_do_telhado(url_imagem, bbox)
            if roi is None:
                logger.error("Failed to download image")
                return []
            
            # Detect panels
            paineis = self.detection_service.detectar_paineis(roi, confianca_minima)
            
            # Convert to DTOs
            return [
                PainelSolarDTO(
                    id_painel=p.id_painel,
                    bbox=p.bbox.to_dict(),
                    centroide=p.centroide.to_dict(),
                    area_pixeis=p.area_pixeis,
                    area_m2=p.area_m2,
                    confianca=p.confianca,
                    tipo_painel=p.tipo_painel,
                    timestamp_deteccao=p.timestamp_deteccao,
                )
                for p in paineis
            ]
        
        except Exception as e:
            logger.error(f"Error detecting panels: {e}")
            return []
    
    def classificar_propriedade(
        self,
        detections: List[Dict],
        potencia_estimada_kw: Optional[float] = None,
    ) -> PropertyClassificationDTO:
        """
        Classify property based on detections
        
        Args:
            detections: List of detections with area and confidence
            potencia_estimada_kw: Estimated power in kW (optional)
        
        Returns:
            Property classification DTO
        """
        property_type, confidence, features = self.classifier.classify(
            detections, estimated_power=potencia_estimada_kw
        )
        
        return PropertyClassificationDTO(
            tipo=property_type.value,
            confianca=confidence,
            descricao=property_type.description,
            faixa_potencia_kw=property_type.power_range(),
            num_paineis=features.get("num_panels", 0),
            potencia_estimada_kw=potencia_estimada_kw,
        )
    
    def estimar_potencia(
        self,
        paineis: List[PainelSolar],
        potencia_por_m2: float = 150.0,
    ) -> EstimativaPotenciaDTO:
        """
        Estimate installed power capacity
        
        Args:
            paineis: List of detected panels
            potencia_por_m2: Power per square meter (default 150 W/m┬▓)
        
        Returns:
            Power estimation DTO
        """
        if not paineis:
            logger.warning("No panels to estimate power")
            return EstimativaPotenciaDTO(
                total_area_m2=0,
                num_paineis=0,
                potencia_instalada_kw=0,
            )
        
        # Calculate totals
        total_area_m2 = sum(p.area_m2 for p in paineis)
        num_paineis = len(paineis)
        potencia_w = total_area_m2 * potencia_por_m2
        potencia_kw = potencia_w / 1000
        
        # Create estimation domain entity
        estimativa = EstimativaPotencia(
            total_area_m2=total_area_m2,
            num_paineis=num_paineis,
            potencia_instalada_kw=potencia_kw,
            potencia_por_m2=potencia_por_m2,
        )
        
        # Calculate annual production
        estimativa.calcular()
        
        logger.info(f"Power estimate: {potencia_kw:.2f} kW")
        logger.info(f"  - Area: {total_area_m2:.2f} m┬▓")
        logger.info(f"  - Panels: {num_paineis}")
        logger.info(f"  - Annual production: {estimativa.producao_anual_kwh:.0f} kWh")
        
        return EstimativaPotenciaDTO(
            total_area_m2=estimativa.total_area_m2,
            num_paineis=estimativa.num_paineis,
            potencia_instalada_kw=estimativa.potencia_instalada_kw,
            producao_diaria_kwh=estimativa.producao_diaria_kwh,
            producao_anual_kwh=estimativa.producao_anual_kwh,
            economia_anual_brl=estimativa.economia_anual_brl,
        )
    
    def processar_telhado_completo(
        self,
        url_imagem: str,
        bbox: Dict[str, float],
        confianca_minima: float = 0.5,
        potencia_por_m2: float = 150.0,
    ) -> DetectionResultDTO:
        """
        Complete roof processing pipeline
        
        Args:
            url_imagem: Google Maps image URL
            bbox: Roof bounding box
            confianca_minima: Minimum confidence threshold
            potencia_por_m2: Power per square meter
        
        Returns:
            Complete detection result DTO
        """
        tempo_inicio = time.time()
        resultado = DetectionResultDTO(sucesso=False)
        
        try:
            # Stage 1: Download and detect
            logger.info("Stage 1/3: Downloading and detecting panels...")
            
            roi = self.detection_service.baixar_roi_do_telhado(url_imagem, bbox)
            if roi is None:
                resultado.erros.append("Failed to download image")
                return resultado
            
            paineis = self.detection_service.detectar_paineis(roi, confianca_minima)
            
            # Convert to DTOs
            resultado.paineis = [
                PainelSolarDTO(
                    id_painel=p.id_painel,
                    bbox=p.bbox.to_dict(),
                    centroide=p.centroide.to_dict(),
                    area_pixeis=p.area_pixeis,
                    area_m2=p.area_m2,
                    confianca=p.confianca,
                    tipo_painel=p.tipo_painel,
                )
                for p in paineis
            ]
            
            # Stage 2: Estimate power
            logger.info("Stage 2/3: Estimating power...")
            if paineis:
                estimativa = self.estimar_potencia(paineis, potencia_por_m2)
                resultado.estimativa_potencia = estimativa
            
            # Stage 3: Classify property
            logger.info("Stage 3/3: Classifying property...")
            detections_dict = [
                {
                    "area_pixels": p.area_pixeis,
                    "confidence": p.confianca,
                }
                for p in paineis
            ]
            
            potencia_kw = (
                resultado.estimativa_potencia.potencia_instalada_kw
                if resultado.estimativa_potencia
                else None
            )
            
            classificacao = self.classificar_propriedade(
                detections_dict, potencia_kw
            )
            resultado.classificacao = classificacao
            
            resultado.sucesso = True
            
        except Exception as e:
            erro_msg = f"Error processing roof: {str(e)}"
            logger.error(f"Error: {erro_msg}")
            resultado.erros.append(erro_msg)
        
        finally:
            resultado.tempo_processamento_s = time.time() - tempo_inicio
        
        return resultado
    
    def classificar_e_estimar_completo(
        self,
        detections: List[Dict],
        resolution_m_per_pixel: float = 0.3,
    ) -> Dict:
        """
        Complete classification and estimation function
        
        Args:
            detections: List of detections with area_pixels and confidence
            resolution_m_per_pixel: Image resolution
        
        Returns:
            Complete dict with classification, power, and production
        """
        # Estimate power
        power_estimate = self.estimator.estimate_power(detections)
        power_kw = power_estimate["total_power_kw"]
        
        # Classify property
        property_type, confidence, features = self.classifier.classify(
            detections, estimated_power=power_kw
        )
        
        # Estimate annual production
        production = self.estimator.estimate_annual_production(power_kw)
        
        # Complete result
        return {
            "classificacao": {
                "tipo": property_type.value,
                "confianca": confidence,
                "descricao": property_type.description,
                "faixa_potencia_kw": property_type.power_range(),
            },
            "potencia": power_estimate,
            "producao_anual": production,
            "deteccoes": {
                "num_paineis": len(detections),
                "confianca_media": features.get("avg_confidence", 0),
                "area_total_m2": power_estimate["total_area_m2"],
            },
        }
