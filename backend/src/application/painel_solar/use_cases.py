"""Application use cases for solar panel services"""

import logging
from typing import List, Dict, Optional

from ...domain.painel_solar import (
    PainelSolar,
    EstimativaPotencia,
    PropertyClassification,
    PropertyType,
    PainelSolarDTO,
    EstimativaPotenciaDTO,
    PropertyClassificationDTO,
)
from ...infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PropertyClassifier,
    PowerEstimator,
)

logger = logging.getLogger(__name__)


class DetectarPainelSolarUseCase:
    """Use case for detecting solar panels from roof images"""
    
    def __init__(self, detection_service: SolarPanelDetectionService):
        """
        Args:
            detection_service: Infrastructure ML service
        """
        self.detection_service = detection_service
    
    def executar(
        self,
        url_imagem: str,
        bbox: Dict[str, float],
        confianca_minima: float = 0.5,
    ) -> List[PainelSolar]:
        """
        Execute panel detection use case
        
        Args:
            url_imagem: Google Maps image URL
            bbox: Roof bounding box
            confianca_minima: Minimum confidence
        
        Returns:
            List of detected panels as domain entities
        """
        try:
            # Download and crop ROI
            roi = self.detection_service.baixar_roi_do_telhado(url_imagem, bbox)
            if roi is None:
                logger.error("Failed to download image")
                return []
            
            # Detect panels
            paineis = self.detection_service.detectar_paineis(roi, confianca_minima)
            
            logger.info(f"Successfully detected {len(paineis)} panels")
            return paineis
        
        except Exception as e:
            logger.error(f"Panel detection failed: {e}")
            return []


class ClassificarPropriedadeUseCase:
    """Use case for classifying property by solar installation type"""
    
    def __init__(self, classifier: PropertyClassifier):
        """
        Args:
            classifier: Domain service for classification
        """
        self.classifier = classifier
    
    def executar(
        self,
        detections: List[Dict],
        potencia_estimada_kw: Optional[float] = None,
    ) -> PropertyClassification:
        """
        Execute property classification use case
        
        Args:
            detections: List of detections
            potencia_estimada_kw: Estimated power (optional)
        
        Returns:
            Property classification domain entity
        """
        property_type, confidence, features = self.classifier.classify(
            detections, estimated_power=potencia_estimada_kw
        )
        
        return PropertyClassification(
            property_type=property_type,
            confidence=confidence,
            num_panels=features.get("num_panels", 0),
            avg_area_pixels=features.get("avg_area_pixels", 0),
            total_area_pixels=features.get("total_area_pixels", 0),
            avg_confidence=features.get("avg_confidence", 0),
            estimated_power_kw=potencia_estimada_kw,
        )


class EstimarPotenciaInstalacaoUseCase:
    """Use case for estimating solar installation capacity"""
    
    def __init__(self, estimator: PowerEstimator):
        """
        Args:
            estimator: Domain service for power estimation
        """
        self.estimator = estimator
    
    def executar(
        self,
        paineis: List[PainelSolar],
        potencia_por_m2: float = 150.0,
    ) -> EstimativaPotencia:
        """
        Execute power estimation use case
        
        Args:
            paineis: List of detected panels
            potencia_por_m2: Power density (W/m┬▓)
        
        Returns:
            Power estimation domain entity
        """
        if not paineis:
            logger.warning("No panels provided for estimation")
            return EstimativaPotencia(
                total_area_m2=0,
                num_paineis=0,
                potencia_instalada_kw=0,
            )
        
        # Calculate totals
        total_area_m2 = sum(p.area_m2 for p in paineis)
        num_paineis = len(paineis)
        potencia_w = total_area_m2 * potencia_por_m2
        potencia_kw = potencia_w / 1000
        
        # Create estimation entity
        estimativa = EstimativaPotencia(
            total_area_m2=total_area_m2,
            num_paineis=num_paineis,
            potencia_instalada_kw=potencia_kw,
            potencia_por_m2=potencia_por_m2,
        )
        
        # Calculate annual production and savings
        estimativa.calcular()
        
        logger.info(f"Power estimated: {potencia_kw:.2f} kW")
        logger.info(f"  - Area: {total_area_m2:.2f} m┬▓")
        logger.info(f"  - Panels: {num_paineis}")
        logger.info(f"  - Annual production: {estimativa.producao_anual_kwh:.0f} kWh")
        logger.info(f"  - Annual savings: R$ {estimativa.economia_anual_brl:.2f}")
        
        return estimativa


class EstimarProducaoAnualUseCase:
    """Use case for estimating annual energy production and ROI"""
    
    def __init__(self, estimator: PowerEstimator):
        """
        Args:
            estimator: Domain service for production estimation
        """
        self.estimator = estimator
    
    def executar(
        self,
        potencia_kw: float,
        location: str = "Brazil",
        capacity_factor: float = 0.18,
    ) -> Dict:
        """
        Execute annual production estimation use case
        
        Args:
            potencia_kw: Installed power in kW
            location: Geographic location
            capacity_factor: Capacity factor
        
        Returns:
            Dict with production and financial estimates
        """
        production = self.estimator.estimate_annual_production(
            potencia_kw, location, capacity_factor
        )
        
        logger.info(f"Annual production estimate for {potencia_kw:.2f} kW:")
        logger.info(f"  - Production: {production['annual_production_kwh']:,.0f} kWh/year")
        logger.info(f"  - Savings: R$ {production['annual_savings_brl']:,.2f}/year")
        logger.info(f"  - Payback: {production['estimated_payback_years']:.1f} years")
        
        return production


class PipelineCompleteDetectionUseCase:
    """Orchestrated use case combining all detection steps"""
    
    def __init__(
        self,
        detection_uc: DetectarPainelSolarUseCase,
        classification_uc: ClassificarPropriedadeUseCase,
        power_estimation_uc: EstimarPotenciaInstalacaoUseCase,
        production_uc: EstimarProducaoAnualUseCase,
    ):
        """
        Args:
            detection_uc: Panel detection use case
            classification_uc: Property classification use case
            power_estimation_uc: Power estimation use case
            production_uc: Production estimation use case
        """
        self.detection_uc = detection_uc
        self.classification_uc = classification_uc
        self.power_estimation_uc = power_estimation_uc
        self.production_uc = production_uc
    
    def executar(
        self,
        url_imagem: str,
        bbox: Dict[str, float],
        confianca_minima: float = 0.5,
        potencia_por_m2: float = 150.0,
    ) -> Dict:
        """
        Execute complete detection pipeline
        
        Args:
            url_imagem: Google Maps image URL
            bbox: Roof bounding box
            confianca_minima: Minimum confidence
            potencia_por_m2: Power density
        
        Returns:
            Complete result dict with all analyses
        """
        import time
        
        tempo_inicio = time.time()
        resultado = {
            "sucesso": False,
            "paineis": [],
            "classificacao": None,
            "potencia": None,
            "producao": None,
            "tempo_processamento_s": 0,
        }
        
        try:
            # Step 1: Detect panels
            logger.info("Pipeline: Step 1 - Detecting panels...")
            paineis = self.detection_uc.executar(url_imagem, bbox, confianca_minima)
            resultado["paineis"] = paineis
            
            if not paineis:
                logger.warning("No panels detected")
                resultado["tempo_processamento_s"] = time.time() - tempo_inicio
                return resultado
            
            # Step 2: Estimate power
            logger.info("Pipeline: Step 2 - Estimating power...")
            potencia_est = self.power_estimation_uc.executar(paineis, potencia_por_m2)
            resultado["potencia"] = potencia_est
            
            # Step 3: Classify property
            logger.info("Pipeline: Step 3 - Classifying property...")
            detections_dict = [
                {
                    "area_pixels": p.area_pixeis,
                    "confidence": p.confianca,
                }
                for p in paineis
            ]
            classificacao = self.classification_uc.executar(
                detections_dict, potencia_est.potencia_instalada_kw
            )
            resultado["classificacao"] = classificacao
            
            # Step 4: Estimate production
            logger.info("Pipeline: Step 4 - Estimating annual production...")
            producao = self.production_uc.executar(
                potencia_est.potencia_instalada_kw
            )
            resultado["producao"] = producao
            
            resultado["sucesso"] = True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            resultado["erro"] = str(e)
        
        finally:
            resultado["tempo_processamento_s"] = time.time() - tempo_inicio
        
        return resultado
