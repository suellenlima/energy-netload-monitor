"""Infrastructure layer for solar panel ML detection"""

import os
import logging
from typing import List, Dict, Optional
import numpy as np
import cv2
import requests
from io import BytesIO
from PIL import Image

try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None

from ...domain.painel_solar import PainelSolar, PropertyType, BoundingBox, Centroide

logger = logging.getLogger(__name__)


class SolarPanelDetectionService:
    """Infrastructure service for YOLOv8-based solar panel detection"""
    
    def __init__(self, modelo_yolo_path: Optional[str] = None):
        """
        Initialize ML detection service
        
        Args:
            modelo_yolo_path: Path to trained YOLOv8 model
                             If None, uses default model
        """
        if modelo_yolo_path is None:
            workspace_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "..")
            )
            modelo_yolo_path = os.path.join(
                workspace_root,
                "notebooks",
                "runs",
                "detect",
                "solar_panel_detection",
                "yolov8_solar3",
                "weights",
                "best.pt",
            )
        
        self.modelo_yolo_path = modelo_yolo_path
        self.modelo_yolo = None
        
        # Load YOLO model
        if YOLO is not None:
            try:
                self.modelo_yolo = YOLO(self.modelo_yolo_path)
                logger.info(f"ÔÜí YOLO model loaded: {self.modelo_yolo_path}")
            except Exception as e:
                logger.warning(f"Error loading YOLO: {e}")
        else:
            logger.error("ultralytics not installed!")
    
    def baixar_roi_do_telhado(
        self,
        url_imagem: str,
        bbox: Dict[str, float],
        timeout: int = 30,
        sem_autenticacao: bool = True,
    ) -> Optional[np.ndarray]:
        """
        Download roof image and crop ROI
        
        Args:
            url_imagem: Google Maps image URL
            bbox: Roof bounding box {"x": int, "y": int, "w": int, "h": int}
            timeout: Timeout in seconds
            sem_autenticacao: If True, download without authentication
            
        Returns:
            ROI image as numpy array (BGR) or None if error
        """
        try:
            logger.info("Downloading image from URL...")
            
            response = requests.get(url_imagem, timeout=timeout)
            response.raise_for_status()
            
            # Load image
            imagem = Image.open(BytesIO(response.content))
            if imagem.mode != "RGB":
                imagem = imagem.convert("RGB")
            
            imagem_array = np.array(imagem)
            imagem_array = cv2.cvtColor(imagem_array, cv2.COLOR_RGB2BGR)
            
            logger.info(f"Image downloaded: {imagem_array.shape}")
            
            # Crop ROI
            x = int(bbox.get("x", 0))
            y = int(bbox.get("y", 0))
            w = int(bbox.get("w", imagem_array.shape[1]))
            h = int(bbox.get("h", imagem_array.shape[0]))
            
            # Validate coordinates
            x = max(0, min(x, imagem_array.shape[1]))
            y = max(0, min(y, imagem_array.shape[0]))
            w = min(w, imagem_array.shape[1] - x)
            h = min(h, imagem_array.shape[0] - y)
            
            roi = imagem_array[y : y + h, x : x + w]
            
            logger.info(f"ROI extracted: {roi.shape}")
            return roi
            
        except Exception as e:
            logger.error(f"Error downloading ROI: {e}")
            return None
    
    def detectar_paineis(
        self,
        imagem_roi: np.ndarray,
        confianca_minima: float = 0.5,
    ) -> List[PainelSolar]:
        """
        Detect solar panels in ROI using YOLOv8
        
        Args:
            imagem_roi: ROI image (BGR)
            confianca_minima: Minimum confidence for detection
            
        Returns:
            List of detected panels as domain entities
        """
        if self.modelo_yolo is None:
            logger.error("YOLO model not loaded!")
            return []
        
        try:
            logger.info("Detecting solar panels...")
            
            # Run detection
            resultados = self.modelo_yolo.predict(
                imagem_roi,
                conf=confianca_minima,
                verbose=False,
            )
            
            paineis = []
            
            for i, resultado in enumerate(resultados):
                if resultado.boxes is None:
                    logger.info("No panels detected in ROI")
                    continue
                
                for j, box in enumerate(resultado.boxes):
                    try:
                        # Extract coordinates
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        conf = float(box.conf[0].cpu().numpy())
                        
                        # Calculate bbox (x, y, w, h)
                        x, y = int(x1), int(y1)
                        w, h = int(x2 - x1), int(y2 - y1)
                        
                        # Centroid
                        cx, cy = x + w // 2, y + h // 2
                        
                        # Area in pixels
                        area_px = w * h
                        
                        # Area in m┬▓ (0.3m/pixel for Google Maps zoom 20)
                        pixel_para_m = 0.3
                        area_m2 = (w * pixel_para_m) * (h * pixel_para_m)
                        
                        painel = PainelSolar(
                            id_painel=f"painel_{j+1}",
                            bbox=BoundingBox(x=x, y=y, w=w, h=h),
                            centroide=Centroide(x=float(cx), y=float(cy)),
                            area_pixeis=area_px,
                            area_m2=area_m2,
                            confianca=conf,
                            tipo_painel="desconhecido",
                        )
                        
                        paineis.append(painel)
                        
                    except Exception as e:
                        logger.warning(f"Error processing detection {j}: {e}")
            
            logger.info(f"{len(paineis)} panels detected")
            return paineis
            
        except Exception as e:
            logger.error(f"Error detecting panels: {e}")
            return []


class PropertyClassifier:
    """Domain service for property classification"""
    
    def __init__(self):
        """Initialize classifier with default thresholds"""
        self.property_types = {
            PropertyType.RESIDENCIAL: {
                "min_panels": 1,
                "max_panels": 5,
                "power_range": (3, 10),  # kW
                "avg_area_range": (500, 2000),  # pixels
            },
            PropertyType.COMERCIAL: {
                "min_panels": 5,
                "max_panels": 20,
                "power_range": (10, 50),
                "avg_area_range": (1500, 5000),
            },
            PropertyType.INDUSTRIAL: {
                "min_panels": 20,
                "max_panels": 500,
                "power_range": (50, 500),
                "avg_area_range": (3000, 20000),
            },
        }
    
    def classify(
        self,
        detections: List[Dict],
        estimated_power: Optional[float] = None,
    ) -> tuple[PropertyType, float, Dict]:
        """
        Classify property based on detections
        
        Args:
            detections: List of detections with area and confidence
            estimated_power: Estimated power in kW (optional)
        
        Returns:
            Tuple (property_type, confidence, features)
        """
        if not detections:
            return PropertyType.UNKNOWN, 0.0, {}
        
        # Extract features
        num_panels = len(detections)
        areas = [d.get("area_pixels", 0) for d in detections]
        avg_area = np.mean(areas) if areas else 0
        total_area = sum(areas)
        confidences = [d.get("confidence", 0) for d in detections]
        avg_confidence = np.mean(confidences) if confidences else 0
        
        features = {
            "num_panels": num_panels,
            "avg_area_pixels": avg_area,
            "total_area_pixels": total_area,
            "avg_confidence": avg_confidence,
            "estimated_power_kw": estimated_power,
        }
        
        # Classification rules
        
        # 1. Based on power (if available)
        if estimated_power:
            if estimated_power < 10:
                return PropertyType.RESIDENCIAL, 0.9, features
            elif estimated_power < 50:
                return PropertyType.COMERCIAL, 0.85, features
            else:
                return PropertyType.INDUSTRIAL, 0.8, features
        
        # 2. Based on number of panels
        if num_panels <= 5:
            confidence = 0.7 if avg_area < 2000 else 0.6
            return PropertyType.RESIDENCIAL, confidence, features
        
        elif num_panels <= 20:
            if avg_area > 4000:
                return PropertyType.INDUSTRIAL, 0.9, features
            else:
                confidence = 0.75 if avg_area > 1500 else 0.65
                return PropertyType.COMERCIAL, confidence, features
        
        else:  # 20+ panels
            confidence = 0.9 if num_panels > 50 else 0.8
            return PropertyType.INDUSTRIAL, confidence, features


class PowerEstimator:
    """Domain service for power estimation"""
    
    def __init__(self, resolution_m_per_pixel: float = 0.3):
        """
        Args:
            resolution_m_per_pixel: Image resolution (meters per pixel)
        """
        self.resolution = resolution_m_per_pixel
        self.power_density = 200  # W/m┬▓
        self.efficiency = 0.20  # 20%
        self.avg_panel_power_w = 400  # Watts
    
    def pixels_to_meters(self, pixels: float) -> float:
        """Convert pixel area to square meters"""
        meters_squared = pixels * (self.resolution ** 2)
        return meters_squared
    
    def estimate_power(
        self,
        detections: List[Dict],
        power_density: float = 200,
        efficiency: float = 0.20,
    ) -> Dict:
        """
        Estimate total installation power
        
        Args:
            detections: List of detections with area in pixels
            power_density: Power density (W/m┬▓)
            efficiency: Panel efficiency
        
        Returns:
            Dict with power and area estimates
        """
        if not detections:
            return {
                "total_power_kw": 0,
                "total_area_m2": 0,
                "num_panels": 0,
                "method": "no_detections",
            }
        
        # Calculate total area
        total_pixels = sum(d.get("area_pixels", 0) for d in detections)
        total_area_m2 = self.pixels_to_meters(total_pixels)
        
        # Method 1: Based on area
        total_power_w = total_area_m2 * power_density * efficiency
        total_power_kw_area = total_power_w / 1000
        
        # Method 2: Based on count (400W per panel)
        num_panels = len(detections)
        total_power_kw_count = num_panels * (self.avg_panel_power_w / 1000)
        
        # Use average of both methods
        total_power_kw = (total_power_kw_area + total_power_kw_count) / 2
        
        return {
            "total_power_kw": total_power_kw,
            "power_from_area_kw": total_power_kw_area,
            "power_from_count_kw": total_power_kw_count,
            "total_area_m2": total_area_m2,
            "num_panels_detected": num_panels,
            "avg_power_per_panel_kw": total_power_kw / num_panels if num_panels > 0 else 0,
            "power_density_used": power_density,
            "efficiency_used": efficiency,
            "resolution_m_per_pixel": self.resolution,
            "method": "hybrid_area_and_count",
        }
    
    def estimate_annual_production(
        self,
        power_kw: float,
        location: str = "Brazil",
        capacity_factor: float = 0.18,
    ) -> Dict:
        """
        Estimate annual energy production
        
        Args:
            power_kw: Installed power in kW
            location: Geographic location
            capacity_factor: Capacity factor (0.18 = 18% for Brazil)
        
        Returns:
            Dict with production and savings estimates
        """
        # Hours per year
        hours_per_year = 365.25 * 24
        
        # Annual production (kWh)
        annual_production_kwh = power_kw * hours_per_year * capacity_factor
        
        # Daily average
        daily_avg_kwh = annual_production_kwh / 365.25
        
        # Monthly average
        monthly_avg_kwh = annual_production_kwh / 12
        
        # Estimated savings (average Brazil tariff: R$ 0.75/kWh)
        tariff_brl_per_kwh = 0.75
        annual_savings_brl = annual_production_kwh * tariff_brl_per_kwh
        monthly_savings_brl = annual_savings_brl / 12
        
        # Simple ROI (average cost: R$ 4,500/kWp)
        installation_cost_per_kw = 4500
        total_investment_brl = power_kw * installation_cost_per_kw
        payback_years = (
            total_investment_brl / annual_savings_brl
            if annual_savings_brl > 0
            else 0
        )
        
        return {
            "annual_production_kwh": annual_production_kwh,
            "daily_avg_kwh": daily_avg_kwh,
            "monthly_avg_kwh": monthly_avg_kwh,
            "annual_savings_brl": annual_savings_brl,
            "monthly_savings_brl": monthly_savings_brl,
            "capacity_factor": capacity_factor,
            "location": location,
            "tariff_brl_per_kwh": tariff_brl_per_kwh,
            "estimated_investment_brl": total_investment_brl,
            "estimated_payback_years": payback_years,
        }
