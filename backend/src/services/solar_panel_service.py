"""
Serviço Unificado de Painéis Solares

Pipeline especializado para:
1. Classificação de propriedades por tipo de instalação solar
2. Detecção de painéis solares usando YOLOv8
3. Estimativa de potência instalada
4. Cálculo de produção anual e economia

Unifica funcionalidades de:
- solar_panel_classifier.py (classificação de propriedades e cálculo de potência)
- painel_solar_detection_service.py (detecção YOLO e pipeline)

Author: Energy Netload Monitor
Date: 2026-02-01
"""

import os
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import numpy as np
import cv2
import requests
from io import BytesIO
from PIL import Image

try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None

logger = logging.getLogger(__name__)


# ===========================
# DATA MODELS / DATACLASSES
# ===========================

@dataclass
class PainelSolarDetectado:
    """Painel solar detectado em uma ROI"""
    
    id_painel: str
    bbox: Dict[str, float]  # {x, y, w, h} em pixels
    centroide: Dict[str, float]  # {x, y}
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_painel: str  # monocristalino, policristalino, filme fino, desconhecido
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Converte para dicionário"""
        return {
            'id_painel': self.id_painel,
            'bbox': self.bbox,
            'centroide': self.centroide,
            'area_pixeis': self.area_pixeis,
            'area_m2': self.area_m2,
            'confianca': self.confianca,
            'tipo_painel': self.tipo_painel,
            'timestamp_deteccao': self.timestamp_deteccao.isoformat()
        }


@dataclass
class EstimativaPotencia:
    """Estimativa de potência e produção"""
    
    total_area_m2: float
    num_paineis: int
    potencia_instalada_kw: float
    potencia_por_m2: float = 150.0  # W/m² (padrão para painéis modernos)
    
    # Produção anual (Brasil)
    producao_anual_kwh: float = 0.0
    producao_diaria_kwh: float = 0.0
    fator_capacidade: float = 0.15  # 15% é padrão (varia 12-18% dependendo região)
    insolacao_media_kwh_m2_dia: float = 4.5  # Brasil: 4-5.5 kWh/m²/dia
    
    economia_anual_brl: float = 0.0
    tarifa_media_brl_kwh: float = 0.80  # Tarifa média Brasil (2026)
    
    def calcular(self):
        """Calcula produção anual e economia"""
        # Produção diária = Potência × Insolação / 1000
        self.producao_diaria_kwh = (self.potencia_instalada_kw * self.insolacao_media_kwh_m2_dia) / 1000
        
        # Produção anual
        self.producao_anual_kwh = self.producao_diaria_kwh * 365
        
        # Economia anual
        self.economia_anual_brl = self.producao_anual_kwh * self.tarifa_media_brl_kwh
        
        return self
    
    def to_dict(self) -> Dict:
        """Converte para dicionário"""
        return {
            'total_area_m2': self.total_area_m2,
            'num_paineis': self.num_paineis,
            'potencia_instalada_kw': self.potencia_instalada_kw,
            'producao_diaria_kwh': self.producao_diaria_kwh,
            'producao_anual_kwh': self.producao_anual_kwh,
            'fator_capacidade': self.fator_capacidade,
            'economia_anual_brl': self.economia_anual_brl
        }


# ===========================
# CLASSIFICATION SERVICE
# ===========================

class PropertyClassifier:
    """
    Classifica propriedades baseado em detecções de painéis solares.
    
    Critérios de classificação:
    - **Residencial**: 1-5 painéis pequenos (3-10 kW)
    - **Comercial**: 5-20 painéis médios (10-50 kW)  
    - **Industrial**: 20+ painéis grandes (50+ kW)
    """
    
    def __init__(self):
        """Inicializa classificador com thresholds padrão"""
        self.property_types = {
            'residencial': {
                'min_panels': 1,
                'max_panels': 5,
                'power_range': (3, 10),  # kW
                'avg_area_range': (500, 2000),  # pixels
                'description': 'Residência unifamiliar'
            },
            'comercial': {
                'min_panels': 5,
                'max_panels': 20,
                'power_range': (10, 50),
                'avg_area_range': (1500, 5000),
                'description': 'Estabelecimento comercial'
            },
            'industrial': {
                'min_panels': 20,
                'max_panels': 500,
                'power_range': (50, 500),
                'avg_area_range': (3000, 20000),
                'description': 'Indústria ou grande instalação'
            }
        }
    
    def classify(
        self, 
        detections: List[Dict], 
        estimated_power: Optional[float] = None
    ) -> Tuple[str, float, Dict]:
        """
        Classifica propriedade baseado em detecções.
        
        Args:
            detections: Lista de detecções com área e confiança
            estimated_power: Potência estimada em kW (opcional)
        
        Returns:
            Tupla (tipo, confiança, features) onde:
            - tipo: 'residencial', 'comercial' ou 'industrial'
            - confiança: 0.0 - 1.0
            - features: dict com características extraídas
        """
        if not detections:
            return 'unknown', 0.0, {}
        
        # Extrair features
        num_panels = len(detections)
        areas = [d.get('area_pixels', 0) for d in detections]
        avg_area = np.mean(areas) if areas else 0
        total_area = sum(areas)
        confidences = [d.get('confidence', 0) for d in detections]
        avg_confidence = np.mean(confidences) if confidences else 0
        
        features = {
            'num_panels': num_panels,
            'avg_area_pixels': avg_area,
            'total_area_pixels': total_area,
            'avg_confidence': avg_confidence,
            'estimated_power_kw': estimated_power
        }
        
        # Regras de classificação
        
        # 1. Baseado em potência (se disponível)
        if estimated_power:
            if estimated_power < 10:
                return 'residencial', 0.9, features
            elif estimated_power < 50:
                return 'comercial', 0.85, features
            else:
                return 'industrial', 0.8, features
        
        # 2. Baseado em número de painéis
        if num_panels <= 5:
            confidence = 0.7 if avg_area < 2000 else 0.6
            return 'residencial', confidence, features
        
        elif num_panels <= 20:
            # Verificar área média
            if avg_area > 4000:
                return 'industrial', 0.9, features
            else:
                confidence = 0.75 if avg_area > 1500 else 0.65
                return 'comercial', confidence, features
        
        else:  # 20+ painéis
            confidence = 0.9 if num_panels > 50 else 0.8
            return 'industrial', confidence, features
    
    def get_description(self, property_type: str) -> str:
        """Retorna descrição do tipo de propriedade"""
        return self.property_types.get(property_type, {}).get(
            'description', 
            'Tipo desconhecido'
        )
    
    def get_power_range(self, property_type: str) -> Tuple[float, float]:
        """Retorna faixa de potência típica (min, max) em kW"""
        return self.property_types.get(property_type, {}).get(
            'power_range',
            (0, 0)
        )


class PowerEstimator:
    """
    Estima potência de instalação solar baseado em detecções.
    
    Premissas:
    - Resolução padrão: 0.3m/pixel (Google Maps zoom 20)
    - Densidade de potência: 200 W/m²
    - Eficiência: 20%
    - Painel médio: 400 Wp (2m²)
    """
    
    def __init__(self, resolution_m_per_pixel: float = 0.3):
        """
        Args:
            resolution_m_per_pixel: Resolução da imagem (metros por pixel)
        """
        self.resolution = resolution_m_per_pixel
        self.power_density = 200  # W/m²
        self.efficiency = 0.20  # 20%
        self.avg_panel_power_w = 400  # Watts
    
    def pixels_to_meters(self, pixels: float) -> float:
        """Converte área de pixels para metros quadrados"""
        meters_squared = pixels * (self.resolution ** 2)
        return meters_squared
    
    def estimate_power(
        self, 
        detections: List[Dict],
        power_density: float = 200,
        efficiency: float = 0.20
    ) -> Dict:
        """
        Estima potência total da instalação.
        
        Args:
            detections: Lista de detecções com área em pixels
            power_density: Densidade de potência (W/m²)
            efficiency: Eficiência dos painéis
        
        Returns:
            Dict com estimativas de potência e área
        """
        if not detections:
            return {
                'total_power_kw': 0,
                'total_area_m2': 0,
                'num_panels': 0,
                'method': 'no_detections'
            }
        
        # Calcular área total
        total_pixels = sum(d.get('area_pixels', 0) for d in detections)
        total_area_m2 = self.pixels_to_meters(total_pixels)
        
        # Método 1: Baseado em área
        total_power_w = total_area_m2 * power_density * efficiency
        total_power_kw_area = total_power_w / 1000
        
        # Método 2: Baseado em contagem (400W por painel)
        num_panels = len(detections)
        total_power_kw_count = num_panels * (self.avg_panel_power_w / 1000)
        
        # Usar média dos dois métodos
        total_power_kw = (total_power_kw_area + total_power_kw_count) / 2
        
        return {
            'total_power_kw': total_power_kw,
            'power_from_area_kw': total_power_kw_area,
            'power_from_count_kw': total_power_kw_count,
            'total_area_m2': total_area_m2,
            'num_panels_detected': num_panels,
            'avg_power_per_panel_kw': total_power_kw / num_panels if num_panels > 0 else 0,
            'power_density_used': power_density,
            'efficiency_used': efficiency,
            'resolution_m_per_pixel': self.resolution,
            'method': 'hybrid_area_and_count'
        }
    
    def estimate_annual_production(
        self, 
        power_kw: float,
        location: str = 'Brazil',
        capacity_factor: float = 0.18
    ) -> Dict:
        """
        Estima produção anual de energia.
        
        Args:
            power_kw: Potência instalada em kW
            location: Localização geográfica
            capacity_factor: Fator de capacidade (0.18 = 18% para Brasil)
        
        Returns:
            Dict com estimativas de produção e economia
        """
        # Horas por ano
        hours_per_year = 365.25 * 24
        
        # Produção anual (kWh)
        annual_production_kwh = power_kw * hours_per_year * capacity_factor
        
        # Produção diária média
        daily_avg_kwh = annual_production_kwh / 365.25
        
        # Produção mensal média
        monthly_avg_kwh = annual_production_kwh / 12
        
        # Economia estimada (tarifa média Brasil: R$ 0.75/kWh)
        tariff_brl_per_kwh = 0.75
        annual_savings_brl = annual_production_kwh * tariff_brl_per_kwh
        monthly_savings_brl = annual_savings_brl / 12
        
        # ROI simplificado (custo médio: R$ 4.500/kWp)
        installation_cost_per_kw = 4500
        total_investment_brl = power_kw * installation_cost_per_kw
        payback_years = total_investment_brl / annual_savings_brl if annual_savings_brl > 0 else 0
        
        return {
            'annual_production_kwh': annual_production_kwh,
            'daily_avg_kwh': daily_avg_kwh,
            'monthly_avg_kwh': monthly_avg_kwh,
            'annual_savings_brl': annual_savings_brl,
            'monthly_savings_brl': monthly_savings_brl,
            'capacity_factor': capacity_factor,
            'location': location,
            'tariff_brl_per_kwh': tariff_brl_per_kwh,
            'estimated_investment_brl': total_investment_brl,
            'estimated_payback_years': payback_years
        }


# ===========================
# DETECTION SERVICE
# ===========================

class SolarPanelService:
    """Serviço unificado de detecção e análise de painéis solares"""
    
    def __init__(self, modelo_yolo_path: str = None):
        """
        Inicializa o serviço
        
        Args:
            modelo_yolo_path: Caminho para modelo YOLOv8 treinado
                             Se None, usa modelo padrão
        """
        # Usar modelo treinado por padrão
        if modelo_yolo_path is None:
            workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            modelo_yolo_path = os.path.join(
                workspace_root, 
                'notebooks', 
                'runs', 
                'detect', 
                'solar_panel_detection',
                'yolov8_solar3',
                'weights',
                'best.pt'
            )
        
        self.modelo_yolo_path = modelo_yolo_path
        self.modelo_yolo = None
        self.classifier = PropertyClassifier()
        self.estimator = PowerEstimator()
        
        # Carregar modelo YOLO
        if YOLO is not None:
            try:
                self.modelo_yolo = YOLO(self.modelo_yolo_path)
                logger.info(f"✅ Modelo YOLO painéis solares carregado: {self.modelo_yolo_path}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao carregar YOLO: {e}")
        else:
            logger.error("❌ ultralytics não está instalado!")
    
    def baixar_roi_do_telhado(self, url_imagem: str, bbox: Dict[str, float], 
                              timeout: int = 30, sem_autenticacao: bool = True) -> Optional[np.ndarray]:
        """
        Baixa imagem do telhado e corta a ROI
        
        Args:
            url_imagem: URL da imagem Google Maps
            bbox: Bounding box do telhado {"x": int, "y": int, "w": int, "h": int}
            timeout: Timeout em segundos
            sem_autenticacao: Se True, baixa sem autenticação
            
        Returns:
            Imagem da ROI como numpy array (BGR) ou None se erro
        """
        try:
            logger.info(f"📥 Baixando imagem da URL...")
            
            if sem_autenticacao:
                response = requests.get(url_imagem, timeout=timeout)
            else:
                response = requests.get(url_imagem, timeout=timeout)
            
            response.raise_for_status()
            
            # Carregar imagem
            imagem = Image.open(BytesIO(response.content))
            if imagem.mode != 'RGB':
                imagem = imagem.convert('RGB')
            
            imagem_array = np.array(imagem)
            imagem_array = cv2.cvtColor(imagem_array, cv2.COLOR_RGB2BGR)
            
            logger.info(f"✓ Imagem baixada: {imagem_array.shape}")
            
            # Cortar ROI do telhado
            x = int(bbox.get('x', 0))
            y = int(bbox.get('y', 0))
            w = int(bbox.get('w', imagem_array.shape[1]))
            h = int(bbox.get('h', imagem_array.shape[0]))
            
            # Validar coordenadas
            x = max(0, min(x, imagem_array.shape[1]))
            y = max(0, min(y, imagem_array.shape[0]))
            w = min(w, imagem_array.shape[1] - x)
            h = min(h, imagem_array.shape[0] - y)
            
            roi = imagem_array[y:y+h, x:x+w]
            
            logger.info(f"✓ ROI extraída: {roi.shape}")
            return roi
            
        except Exception as e:
            logger.error(f"❌ Erro ao baixar ROI: {e}")
            return None
    
    def detectar_paineis(self, imagem_roi: np.ndarray, 
                        confianca_minima: float = 0.5) -> List[PainelSolarDetectado]:
        """
        Detecta painéis solares na ROI usando YOLOv8
        
        Args:
            imagem_roi: Imagem da ROI (BGR)
            confianca_minima: Confiança mínima para detecção
            
        Returns:
            Lista de painéis detectados
        """
        if self.modelo_yolo is None:
            logger.error("❌ Modelo YOLO não foi carregado!")
            return []
        
        try:
            logger.info(f"🔍 Detectando painéis solares...")
            
            # Executar detecção
            resultados = self.modelo_yolo.predict(
                imagem_roi,
                conf=confianca_minima,
                verbose=False
            )
            
            paineis = []
            
            for i, resultado in enumerate(resultados):
                if resultado.boxes is None:
                    logger.info(f"✓ Nenhum painel detectado na ROI")
                    continue
                
                for j, box in enumerate(resultado.boxes):
                    try:
                        # Extrair coordenadas
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        conf = float(box.conf[0].cpu().numpy())
                        
                        # Calcular bbox (x, y, w, h)
                        x, y = int(x1), int(y1)
                        w, h = int(x2 - x1), int(y2 - y1)
                        
                        # Centroide
                        cx, cy = x + w // 2, y + h // 2
                        
                        # Área em pixels
                        area_px = w * h
                        
                        # Área em m² (estimada com 30cm/pixel para Google Maps zoom 20)
                        pixel_para_m = 0.3  # Google Maps zoom 20: ~30cm/pixel
                        area_m2 = (w * pixel_para_m) * (h * pixel_para_m)
                        
                        painel = PainelSolarDetectado(
                            id_painel=f"painel_{j+1}",
                            bbox={"x": x, "y": y, "w": w, "h": h},
                            centroide={"x": cx, "y": cy},
                            area_pixeis=area_px,
                            area_m2=area_m2,
                            confianca=conf,
                            tipo_painel="desconhecido"  # TODO: Classificar tipo
                        )
                        
                        paineis.append(painel)
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao processar detecção {j}: {e}")
            
            logger.info(f"✓ {len(paineis)} painéis detectados")
            return paineis
            
        except Exception as e:
            logger.error(f"❌ Erro ao detectar painéis: {e}")
            return []
    
    def estimar_potencia(self, paineis: List[PainelSolarDetectado],
                        potencia_por_m2: float = 150.0) -> EstimativaPotencia:
        """
        Estima potência instalada com base nos painéis detectados
        
        Args:
            paineis: Lista de painéis detectados
            potencia_por_m2: Potência por m² (padrão 150W/m²)
            
        Returns:
            Estimativa de potência e produção
        """
        if not paineis:
            logger.warning("⚠️ Nenhum painel para estimar potência")
            return EstimativaPotencia(
                total_area_m2=0,
                num_paineis=0,
                potencia_instalada_kw=0
            )
        
        # Calcular totais
        total_area_m2 = sum(p.area_m2 for p in paineis)
        num_paineis = len(paineis)
        potencia_w = total_area_m2 * potencia_por_m2
        potencia_kw = potencia_w / 1000
        
        estimativa = EstimativaPotencia(
            total_area_m2=total_area_m2,
            num_paineis=num_paineis,
            potencia_instalada_kw=potencia_kw,
            potencia_por_m2=potencia_por_m2
        )
        
        # Calcular produção anual
        estimativa.calcular()
        
        logger.info(f"✅ Estimativa de potência: {potencia_kw:.2f} kW")
        logger.info(f"   • Área: {total_area_m2:.2f} m²")
        logger.info(f"   • Painéis: {num_paineis}")
        logger.info(f"   • Produção anual: {estimativa.producao_anual_kwh:.0f} kWh")
        
        return estimativa
    
    def processar_telhado(self, url_imagem: str, bbox: Dict[str, float],
                         confianca_minima: float = 0.5,
                         potencia_por_m2: float = 150.0) -> Dict:
        """
        Pipeline completo de processamento de um telhado
        
        Args:
            url_imagem: URL da imagem Google Maps
            bbox: Bounding box do telhado
            confianca_minima: Confiança mínima para detecção
            potencia_por_m2: Potência por m²
            
        Returns:
            Dicionário com resultados completos
        """
        import time
        tempo_inicio = time.time()
        
        resultado = {
            'sucesso': False,
            'paineis': [],
            'potencia': None,
            'erros': [],
            'tempo_processamento_s': 0
        }
        
        try:
            # Etapa 1: Baixar ROI (sem autenticação para não gastar tokens)
            logger.info("📥 Etapa 1/3: Baixando ROI do telhado...")
            roi = self.baixar_roi_do_telhado(url_imagem, bbox, sem_autenticacao=True)
            
            if roi is None:
                resultado['erros'].append("Falha ao baixar imagem")
                return resultado
            
            # Etapa 2: Detectar painéis
            logger.info("🔍 Etapa 2/3: Detectando painéis solares...")
            paineis = self.detectar_paineis(roi, confianca_minima)
            
            resultado['paineis'] = [p.to_dict() for p in paineis]
            
            # Etapa 3: Estimar potência
            logger.info("⚡ Etapa 3/3: Estimando potência...")
            potencia = self.estimar_potencia(paineis, potencia_por_m2)
            
            resultado['potencia'] = potencia.to_dict()
            resultado['sucesso'] = True
            
        except Exception as e:
            erro_msg = f"Erro ao processar telhado: {str(e)}"
            logger.error(f"❌ {erro_msg}")
            resultado['erros'].append(erro_msg)
        
        finally:
            resultado['tempo_processamento_s'] = time.time() - tempo_inicio
        
        return resultado
    
    def classificar_e_estimar(self, detections: List[Dict],
                             resolution_m_per_pixel: float = 0.3) -> Dict:
        """
        Função completa de classificação e estimativa.
        
        Args:
            detections: Lista de detecções com área_pixels e confidence
            resolution_m_per_pixel: Resolução da imagem
        
        Returns:
            Dict completo com classificação, potência e produção
        """
        # Estimar potência
        power_estimate = self.estimator.estimate_power(detections)
        power_kw = power_estimate['total_power_kw']
        
        # Classificar propriedade
        property_type, confidence, features = self.classifier.classify(
            detections, 
            estimated_power=power_kw
        )
        
        # Estimar produção anual
        production = self.estimator.estimate_annual_production(power_kw)
        
        # Resultado completo
        return {
            'classificacao': {
                'tipo': property_type,
                'confianca': confidence,
                'descricao': self.classifier.get_description(property_type),
                'faixa_potencia_kw': self.classifier.get_power_range(property_type)
            },
            'potencia': power_estimate,
            'producao_anual': production,
            'deteccoes': {
                'num_paineis': len(detections),
                'confianca_media': features.get('avg_confidence', 0),
                'area_total_m2': power_estimate['total_area_m2']
            }
        }


# ===========================
# BACKWARD COMPATIBILITY ALIASES
# ===========================

# Aliases para compatibilidade com código antigo
SolarPanelClassifier = SolarPanelService
PainelSolarDetectionService = SolarPanelService


if __name__ == "__main__":
    # Teste rápido
    print("🧪 Teste do Serviço de Painéis Solares\n")
    
    # Simular detecções
    deteccoes_teste = [
        {
            'nome': 'Residencial (3 painéis)',
            'detections': [
                {'area_pixels': 1500, 'confidence': 0.85},
                {'area_pixels': 1600, 'confidence': 0.82},
                {'area_pixels': 1450, 'confidence': 0.88}
            ]
        },
        {
            'nome': 'Comercial (12 painéis)',
            'detections': [
                {'area_pixels': 2500, 'confidence': 0.90} for _ in range(12)
            ]
        },
        {
            'nome': 'Industrial (50 painéis)',
            'detections': [
                {'area_pixels': 4000, 'confidence': 0.92} for _ in range(50)
            ]
        }
    ]
    
    service = SolarPanelService()
    
    for caso in deteccoes_teste:
        print(f"📍 {caso['nome']}")
        resultado = service.classificar_e_estimar(caso['detections'])
        
        print(f"   Tipo: {resultado['classificacao']['tipo'].upper()}")
        print(f"   Confiança: {resultado['classificacao']['confianca']:.0%}")
        print(f"   Potência: {resultado['potencia']['total_power_kw']:.2f} kW")
        print(f"   Produção anual: {resultado['producao_anual']['annual_production_kwh']:,.0f} kWh")
        print(f"   Economia anual: R$ {resultado['producao_anual']['annual_savings_brl']:,.2f}")
        print(f"   Payback: {resultado['producao_anual']['estimated_payback_years']:.1f} anos")
        print()
