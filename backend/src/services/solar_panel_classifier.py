"""
Classificador de Propriedades por Tipo de Instalação Solar

Extraído do notebook 09_yolo_solar_panel_detection_classification.ipynb
Classifica instalações solares em: Residencial, Comercial ou Industrial

Author: Energy Netload Monitor
Date: 2026-01-30
"""

import numpy as np
from typing import List, Dict, Tuple, Optional


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


# Funções auxiliares para integração

def classificar_e_estimar(
    detections: List[Dict],
    resolution_m_per_pixel: float = 0.3
) -> Dict:
    """
    Função completa de classificação e estimativa.
    
    Args:
        detections: Lista de detecções com área_pixels e confidence
        resolution_m_per_pixel: Resolução da imagem
    
    Returns:
        Dict completo com classificação, potência e produção
    """
    # Inicializar serviços
    classifier = PropertyClassifier()
    estimator = PowerEstimator(resolution_m_per_pixel)
    
    # Estimar potência
    power_estimate = estimator.estimate_power(detections)
    power_kw = power_estimate['total_power_kw']
    
    # Classificar propriedade
    property_type, confidence, features = classifier.classify(
        detections, 
        estimated_power=power_kw
    )
    
    # Estimar produção anual
    production = estimator.estimate_annual_production(power_kw)
    
    # Resultado completo
    return {
        'classificacao': {
            'tipo': property_type,
            'confianca': confidence,
            'descricao': classifier.get_description(property_type),
            'faixa_potencia_kw': classifier.get_power_range(property_type)
        },
        'potencia': power_estimate,
        'producao_anual': production,
        'deteccoes': {
            'num_paineis': len(detections),
            'confianca_media': features.get('avg_confidence', 0),
            'area_total_m2': power_estimate['total_area_m2']
        }
    }


if __name__ == "__main__":
    # Teste rápido
    print("🧪 Teste do Classificador de Propriedades\n")
    
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
    
    for caso in deteccoes_teste:
        print(f"📍 {caso['nome']}")
        resultado = classificar_e_estimar(caso['detections'])
        
        print(f"   Tipo: {resultado['classificacao']['tipo'].upper()}")
        print(f"   Confiança: {resultado['classificacao']['confianca']:.0%}")
        print(f"   Potência: {resultado['potencia']['total_power_kw']:.2f} kW")
        print(f"   Produção anual: {resultado['producao_anual']['annual_production_kwh']:,.0f} kWh")
        print(f"   Economia anual: R$ {resultado['producao_anual']['annual_savings_brl']:,.2f}")
        print(f"   Payback: {resultado['producao_anual']['estimated_payback_years']:.1f} anos")
        print()
