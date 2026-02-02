"""
Serviço de Detecção de Painéis Solares

Pipeline especializado para:
1. Baixar ROI do telhado (usando URL + bbox)
2. Detectar painéis solares usando YOLOv8
3. Estimar potência instalada
4. Calcular produção anual

Author: Energy Netload Monitor
Date: 2026-02-01
"""

import os
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
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

logger = logging.getLogger(__name__)


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


class PainelSolarDetectionService:
    """Serviço de detecção de painéis solares"""
    
    def __init__(self, modelo_yolo_path: str = None):
        """
        Inicializa o serviço
        
        Args:
            modelo_yolo_path: Caminho para modelo YOLOv8 treinado
                             Se None, usa modelo padrão: notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt
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
