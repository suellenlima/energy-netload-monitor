"""
Serviço de Detecção e Segmentação de Telhados (ML-only)

Componente de infraestrutura responsável apenas por:
1. Detecção de edifícios/telhados usando YOLOv8
2. Segmentação de telhados individuais com OpenCV
3. Extração de ROIs para processamento posterior

Este serviço é agnóstico a banco de dados e lógica de negócio.
Deve ser usado pela camada de aplicação (Application Layer - DDD).

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import os
import logging
import numpy as np
import cv2
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
from io import BytesIO

from PIL import Image

try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ===========================
# DATACLASSES
# ===========================

@dataclass
class TelhadoDetectado:
    """Informações de um telhado/edifício detectado"""
    
    id_telhado: str
    id_subestacao: str
    id_imagem_satelite: str
    bbox: Dict[str, float]  # {x, y, w, h} em pixels
    bbox_normalizado: Dict[str, float]  # {x, y, w, h} normalizados (0-1)
    centroide: Dict[str, float]  # {x, y} em pixels
    lat: float
    lon: float
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_edificio: str
    mascara_segmentacao: Optional[np.ndarray] = None
    contorno: Optional[List[Tuple[int, int]]] = None
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    modelo_deteccao: str = "yolov8n-seg"
    propriedades_adicionais: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Converte para dicionário JSON-serializable"""
        data = asdict(self)
        data['timestamp_deteccao'] = self.timestamp_deteccao.isoformat()
        data['mascara_segmentacao'] = None
        data['contorno'] = None
        return data


@dataclass
class TelhadoSegmentado:
    """Telhado após segmentação e extração de ROI"""
    
    id_telhado: str
    imagem_roi: np.ndarray
    mascara: np.ndarray
    bbox_original: Dict[str, float]
    tamanho_pixeis: Tuple[int, int]
    resolucao_m_por_pixel: float
    percentual_cobertura: float
    indice_qualidade: float
    timestamp: datetime = field(default_factory=datetime.now)
    caminho_arquivo: Optional[str] = None


# ===========================
# SERVIÇO DE DETECÇÃO (ML-only)
# ===========================

class RoofDetectionService:
    """
    Serviço de detecção e segmentação de telhados (apenas ML).
    
    Responsável APENAS por:
    - Carregar modelo YOLO
    - Detectar telhados em imagens
    - Segmentar telhados
    - Extrair ROIs
    
    NÃO faz: operações CRUD, lógica de negócio, persistência
    """

    def __init__(self, model_path: str = None, use_gpu: bool = True):
        """
        Inicializa o serviço de detecção.
        
        Args:
            model_path: Caminho para modelo YOLO pré-treinado
            use_gpu: Se True, usa GPU se disponível
        """
        self.model_path = model_path or "yolov8n-seg.pt"
        self.use_gpu = use_gpu and self._check_gpu_available()
        self.device = "0" if self.use_gpu else "cpu"
        self.model = None
        
        # Carregar modelo
        self._carregar_modelo()
        logger.info(f"RoofDetectionService inicializado. Device: {self.device}")

    def _check_gpu_available(self) -> bool:
        """Verifica disponibilidade de GPU."""
        try:
            return cv2.cuda.getCudaEnabledDeviceCount() > 0
        except:
            return False

    def _carregar_modelo(self):
        """Carrega modelo YOLO para detecção."""
        if YOLO is None:
            logger.warning("YOLOv8 não está instalado. Detecção desabilitada.")
            return

        try:
            self.model = YOLO(self.model_path)
            logger.info(f"✓ Modelo YOLO carregado: {self.model_path}")
        except Exception as e:
            logger.error(f"✗ Erro ao carregar modelo YOLO: {e}")
            self.model = None

    def detectar_telhados(
        self,
        imagem: np.ndarray,
        confianca_minima: float = 0.5,
        iou_threshold: float = 0.45
    ) -> List[Dict]:
        """
        Detecta telhados em imagem usando YOLOv8.
        
        Args:
            imagem: Array numpy com imagem RGB
            confianca_minima: Threshold de confiança mínima
            iou_threshold: Threshold de IoU para NMS
            
        Returns:
            Lista de dicionários com dados dos telhados detectados
        """
        if self.model is None:
            logger.warning("Modelo não carregado. Retornando lista vazia.")
            return []

        try:
            # Executar detecção
            resultados = self.model.predict(
                imagem,
                conf=confianca_minima,
                iou=iou_threshold,
                device=self.device,
                verbose=False
            )

            telhados = []
            for resultado in resultados:
                if resultado.boxes is not None:
                    for box in resultado.boxes:
                        # Extrair informações do bounding box
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        confianca = box.conf[0].item()

                        telhado = {
                            'bbox': {
                                'x': float(x1),
                                'y': float(y1),
                                'x2': float(x2),
                                'y2': float(y2),
                                'w': float(x2 - x1),
                                'h': float(y2 - y1)
                            },
                            'centroide': {
                                'x': float((x1 + x2) / 2),
                                'y': float((y1 + y2) / 2)
                            },
                            'confianca': float(confianca),
                            'area_pixeis': int((x2 - x1) * (y2 - y1))
                        }
                        telhados.append(telhado)

            logger.info(f"✓ Detectados {len(telhados)} telhados com confiança ≥ {confianca_minima}")
            return telhados

        except Exception as e:
            logger.error(f"✗ Erro durante detecção: {e}")
            return []

    def segmentar_telhados(
        self,
        imagem: np.ndarray,
        deteccoes: List[Dict]
    ) -> List[TelhadoSegmentado]:
        """
        Segmenta telhados individuais usando OpenCV.
        
        Args:
            imagem: Array numpy com imagem RGB
            deteccoes: Lista de detecções (bboxes)
            
        Returns:
            Lista de TelhadoSegmentado
        """
        telhados_segmentados = []

        for i, det in enumerate(deteccoes):
            bbox = det['bbox']
            x1, y1 = int(bbox['x']), int(bbox['y'])
            x2, y2 = int(bbox['x2']), int(bbox['y2'])

            # Extrair ROI
            roi = imagem[y1:y2, x1:x2]

            if roi.size == 0:
                continue

            # Criar máscara (placeholder - seria mais complexo com contornos reais)
            mascara = np.ones((roi.shape[0], roi.shape[1]), dtype=np.uint8) * 255

            telhado_seg = TelhadoSegmentado(
                id_telhado=f"telhado_{i}",
                imagem_roi=roi,
                mascara=mascara,
                bbox_original=bbox,
                tamanho_pixeis=(roi.shape[1], roi.shape[0]),
                resolucao_m_por_pixel=1.0,
                percentual_cobertura=100.0,
                indice_qualidade=det.get('confianca', 0.5)
            )
            telhados_segmentados.append(telhado_seg)

        logger.info(f"✓ Segmentados {len(telhados_segmentados)} telhados")
        return telhados_segmentados

    def extrair_rois_telhados(
        self,
        imagem: np.ndarray,
        deteccoes: List[Dict],
        diretorio_saida: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extrai ROIs dos telhados detectados.
        
        Args:
            imagem: Array numpy com imagem RGB
            deteccoes: Lista de detecções
            diretorio_saida: Diretório para salvar ROIs
            
        Returns:
            Dicionário com estatísticas de extração
        """
        rois_extratos = []
        
        for i, det in enumerate(deteccoes):
            bbox = det['bbox']
            x1, y1 = int(bbox['x']), int(bbox['y'])
            x2, y2 = int(bbox['x2']), int(bbox['y2'])

            roi = imagem[y1:y2, x1:x2]

            if diretorio_saida:
                Path(diretorio_saida).mkdir(parents=True, exist_ok=True)
                caminho = Path(diretorio_saida) / f"roi_{i}.png"
                cv2.imwrite(str(caminho), cv2.cvtColor(roi, cv2.COLOR_RGB2BGR))
                rois_extratos.append(str(caminho))

        return {
            'total_rois': len(rois_extratos),
            'rois': rois_extratos,
            'timestamp': datetime.now().isoformat()
        }

    @staticmethod
    def _carregar_imagem(imagem_path: str) -> Optional[np.ndarray]:
        """Carrega imagem do caminho."""
        try:
            img = cv2.imread(imagem_path)
            if img is not None:
                return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar imagem {imagem_path}: {e}")
            return None
