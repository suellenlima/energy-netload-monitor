"""
Serviço Unificado de Detecção e Segmentação de Telhados

Pipeline completo para processamento de telhados em imagens de satélite:
1. Download de imagens (CBERS-4A 2m / Google Maps 0.3m)
2. Detecção de edifícios/telhados usando YOLOv8
3. Segmentação de telhados individuais com OpenCV
4. Extração de ROIs para processamento posterior
5. Processamento por transformador com histórico

Unifica funcionalidades de:
- telhado_segmentation_service.py (detecção, segmentação, ROI)
- telhado_service.py (lógica de negócio e agregação)
- telhado_transformador_service.py (detecção por transformador)

Author: Energy Netload Monitor
Date: 2025-01-30 + 2026-02-04 (unified)
"""

import os
import json
import logging
import requests
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
from io import BytesIO

import numpy as np
import cv2
from PIL import Image
from sqlalchemy import text
from sqlalchemy.engine import Engine

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


@dataclass
class TelhadoTransformador:
    """Telhado detectado em contexto de transformador"""
    
    id_telhado: str
    id_transformador: int
    id_subestacao: int
    id_imagem_fonte: str
    bbox: Dict[str, float]
    centroide: Dict[str, float]
    latitude: float
    longitude: float
    area_pixeis: int
    area_m2: float
    confianca: float
    tipo_edificio: str
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    fonte_imagem: str = "google_maps"
    resolucao_cm: float = 30.0
    
    def to_dict(self) -> Dict:
        """Converte para dicionário"""
        data = asdict(self)
        data['timestamp_deteccao'] = self.timestamp_deteccao.isoformat()
        return data


@dataclass
class ResultadoProcessamentoTelhados:
    """Resultado do processamento de telhados em uma subestação"""
    
    id_subestacao: str
    id_imagem_satelite: str
    timestamp_processamento: datetime = field(default_factory=datetime.now)
    telhados_detectados: int = 0
    total_telhados_segmentados: int = 0
    telhados_com_erro: int = 0
    tempo_processamento_segundos: float = 0.0
    telhados: List[TelhadoDetectado] = field(default_factory=list)
    telhados_segmentados: List[TelhadoSegmentado] = field(default_factory=list)
    erros: List[str] = field(default_factory=list)
    avisos: List[str] = field(default_factory=list)


@dataclass
class ResultadoDeteccaoTransformador:
    """Resultado da detecção de telhados para um transformador"""
    
    transformador_id: int
    subestacao_id: int
    sucesso: bool
    total_telhados: int
    telhados: List[TelhadoTransformador]
    area_total_m2: float
    confianca_media: float
    motivo: str
    tempo_processamento_ms: float
    fonte_imagem: str
    timestamp: datetime = field(default_factory=datetime.now)


# ===========================
# MAIN SERVICE
# ===========================

class RoofService:
    """
    Serviço unificado para segmentação, detecção e análise de telhados
    
    Funcionalidades:
    - Detecção de telhados com YOLOv8
    - Segmentação com OpenCV
    - Extração de ROIs
    - Processamento por transformador
    - Lógica de negócio e agregação
    """
    
    def __init__(self, engine: Engine = None, model_path: str = None, 
                 use_gpu: bool = True, use_cache: bool = True):
        """
        Inicializa o serviço de telhados
        
        Args:
            engine: SQLAlchemy engine (opcional, para modo com BD)
            model_path: Caminho para modelo YOLOv8 customizado
            use_gpu: Usar GPU se disponível
            use_cache: Usar cache para imagens CBERS
        """
        self.engine = engine
        self.use_cache = use_cache
        self.use_gpu = use_gpu and self._check_gpu_available()
        self.device = "0" if self.use_gpu else "cpu"
        
        # Usar modelo treinado por padrão
        if model_path is None:
            model_path = "notebooks/roof_dataset_yolo/trained_models/best.pt"
        
        self.model_path = model_path
        self.modelo_deteccao = None
        self._carregar_modelo_deteccao()
        
        # Repository para BD
        if engine:
            try:
                from ..repositories.telhado_repository import TelhadoRepository
                self.repository = TelhadoRepository(engine)
            except ImportError:
                self.repository = None
        else:
            self.repository = None
        
        logger.info(f"RoofService inicializado. Device: {self.device}")
    
    def _check_gpu_available(self) -> bool:
        """Verifica se CUDA está disponível"""
        try:
            import torch
            return torch.cuda.is_available()
        except ImportError:
            return False
    
    def _carregar_modelo_deteccao(self):
        """Carrega modelo YOLO treinado para detecção de telhados"""
        if YOLO is None:
            logger.error("❌ ultralytics não está instalado. Instale com: pip install ultralytics")
            return
        
        try:
            import os
            from pathlib import Path
            
            logger.info(f"📁 Tentando carregar modelo YOLO de: {self.model_path}")
            
            if not os.path.isabs(self.model_path):
                base_path = Path(__file__).parent.parent.parent.parent
                model_full_path = base_path / self.model_path
            else:
                model_full_path = Path(self.model_path)
            
            if not model_full_path.exists():
                logger.warning(f"⚠️ Modelo treinado não encontrado: {model_full_path}")
                logger.info("🔍 Tentando usar modelo genérico yolov8n-seg.pt como fallback")
                
                fallback1 = Path(__file__).parent.parent.parent.parent / "yolov8n-seg.pt"
                fallback2 = Path(__file__).parent.parent.parent / "yolov8n-seg.pt"
                
                if fallback1.exists():
                    model_full_path = fallback1
                    logger.info(f"✓ Fallback encontrado: {fallback1}")
                elif fallback2.exists():
                    model_full_path = fallback2
                    logger.info(f"✓ Fallback encontrado: {fallback2}")
                else:
                    logger.error(f"❌ Nenhum modelo encontrado!")
                    return
            
            logger.info(f"⏳ Carregando modelo YOLO...")
            self.modelo_deteccao = YOLO(str(model_full_path))
            self.modelo_deteccao.to(self.device)
            logger.info(f"✅ Modelo YOLO carregado com sucesso")
        
        except Exception as e:
            logger.error(f"❌ Erro ao carregar modelo YOLO: {e}")
            self.modelo_deteccao = None
    
    # ========================================================================
    # PASSO 1: DOWNLOAD DE IMAGEM
    # ========================================================================
    
    def download_imagem_satelite(self, url_imagem: str, 
                                 timeout: int = 30,
                                 sem_autenticacao: bool = False) -> Optional[np.ndarray]:
        """Baixa imagem de satélite de URL ou carrega arquivo local"""
        try:
            if url_imagem.startswith('./') or url_imagem.startswith('/') or ':\\' in url_imagem:
                logger.info(f"Carregando imagem local: {url_imagem}")
                imagem = Image.open(url_imagem)
                if imagem.mode != 'RGB':
                    imagem = imagem.convert('RGB')
                
                from PIL import ImageEnhance
                enhancer_color = ImageEnhance.Color(imagem)
                imagem = enhancer_color.enhance(1.5)
            else:
                from PIL import Image as PILImage
                PILImage.MAX_IMAGE_PIXELS = None
                
                if sem_autenticacao:
                    logger.info(f"Baixando imagem SEM autenticação: {url_imagem[:80]}...")
                    response = requests.get(url_imagem, timeout=timeout)
                    response.raise_for_status()
                else:
                    logger.info(f"Baixando imagem com autenticação: {url_imagem[:80]}...")
                    response = requests.get(url_imagem, timeout=timeout)
                    response.raise_for_status()
                
                imagem = PILImage.open(BytesIO(response.content))
            
            if imagem.mode != 'RGB':
                imagem = imagem.convert('RGB')
            
            from PIL import ImageEnhance
            enhancer_color = ImageEnhance.Color(imagem)
            imagem = enhancer_color.enhance(1.5)
            logger.info("Saturação da imagem aumentada em 50% para melhor detecção")
            
            imagem_array = np.array(imagem)
            
            if len(imagem_array.shape) == 3 and imagem_array.shape[2] >= 3:
                imagem_array = cv2.cvtColor(imagem_array, cv2.COLOR_RGB2BGR)
            
            logger.info(f"Imagem carregada: {imagem_array.shape}")
            return imagem_array
            
        except Exception as e:
            logger.error(f"Erro ao baixar imagem: {e}")
            return None
    
    # ========================================================================
    # PASSO 2: DETECÇÃO COM YOLO
    # ========================================================================
    
    def detectar_telhados(self, imagem: np.ndarray, 
                         confianca_minima: float = 0.5,
                         iou_threshold: float = 0.5) -> List[TelhadoDetectado]:
        """Detecta telhados em imagem usando YOLOv8 treinado"""
        if self.modelo_deteccao is None:
            logger.error("❌ MODELO YOLO NÃO ESTÁ CARREGADO!")
            return []
        
        try:
            resultados = self.modelo_deteccao(imagem, 
                                             conf=confianca_minima,
                                             iou=iou_threshold,
                                             device=self.device)
            
            telhados_detectados = []
            
            for i, resultado in enumerate(resultados):
                if resultado.boxes is not None:
                    boxes = resultado.boxes
                    
                    for j, box in enumerate(boxes):
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        confianca = box.conf[0].cpu().item()
                        
                        if confianca < confianca_minima:
                            continue
                        
                        x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
                        w = x2 - x1
                        h = y2 - y1
                        cx = x1 + w / 2
                        cy = y1 + h / 2
                        area_pixeis = w * h
                        
                        img_h, img_w = imagem.shape[:2]
                        bbox_norm = {
                            "x": (x1 / img_w),
                            "y": (y1 / img_h),
                            "w": (w / img_w),
                            "h": (h / img_h)
                        }
                        
                        telhado = TelhadoDetectado(
                            id_telhado=f"telhado_{i}_{j}",
                            id_subestacao="desconhecido",
                            id_imagem_satelite="desconhecido",
                            bbox={"x": x1, "y": y1, "w": w, "h": h},
                            bbox_normalizado=bbox_norm,
                            centroide={"x": cx, "y": cy},
                            lat=0.0,
                            lon=0.0,
                            area_pixeis=area_pixeis,
                            area_m2=0.0,
                            confianca=confianca,
                            tipo_edificio="desconhecido",
                            modelo_deteccao="yolov8n-seg"
                        )
                        
                        telhados_detectados.append(telhado)
            
            logger.info(f"Detectados {len(telhados_detectados)} telhados")
            return telhados_detectados
            
        except Exception as e:
            logger.error(f"Erro na detecção de telhados: {e}")
            return []
    
    # ========================================================================
    # PASSO 3: SEGMENTAÇÃO
    # ========================================================================
    
    def segmentar_telhados(self, imagem: np.ndarray,
                          telhados: List[TelhadoDetectado]) -> List[TelhadoDetectado]:
        """Segmenta telhados utilizando OpenCV"""
        gray = cv2.cvtColor(imagem, cv2.COLOR_BGR2GRAY)
        gray = cv2.equalizeHist(gray)
        gray = cv2.bilateralFilter(gray, 9, 75, 75)
        edges = cv2.Canny(gray, 50, 150)
        
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
        edges = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel, iterations=2)
        edges = cv2.morphologyEx(edges, cv2.MORPH_OPEN, kernel, iterations=1)
        
        telhados_segmentados = []
        
        for telhado in telhados:
            try:
                x = int(telhado.bbox["x"])
                y = int(telhado.bbox["y"])
                w = int(telhado.bbox["w"])
                h = int(telhado.bbox["h"])
                
                roi_edges = edges[y:y+h, x:x+w]
                contours, _ = cv2.findContours(roi_edges, 
                                               cv2.RETR_TREE, 
                                               cv2.CHAIN_APPROX_SIMPLE)
                
                if contours:
                    contorno_principal = max(contours, key=cv2.contourArea)
                    mascara = np.zeros((h, w), dtype=np.uint8)
                    cv2.drawContours(mascara, [contorno_principal], 0, 255, -1)
                    mascara = cv2.morphologyEx(mascara, cv2.MORPH_CLOSE, kernel, iterations=2)
                    
                    area_mascara = cv2.countNonZero(mascara)
                    area_total = w * h
                    cobertura_percentual = (area_mascara / area_total) * 100 if area_total > 0 else 0
                    
                    telhado.mascara_segmentacao = mascara
                    telhado.contorno = contorno_principal.tolist()
                    telhado.area_pixeis = area_mascara
                    
                    roi_cinza = gray[y:y+h, x:x+w]
                    indice_qualidade = min(1.0, np.std(roi_cinza) / 50.0)
                    telhado.propriedades_adicionais['indice_qualidade'] = indice_qualidade
                    telhado.propriedades_adicionais['percentual_cobertura'] = cobertura_percentual
                    
                    telhados_segmentados.append(telhado)
                
            except Exception as e:
                logger.warning(f"Erro ao segmentar telhado {telhado.id_telhado}: {e}")
        
        logger.info(f"Segmentados {len(telhados_segmentados)} de {len(telhados)} telhados")
        return telhados_segmentados
    
    # ========================================================================
    # PASSO 4: EXTRAÇÃO DE ROIs
    # ========================================================================
    
    def extrair_rois_telhados(self, imagem: np.ndarray,
                              telhados: List[TelhadoDetectado],
                              resolucao_m_por_pixel: float = 3.0,
                              padding_percentual: float = 0.1) -> List[TelhadoSegmentado]:
        """Extrai ROIs individuais de telhados"""
        telhados_segmentados = []
        
        for telhado in telhados:
            try:
                x = int(telhado.bbox["x"])
                y = int(telhado.bbox["y"])
                w = int(telhado.bbox["w"])
                h = int(telhado.bbox["h"])
                
                padding_x = int(w * padding_percentual)
                padding_y = int(h * padding_percentual)
                
                x_start = max(0, x - padding_x)
                y_start = max(0, y - padding_y)
                x_end = min(imagem.shape[1], x + w + padding_x)
                y_end = min(imagem.shape[0], y + h + padding_y)
                
                roi_imagem = imagem[y_start:y_end, x_start:x_end]
                
                roi_mascara = None
                if telhado.mascara_segmentacao is not None:
                    roi_mascara = telhado.mascara_segmentacao
                else:
                    roi_mascara = np.ones_like(roi_imagem[:, :, 0], dtype=np.uint8) * 255
                
                roi_cinza = cv2.cvtColor(roi_imagem, cv2.COLOR_BGR2GRAY)
                percentual_cobertura = np.sum(roi_mascara > 127) / roi_mascara.size * 100
                indice_qualidade = telhado.propriedades_adicionais.get('indice_qualidade', 0.5)
                
                telhado_seg = TelhadoSegmentado(
                    id_telhado=telhado.id_telhado,
                    imagem_roi=roi_imagem,
                    mascara=roi_mascara,
                    bbox_original=telhado.bbox,
                    tamanho_pixeis=roi_imagem.shape[:2],
                    resolucao_m_por_pixel=resolucao_m_por_pixel,
                    percentual_cobertura=percentual_cobertura,
                    indice_qualidade=indice_qualidade
                )
                
                telhados_segmentados.append(telhado_seg)
                
            except Exception as e:
                logger.error(f"Erro ao extrair ROI do telhado {telhado.id_telhado}: {e}")
        
        logger.info(f"Extraídas {len(telhados_segmentados)} ROIs")
        return telhados_segmentados
    
    # ========================================================================
    # PASSO 5: PIPELINE COMPLETO
    # ========================================================================
    
    def processar_telhados_lote(self, url_imagem: str,
                               id_subestacao: str,
                               id_imagem_satelite: str,
                               resolucao_m_por_pixel: float = 3.0,
                               confianca_minima: float = 0.5,
                               diretorio_saida: Optional[str] = None,
                               sem_autenticacao: bool = False) -> ResultadoProcessamentoTelhados:
        """Pipeline completo: download → detecção → segmentação → extração"""
        import time
        
        tempo_inicio = time.time()
        resultado = ResultadoProcessamentoTelhados(
            id_subestacao=id_subestacao,
            id_imagem_satelite=id_imagem_satelite
        )
        
        try:
            logger.info(f"[1/4] Baixando imagem de {url_imagem}...")
            imagem = self.download_imagem_satelite(url_imagem, sem_autenticacao=sem_autenticacao)
            if imagem is None:
                resultado.erros.append("Falha ao baixar imagem")
                return resultado
            
            logger.info("[2/4] Detectando telhados...")
            telhados_detectados = self.detectar_telhados(imagem, confianca_minima)
            
            if not telhados_detectados:
                resultado.avisos.append("Nenhum telhado detectado")
                return resultado
            
            for telhado in telhados_detectados:
                telhado.id_subestacao = id_subestacao
                telhado.id_imagem_satelite = id_imagem_satelite
            
            resultado.telhados = telhados_detectados
            resultado.telhados_detectados = len(telhados_detectados)
            
            logger.info("[3/4] Segmentando telhados...")
            telhados_seg = self.segmentar_telhados(imagem, telhados_detectados)
            resultado.total_telhados_segmentados = len(telhados_seg)
            
            logger.info("[4/4] Extraindo ROIs...")
            rois = self.extrair_rois_telhados(imagem, telhados_seg, resolucao_m_por_pixel)
            resultado.telhados_segmentados = rois
            resultado.total_telhados_segmentados = len(rois)
            
            if diretorio_saida:
                self._salvar_rois(rois, diretorio_saida, id_subestacao)
            
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            logger.info(f"✓ Pipeline concluído em {resultado.tempo_processamento_segundos:.2f}s")
            
            return resultado
            
        except Exception as e:
            logger.error(f"Erro crítico no pipeline: {e}")
            resultado.erros.append(f"Erro crítico: {str(e)}")
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            return resultado
    
    # ========================================================================
    # PROCESSAMENTO POR TRANSFORMADOR
    # ========================================================================
    
    def detectar_telhados_transformador(
        self,
        transformador_id: int,
        imagem_path: str,
        fonte_imagem: str = "google_maps"
    ) -> ResultadoDeteccaoTransformador:
        """Detecta telhados em área de um transformador"""
        tempo_inicio = datetime.now()
        
        try:
            trans_data = self._obter_transformador(transformador_id)
            if not trans_data:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    motivo=f"Transformador {transformador_id} não encontrado",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            subestacao_id = trans_data['subestacao_id']
            
            imagem = self._carregar_imagem(imagem_path)
            if imagem is None:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    motivo=f"Não foi possível carregar imagem: {imagem_path}",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            if self.modelo_deteccao is None:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    motivo="Modelo YOLO não disponível",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            deteccoes = self._detectar_com_yolo(imagem, trans_data)
            
            area_total = sum(t.area_m2 for t in deteccoes)
            confianca_media = (sum(t.confianca for t in deteccoes) / len(deteccoes)) if deteccoes else 0
            
            tempo_ms = (datetime.now() - tempo_inicio).total_seconds() * 1000
            
            resultado = ResultadoDeteccaoTransformador(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                sucesso=True,
                total_telhados=len(deteccoes),
                telhados=deteccoes,
                area_total_m2=area_total,
                confianca_media=confianca_media,
                motivo="Sucesso",
                tempo_processamento_ms=tempo_ms,
                fonte_imagem=fonte_imagem
            )
            
            logger.info(f"✅ {len(deteccoes)} telhados detectados em transformador {transformador_id}")
            return resultado
        
        except Exception as e:
            logger.error(f"❌ Erro na detecção: {e}", exc_info=True)
            return self._resultado_erro(
                transformador_id=transformador_id,
                motivo=str(e),
                tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
            )
    
    def detectar_telhados_subestacao(
        self,
        subestacao_id: int,
        imagens_por_transformador: Dict[int, str],
        fonte_imagem: str = "google_maps"
    ) -> List[ResultadoDeteccaoTransformador]:
        """Detecta telhados para todos os transformadores de uma subestação"""
        logger.info(f"🔍 Iniciando detecção para SE {subestacao_id}")
        
        resultados = []
        
        for trans_id, imagem_path in imagens_por_transformador.items():
            resultado = self.detectar_telhados_transformador(
                transformador_id=trans_id,
                imagem_path=imagem_path,
                fonte_imagem=fonte_imagem
            )
            resultados.append(resultado)
        
        total_sucesso = sum(1 for r in resultados if r.sucesso)
        total_telhados = sum(r.total_telhados for r in resultados)
        area_total = sum(r.area_total_m2 for r in resultados)
        
        logger.info(f"✅ Processamento concluído: {total_sucesso}/{len(resultados)} sucesso, "
                   f"{total_telhados} telhados, {area_total:.0f} m²")
        
        return resultados
    
    # ========================================================================
    # LÓGICA DE NEGÓCIO (do telhado_service.py)
    # ========================================================================
    
    def listar_telhados(
        self,
        id_subestacao: Optional[str] = None,
        tipo_edificio: Optional[str] = None,
        confianca_minima: float = 0.0,
        pagina: int = 1,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Lista telhados com filtros e paginação"""
        if not self.repository:
            return {'erro': 'Engine não configurado'}
        
        try:
            resultado = self.repository.listar_telhados_com_filtros(
                id_subestacao=id_subestacao,
                tipo_edificio=tipo_edificio,
                confianca_minima=confianca_minima,
                pagina=pagina,
                limite=limite
            )
            
            logger.info(f"Listados {len(resultado['telhados'])} telhados")
            return resultado
        
        except Exception as e:
            logger.error(f"Erro ao listar telhados: {e}")
            raise
    
    def obter_detalhes_subestacao(self, subestacao_id: int) -> Dict[str, Any]:
        """Obtém dados completos de uma subestação com seus telhados"""
        if not self.repository:
            return {'erro': 'Engine não configurado'}
        
        try:
            telhados = self.repository.obter_telhados_subestacao(subestacao_id)
            
            if not telhados:
                stats = {
                    'total_telhados': 0,
                    'area_total_m2': 0,
                    'confianca_media': 0,
                    'transformadores_unicos': 0
                }
            else:
                transformadores_set = set(t['transformador_id'] for t in telhados if t['transformador_id'])
                stats = {
                    'total_telhados': len(telhados),
                    'area_total_m2': sum(t['area_m2'] for t in telhados),
                    'confianca_media': sum(t['confianca'] for t in telhados) / len(telhados),
                    'transformadores_unicos': len(transformadores_set)
                }
            
            return {
                'subestacao_id': subestacao_id,
                'timestamp_processamento': datetime.now().isoformat(),
                'telhados_detectados': stats['total_telhados'],
                'area_total_m2': stats['area_total_m2'],
                'confianca_media': round(stats['confianca_media'], 3),
                'transformadores_processados': stats['transformadores_unicos'],
                'telhados': telhados,
                'sucesso': True
            }
        
        except Exception as e:
            logger.error(f"Erro ao obter detalhes subestação {subestacao_id}: {e}")
            raise
    
    def obter_telhados_transformador(
        self,
        transformador_id: int,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Obtém telhados de um transformador específico"""
        if not self.repository:
            return {'erro': 'Engine não configurado'}
        
        try:
            telhados = self.repository.obter_telhados_transformador(
                transformador_id=transformador_id,
                limite=limite
            )
            
            if not telhados:
                stats = {
                    'total': 0,
                    'area_total_m2': 0,
                    'confianca_media': 0
                }
            else:
                stats = {
                    'total': len(telhados),
                    'area_total_m2': sum(t['area_m2'] for t in telhados),
                    'confianca_media': round(
                        sum(t['confianca'] for t in telhados) / len(telhados), 3
                    )
                }
            
            return {
                'transformador_id': transformador_id,
                'total': stats['total'],
                'area_total_m2': stats['area_total_m2'],
                'confianca_media': stats['confianca_media'],
                'telhados': telhados,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Erro ao obter telhados do transformador {transformador_id}: {e}")
            raise
    
    def obter_estatisticas_gerais(self, periodo: Optional[str] = None) -> Dict[str, Any]:
        """Obtém estatísticas gerais de telhados"""
        if not self.repository:
            return {'erro': 'Engine não configurado'}
        
        try:
            # Tenta obter do repository se disponível
            if hasattr(self.repository, 'obter_estatisticas_telhados'):
                return self.repository.obter_estatisticas_telhados(periodo)
            
            # Fallback: calcula localmente
            todos_telhados = self.repository.listar_telhados_com_filtros()
            telhados = todos_telhados.get('telhados', [])
            
            if not telhados:
                return {
                    'total_telhados': 0,
                    'area_total_m2': 0,
                    'confianca_media': 0,
                    'confianca_minima': 0,
                    'confianca_maxima': 0,
                    'area_minima_m2': 0,
                    'area_maxima_m2': 0,
                    'transformadores_unicos': 0,
                    'subestacoes_unicas': 0
                }
            
            areas = [t['area_m2'] for t in telhados if t.get('area_m2')]
            confiancas = [t['confianca'] for t in telhados if t.get('confianca')]
            transformadores = set(t['transformador_id'] for t in telhados if t.get('transformador_id'))
            subestacoes = set(t['subestacao_id'] for t in telhados if t.get('subestacao_id'))
            
            return {
                'total_telhados': len(telhados),
                'area_total_m2': sum(areas) if areas else 0,
                'area_media_m2': sum(areas) / len(areas) if areas else 0,
                'confianca_media': sum(confiancas) / len(confiancas) if confiancas else 0,
                'confianca_minima': min(confiancas) if confiancas else 0,
                'confianca_maxima': max(confiancas) if confiancas else 0,
                'area_minima_m2': min(areas) if areas else 0,
                'area_maxima_m2': max(areas) if areas else 0,
                'transformadores_unicos': len(transformadores),
                'subestacoes_unicas': len(subestacoes),
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas gerais: {e}")
            raise
    
    def obter_telhado(self, telhado_id: int) -> Optional[Dict]:
        """Obtém detalhes de um telhado específico"""
        if not self.repository:
            return None
        
        try:
            return self.repository.obter_telhado_por_id(telhado_id)
        except Exception as e:
            logger.error(f"Erro ao obter telhado {telhado_id}: {e}")
            raise
    
    def salvar_telhado(self, dados: Dict) -> int:
        """Salva um novo telhado"""
        if not self.repository:
            raise ValueError('Engine não configurado')
        
        try:
            obrigatorios = ['transformador_id', 'subestacao_id', 'latitude', 'longitude', 'area_m2', 'confianca']
            for campo in obrigatorios:
                if campo not in dados:
                    raise ValueError(f"Campo obrigatório faltando: {campo}")
            
            return self.repository.salvar_telhado(dados)
        except Exception as e:
            logger.error(f"Erro ao salvar telhado: {e}")
            raise
    
    def deletar_telhado(self, telhado_id: int) -> bool:
        """Deleta um telhado"""
        if not self.repository:
            return False
        
        try:
            return self.repository.deletar_telhado(telhado_id)
        except Exception as e:
            logger.error(f"Erro ao deletar telhado {telhado_id}: {e}")
            raise
    
    # ========================================================================
    # AUXILIARES PRIVADOS
    # ========================================================================
    
    def _obter_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca dados do transformador no banco"""
        if not self.engine:
            return None
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, nome, latitude, longitude, 
                        id as subestacao_id, potencia_kva, codigo
                    FROM transformadores_aneel
                    WHERE id = :id
                """), {'id': transformador_id})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'nome': row[1],
                    'latitude': float(row[2]),
                    'longitude': float(row[3]),
                    'subestacao_id': row[4],
                    'potencia_kva': float(row[5]),
                    'codigo': row[6]
                }
        except Exception as e:
            logger.error(f"Erro ao obter transformador: {e}")
            return None
    
    def _carregar_imagem(self, imagem_path: str) -> Optional[np.ndarray]:
        """Carrega imagem de arquivo local ou URL"""
        try:
            if imagem_path.startswith('http'):
                response = requests.get(imagem_path)
                imagem = Image.open(BytesIO(response.content))
            else:
                imagem = Image.open(imagem_path)
            
            return cv2.cvtColor(np.array(imagem), cv2.COLOR_RGB2BGR)
        
        except Exception as e:
            logger.error(f"Erro ao carregar imagem: {e}")
            return None
    
    def _detectar_com_yolo(
        self,
        imagem: np.ndarray,
        trans_data: Dict
    ) -> List[TelhadoTransformador]:
        """Detecta telhados com YOLO"""
        
        deteccoes = []
        
        try:
            resultados = self.modelo_deteccao(imagem, conf=0.5)
            
            for i, resultado in enumerate(resultados):
                if resultado.boxes is None:
                    continue
                
                for j, box in enumerate(resultado.boxes):
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    confianca = float(box.conf[0])
                    
                    x = int(x1)
                    y = int(y1)
                    w = int(x2 - x1)
                    h = int(y2 - y1)
                    
                    area_pixeis = w * h
                    cx = x + w // 2
                    cy = y + h // 2
                    
                    resolucao_m = 0.3
                    area_m2 = area_pixeis * (resolucao_m ** 2)
                    
                    telhado = TelhadoTransformador(
                        id_telhado=f"trafo_{trans_data['id']}_telhado_{j}",
                        id_transformador=trans_data['id'],
                        id_subestacao=trans_data['subestacao_id'],
                        id_imagem_fonte=f"img_{trans_data['id']}",
                        bbox={'x': x, 'y': y, 'w': w, 'h': h},
                        centroide={'x': cx, 'y': cy},
                        latitude=trans_data['latitude'],
                        longitude=trans_data['longitude'],
                        area_pixeis=area_pixeis,
                        area_m2=area_m2,
                        confianca=confianca,
                        tipo_edificio="residencial",
                        fonte_imagem="google_maps",
                        resolucao_cm=30.0
                    )
                    
                    deteccoes.append(telhado)
            
            return deteccoes
        
        except Exception as e:
            logger.error(f"Erro ao detectar com YOLO: {e}")
            return []
    
    def _resultado_erro(
        self,
        transformador_id: int,
        motivo: str,
        tempo_ms: float,
        subestacao_id: int = 0,
        fonte_imagem: str = "google_maps"
    ) -> ResultadoDeteccaoTransformador:
        """Cria resultado de erro padronizado"""
        return ResultadoDeteccaoTransformador(
            transformador_id=transformador_id,
            subestacao_id=subestacao_id,
            sucesso=False,
            total_telhados=0,
            telhados=[],
            area_total_m2=0,
            confianca_media=0,
            motivo=motivo,
            tempo_processamento_ms=tempo_ms,
            fonte_imagem=fonte_imagem
        )
    
    def _salvar_rois(self, rois: List[TelhadoSegmentado], 
                     diretorio_saida: str,
                     id_subestacao: str):
        """Salva ROIs em disco"""
        Path(diretorio_saida).mkdir(parents=True, exist_ok=True)
        
        for roi in rois:
            try:
                nome_arquivo = f"{id_subestacao}_{roi.id_telhado}.png"
                caminho = os.path.join(diretorio_saida, nome_arquivo)
                
                imagem_bgr = cv2.cvtColor(roi.imagem_roi, cv2.COLOR_BGR2RGB)
                cv2.imwrite(caminho, imagem_bgr)
                
                roi.caminho_arquivo = caminho
                logger.debug(f"ROI salva: {caminho}")
                
            except Exception as e:
                logger.error(f"Erro ao salvar ROI {roi.id_telhado}: {e}")
    
    def salvar_deteccoes(self, resultado: ResultadoDeteccaoTransformador) -> bool:
        """Salva detecções de telhados no banco de dados"""
        if not self.engine:
            return False
        
        try:
            with self.engine.begin() as conn:
                for telhado in resultado.telhados:
                    conn.execute(text("""
                        INSERT INTO telhados_detectados_transformador
                        (transformador_id, subestacao_id, latitude, longitude, 
                         area_m2, confianca, bbox_json, timestamp_deteccao)
                        VALUES (:trans_id, :sub_id, :lat, :lon, 
                                :area, :conf, :bbox, :timestamp)
                    """), {
                        'trans_id': telhado.id_transformador,
                        'sub_id': telhado.id_subestacao,
                        'lat': telhado.latitude,
                        'lon': telhado.longitude,
                        'area': telhado.area_m2,
                        'conf': telhado.confianca,
                        'bbox': json.dumps(telhado.bbox),
                        'timestamp': telhado.timestamp_deteccao
                    })
            
            logger.info(f"✅ {len(resultado.telhados)} telhados salvos no banco")
            return True
        
        except Exception as e:
            logger.error(f"Erro ao salvar detecções: {e}")
            return False


# ===========================
# BACKWARD COMPATIBILITY ALIASES
# ===========================

TelhadoSegmentationService = RoofService
TelhadoService = RoofService
TelhadoTransformadorService = RoofService
