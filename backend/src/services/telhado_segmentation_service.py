"""
Serviço de Segmentação e Detecção de Telhados em Imagens de Satélite

Pipeline completo para:
1. Baixar imagens de satélite via CBERS-4A/INPE (2m resolução)
2. Detectar edifícios/telhados usando YOLOv8
3. Segmentar telhados individuais
4. Extrair ROIs (Region of Interest) para processamento posterior
5. Integrar com modelos YOLO de painéis solares

MIGRAÇÃO: Sentinel-2 (10m) → CBERS-4A (2m) para melhor detecção de telhados

"""

import os
import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path

import numpy as np
import cv2
import requests
from PIL import Image
from io import BytesIO

try:
    from ultralytics import YOLO
    print("✅ [IMPORT] ultralytics importado com sucesso")
except ImportError as e:
    YOLO = None
    print(f"❌ [IMPORT] Falha ao importar ultralytics: {e}")
except Exception as e:
    YOLO = None
    print(f"❌ [IMPORT] Erro inesperado ao importar ultralytics: {e}")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class TelhadoDetectado:
    """Informações de um telhado/edifício detectado"""
    
    # Identificação
    id_telhado: str
    id_subestacao: str
    id_imagem_satelite: str
    
    # Localização na imagem
    bbox: Dict[str, float]  # {x, y, w, h} em pixels
    bbox_normalizado: Dict[str, float]  # {x, y, w, h} normalizados (0-1)
    centroide: Dict[str, float]  # {x, y} em pixels
    
    # Coordenadas geográficas
    lat: float
    lon: float
    
    # Propriedades
    area_pixeis: int  # Número de pixels do telhado
    area_m2: float  # Estimativa de área em m²
    confianca: float  # Confiança da detecção (0-1)
    tipo_edificio: str  # "residencial", "comercial", "industrial", "desconhecido"
    
    # Segmentação
    mascara_segmentacao: Optional[np.ndarray] = None  # Máscara binária
    contorno: Optional[List[Tuple[int, int]]] = None  # Contorno do telhado
    
    # Metadados
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    modelo_deteccao: str = "yolov8n-seg"  # Modelo usado
    propriedades_adicionais: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Converte para dicionário JSON-serializable"""
        data = asdict(self)
        data['timestamp_deteccao'] = self.timestamp_deteccao.isoformat()
        data['mascara_segmentacao'] = None  # Não serializar máscara em JSON
        data['contorno'] = None
        return data


@dataclass
class TelhadoSegmentado:
    """Telhado após segmentação e extração de ROI"""
    
    id_telhado: str
    imagem_roi: np.ndarray  # Imagem do telhado extraída (RGB ou RGBA)
    mascara: np.ndarray  # Máscara do telhado
    bbox_original: Dict[str, float]  # Coordenadas na imagem original
    
    # Resolução e escala
    tamanho_pixeis: Tuple[int, int]  # (altura, largura)
    resolucao_m_por_pixel: float  # Escala: quantos metros por pixel
    
    # Qualidade
    percentual_cobertura: float  # % da ROI que é telhado
    indice_qualidade: float  # 0-1, baseado em contraste/ruído
    
    # Metadados
    timestamp: datetime = field(default_factory=datetime.now)
    caminho_arquivo: Optional[str] = None  # Se salvo em disco


@dataclass
class ResultadoProcessamentoTelhados:
    """Resultado do processamento de telhados em uma subestação"""
    
    id_subestacao: str
    id_imagem_satelite: str
    timestamp_processamento: datetime = field(default_factory=datetime.now)
    
    # Estatísticas
    telhados_detectados: int = 0
    total_telhados_segmentados: int = 0
    telhados_com_erro: int = 0
    tempo_processamento_segundos: float = 0.0
    
    # Dados
    telhados: List[TelhadoDetectado] = field(default_factory=list)
    telhados_segmentados: List[TelhadoSegmentado] = field(default_factory=list)
    
    # Alertas/Erros
    erros: List[str] = field(default_factory=list)
    avisos: List[str] = field(default_factory=list)


class TelhadoSegmentationService:
    """
    Serviço principal para segmentação e processamento de telhados
    
    Fluxo:
    1. download_imagem_cbers() → Baixa imagem de CBERS-4A (2m resolução)
    2. detectar_telhados() → YOLOv8 detecção
    3. segmentar_telhados() → OpenCV segmentação
    4. extrair_rois_telhados() → Croppa ROIs
    5. processar_telhados_lote() → Integração completa
    
    NOVO: Suporte a cache de imagens e CBERS-4A do INPE
    """
    
    def __init__(self, model_path: str = None, 
                 use_gpu: bool = True, 
                 use_cache: bool = True, usar_estrategia_hibrida: bool = True):
        """
        Inicializa o serviço
        
        Args:
            model_path: Caminho para modelo YOLOv8 customizado. 
                       Se None, usa modelo treinado: notebooks/roof_dataset_yolo/trained_models/best.pt
            use_gpu: Usar GPU se disponível
            use_cache: Usar cache para imagens CBERS
            usar_estrategia_hibrida: Usar fallback automático (CBERS→Google Maps)
        """
        logger.info("="*80)
        logger.info("🚀 INICIALIZANDO TelhadoSegmentationService")
        logger.info("="*80)
        
        # Usar modelo treinado de telhados por padrão
        if model_path is None:
            model_path = "notebooks/roof_dataset_yolo/trained_models/best.pt"
        
        logger.info(f"📁 model_path configurado: {model_path}")
        
        self.model_path = model_path
        self.model_path = model_path
        self.use_gpu = use_gpu and self._check_gpu_available()
        self.device = "0" if self.use_gpu else "cpu"
        self.use_cache = use_cache
        self.usar_estrategia_hibrida = usar_estrategia_hibrida
        
        logger.info(f"⚙️ Device: {self.device}")
        logger.info(f"⚙️ Estratégia híbrida: {usar_estrategia_hibrida}")
        
        # Carregar modelo YOLO (modelo treinado para detecção de telhados por padrão)
        self.modelo_deteccao = None
        logger.info("📥 Chamando _carregar_modelo_deteccao()...")
        self._carregar_modelo_deteccao()
        logger.info(f"📊 Modelo após carregamento: {self.modelo_deteccao}")
        
        # Inicializar estratégia híbrida ou apenas CBERS
        if self.usar_estrategia_hibrida:
            from .imagem_strategy_service import ImagemStrategyService
            self.strategy = ImagemStrategyService(preferencia_resolucao=2.0)
            logger.info("✓ Estratégia híbrida habilitada (CBERS→Google Maps fallback)")
        else:
            # Apenas CBERS + cache
            from .cbers_service import CBERSService
            self.cbers_service = CBERSService()
            
            if self.use_cache:
                from .cache_service import CacheService
                self.cache = CacheService(cache_dir="data/cache/cbers", max_age_days=30)
                logger.info("Cache de imagens CBERS habilitado")
            else:
                self.cache = None
        
        logger.info(f"TelhadoSegmentationService inicializado. Device: {self.device}")
    
    def _check_gpu_available(self) -> bool:
        """Verifica se CUDA está disponível"""
        try:
            import torch
            return torch.cuda.is_available()
        except ImportError:
            return False
    
    def _carregar_modelo_deteccao(self):
        """Carrega modelo YOLO treinado para detecção de telhados"""
        global YOLO
        
        logger.info("="*80)
        logger.info("🔧 _carregar_modelo_deteccao() CHAMADO")
        logger.info(f"🔍 YOLO global = {YOLO}")
        logger.info(f"🔍 YOLO is None = {YOLO is None}")
        
        # Tentar reimportar
        try:
            from ultralytics import YOLO as YOLO_LOCAL
            logger.info(f"✅ Reimport bem-sucedido! YOLO_LOCAL = {YOLO_LOCAL}")
            # Usar o import local
            YOLO = YOLO_LOCAL
            logger.info(f"✅ YOLO global atualizado para: {YOLO}")
        except Exception as e:
            logger.error(f"❌ Falha ao reimportar ultralytics: {e}")
        
        logger.info("="*80)
        
        if YOLO is None:
            logger.error("❌ ultralytics não está instalado. Instale com: pip install ultralytics")
            logger.error("   Sem o modelo YOLO, nenhum telhado será detectado!")
            logger.error("   VERIFIQUE OS LOGS DE IMPORT NO INÍCIO DO ARQUIVO!")
            return
        
        logger.info(f"✓ ultralytics IMPORTADO com sucesso. YOLO={YOLO}")
        
        try:
            # Tentar caminho relativo do backend
            import os
            from pathlib import Path
            
            logger.info(f"📂 Tentando carregar modelo YOLO de: {self.model_path}")
            
            # Se for caminho relativo, converter para absoluto
            if not os.path.isabs(self.model_path):
                base_path = Path(__file__).parent.parent.parent.parent  # Volta para raiz do projeto
                model_full_path = base_path / self.model_path
                logger.info(f"   Caminho absoluto calculado: {model_full_path}")
            else:
                model_full_path = Path(self.model_path)
            
            if not model_full_path.exists():
                logger.warning(f"⚠️ Modelo treinado não encontrado: {model_full_path}")
                logger.info("🔍 Tentando usar modelo genérico yolov8n-seg.pt como fallback")
                
                # Tentar na raiz do projeto
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
                    logger.error(f"   Procurado em: {model_full_path}")
                    logger.error(f"   Fallback 1: {fallback1}")
                    logger.error(f"   Fallback 2: {fallback2}")
                    self.modelo_deteccao = None
                    return
            
            logger.info(f"⏳ Carregando modelo YOLO...")
            self.modelo_deteccao = YOLO(str(model_full_path))
            self.modelo_deteccao.to(self.device)
            logger.info(f"✅ Modelo YOLO carregado com sucesso: {model_full_path.name}")
            logger.info(f"   Caminho completo: {model_full_path}")
            logger.info(f"   Device: {self.device}")
        except Exception as e:
            logger.error(f"❌ ERRO ao carregar modelo YOLO: {e}")
            logger.exception(e)
            self.modelo_deteccao = None
    
    # ============================================================================
    # HELPERS: AUTENTICAÇÃO AZURE
    # ============================================================================
    
    def _get_azure_headers(self) -> Dict[str, str]:
        """Retorna headers com autenticação Azure para Sentinel-2"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Verificar se SAS token está configurado
        sas_token = os.getenv('AZURE_SAS_TOKEN', '').strip()
        if sas_token:
            headers['Authorization'] = f'Bearer {sas_token}'
            logger.debug("Usando autenticação Azure SAS token")
        
        return headers
    
    def _build_authenticated_url(self, url: str) -> str:
        """Constrói URL autenticada com SAS token se necessário"""
        if 'blob.core.windows.net' in url:
            sas_token = os.getenv('AZURE_SAS_TOKEN', '').strip()
            if sas_token and '?' not in url:
                # Adicionar SAS token como query parameter
                return f"{url}?{sas_token}"
        
        return url
    
    # ============================================================================
    # PASSO 1 (LEGADO): DOWNLOAD DE IMAGEM SENTINEL-2
    # ============================================================================
    
    def download_imagem_satelite(self, url_imagem: str, 
                                 timeout: int = 30,
                                 sem_autenticacao: bool = False) -> Optional[np.ndarray]:
        """
        Baixa imagem de satélite de URL ou carrega arquivo local
        
        Args:
            url_imagem: URL da imagem ou caminho local (ex: ./data/test_images/test_roof.jpg)
            timeout: Timeout em segundos
            sem_autenticacao: Se True, baixa sem autenticação (útil para Google Maps, etc)
            
        Returns:
            Imagem como numpy array (BGR) ou None se erro
        """
        try:
            # Verificar se é caminho local
            if url_imagem.startswith('./') or url_imagem.startswith('/') or ':\\' in url_imagem:
                logger.info(f"Carregando imagem local: {url_imagem}")
                imagem = Image.open(url_imagem)
                # IMPORTANTE: Converter para RGB (remove modo paleta 'P', preto&branco, etc)
                if imagem.mode != 'RGB':
                    logger.info(f"Convertendo imagem local do modo '{imagem.mode}' para RGB")
                    imagem = imagem.convert('RGB')
                
                # Aumentar saturação para melhor detecção
                from PIL import ImageEnhance
                enhancer_color = ImageEnhance.Color(imagem)
                imagem = enhancer_color.enhance(1.5)  # Aumenta saturação em 50%
            else:
                # Aumentar limite do Pillow para TIFF grandes ANTES de usar
                from PIL import Image as PILImage
                PILImage.MAX_IMAGE_PIXELS = None  # Remover limite
                
                # Se sem_autenticacao=True, baixa direto sem headers/SAS token
                if sem_autenticacao:
                    logger.info(f"Baixando imagem SEM autenticação: {url_imagem[:80]}...")
                    response = requests.get(url_imagem, timeout=timeout)
                    response.raise_for_status()
                else:
                    # Construir headers com autenticação Azure
                    headers = self._get_azure_headers()
                    
                    # Construir URL autenticada (com SAS token se houver)
                    url_autenticada = self._build_authenticated_url(url_imagem)
                    
                    # Baixar de URL
                    logger.info(f"Baixando imagem com autenticação: {url_imagem[:80]}...")
                    response = requests.get(url_autenticada, timeout=timeout, headers=headers)
                    response.raise_for_status()
                
                imagem = PILImage.open(BytesIO(response.content))
            
            # IMPORTANTE: Converter para RGB (remove modo paleta 'P', preto&branco, etc)
            if imagem.mode != 'RGB':
                logger.info(f"Convertendo imagem do modo '{imagem.mode}' para RGB")
                imagem = imagem.convert('RGB')
            
            # Aumentar saturação para melhor detecção (Google Maps costuma retornar cores desbotadas)
            from PIL import ImageEnhance
            enhancer_color = ImageEnhance.Color(imagem)
            imagem = enhancer_color.enhance(1.5)  # Aumenta saturação em 50%
            logger.info("Saturação da imagem aumentada em 50% para melhor detecção")
            
            # Converter para numpy array
            imagem_array = np.array(imagem)
            
            # Converter RGB→BGR para OpenCV
            if len(imagem_array.shape) == 3 and imagem_array.shape[2] >= 3:
                imagem_array = cv2.cvtColor(imagem_array, cv2.COLOR_RGB2BGR)
            
            logger.info(f"Imagem carregada: {imagem_array.shape}")
            return imagem_array
            
        except Exception as e:
            logger.error(f"Erro ao baixar imagem: {e}")
            return None
    
    # ============================================================================
    # PASSO 2: DETECÇÃO DE EDIFÍCIOS/TELHADOS COM YOLO
    # ============================================================================
    
    def detectar_telhados(self, imagem: np.ndarray, 
                         confianca_minima: float = 0.5,
                         iou_threshold: float = 0.5) -> List[TelhadoDetectado]:
        """
        Detecta painéis solares em imagem usando YOLOv8 treinado
        
        Args:
            imagem: Imagem em numpy array (BGR)
            confianca_minima: Confiança mínima para aceitação (0-1)
            iou_threshold: IOU threshold para NMS
            
        Returns:
            Lista de TelhadoDetectado (painéis solares detectados)
        """
        if self.modelo_deteccao is None:
            logger.error("❌ MODELO YOLO NÃO ESTÁ CARREGADO!")
            logger.error("   Verifique os logs de inicialização do serviço.")
            logger.error("   Possíveis causas:")
            logger.error("   1. ultralytics não está instalado")
            logger.error("   2. Arquivo do modelo não foi encontrado")
            logger.error("   3. Erro ao carregar o modelo")
            logger.error("   RETORNANDO LISTA VAZIA - NENHUM TELHADO SERÁ DETECTADO!")
            return []
        
        # 📸 [DEBUG] Salvar imagem final antes de YOLO
        try:
            diretorio_debug = os.path.join(os.path.dirname(__file__), "../../data/debug_imagens")
            os.makedirs(diretorio_debug, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_imagem_debug = os.path.join(diretorio_debug, f"imagem_final_{timestamp}.png")
            
            cv2.imwrite(caminho_imagem_debug, imagem)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"📸 IMAGEM FINAL ANTES DE YOLO")
            logger.info(f"{'='*80}")
            logger.info(f"Salva em: {caminho_imagem_debug}")
            logger.info(f"Shape: {imagem.shape} | Dtype: {imagem.dtype}")
            logger.info(f"Pixel Min: {imagem.min()}, Max: {imagem.max()}")
            logger.info(f"Channels: BGR (B={imagem[:,:,0].mean():.1f}, G={imagem[:,:,1].mean():.1f}, R={imagem[:,:,2].mean():.1f})")
            logger.info(f"Memory: {imagem.nbytes / (1024*1024):.2f} MB")
            logger.info(f"{'='*80}\n")
        except Exception as e:
            logger.warning(f"Erro ao salvar imagem de debug: {e}")
        
        try:
            # Executar detecção
            resultados = self.modelo_deteccao(imagem, 
                                             conf=confianca_minima,
                                             iou=iou_threshold,
                                             device=self.device)
            
            telhados_detectados = []
            
            for i, resultado in enumerate(resultados):
                # Extrair bounding boxes
                if resultado.boxes is not None:
                    boxes = resultado.boxes
                    
                    for j, box in enumerate(boxes):
                        # Coordenadas em pixels
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        confianca = box.conf[0].cpu().item()
                        
                        # Validações
                        if confianca < confianca_minima:
                            continue
                        
                        x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
                        
                        # Calcular dimensões
                        w = x2 - x1
                        h = y2 - y1
                        cx = x1 + w / 2
                        cy = y1 + h / 2
                        area_pixeis = w * h
                        
                        # Normalizar coordenadas
                        img_h, img_w = imagem.shape[:2]
                        bbox_norm = {
                            "x": (x1 / img_w),
                            "y": (y1 / img_h),
                            "w": (w / img_w),
                            "h": (h / img_h)
                        }
                        
                        # Criar objeto TelhadoDetectado
                        telhado = TelhadoDetectado(
                            id_telhado=f"telhado_{i}_{j}",
                            id_subestacao="desconhecido",  # Preenchido depois
                            id_imagem_satelite="desconhecido",
                            bbox={"x": x1, "y": y1, "w": w, "h": h},
                            bbox_normalizado=bbox_norm,
                            centroide={"x": cx, "y": cy},
                            lat=0.0,  # Preenchido depois
                            lon=0.0,
                            area_pixeis=area_pixeis,
                            area_m2=0.0,  # Preenchido depois com resolução
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
    
    # ============================================================================
    # PASSO 3: SEGMENTAÇÃO COM OPENCV
    # ============================================================================
    
    def segmentar_telhados(self, imagem: np.ndarray,
                          telhados: List[TelhadoDetectado]) -> List[TelhadoDetectado]:
        """
        Segmenta telhados utilizando OpenCV (contours, morphology, etc)
        
        Args:
            imagem: Imagem original
            telhados: Lista de telhados detectados
            
        Returns:
            Lista de telhados com máscaras e contornos preenchidos
        """
        # Converter para escala de cinza
        gray = cv2.cvtColor(imagem, cv2.COLOR_BGR2GRAY)
        
        # Equalizar histograma para melhor contraste
        gray = cv2.equalizeHist(gray)
        
        # Aplicar desfoque bilateral (suaviza mantendo bordas)
        gray = cv2.bilateralFilter(gray, 9, 75, 75)
        
        # Detectar bordas com Canny
        edges = cv2.Canny(gray, 50, 150)
        
        # Morfologia para fechar lacunas e refinar bordas
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
        edges = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel, iterations=2)
        edges = cv2.morphologyEx(edges, cv2.MORPH_OPEN, kernel, iterations=1)
        
        # Processar cada telhado detectado
        telhados_segmentados = []
        
        for telhado in telhados:
            try:
                x = int(telhado.bbox["x"])
                y = int(telhado.bbox["y"])
                w = int(telhado.bbox["w"])
                h = int(telhado.bbox["h"])
                
                # Extrair ROI da borda
                roi_edges = edges[y:y+h, x:x+w]
                
                # Encontrar contornos
                contours, _ = cv2.findContours(roi_edges, 
                                               cv2.RETR_TREE, 
                                               cv2.CHAIN_APPROX_SIMPLE)
                
                if contours:
                    # Maior contorno é provavelmente o telhado
                    contorno_principal = max(contours, key=cv2.contourArea)
                    
                    # Criar máscara
                    mascara = np.zeros((h, w), dtype=np.uint8)
                    cv2.drawContours(mascara, [contorno_principal], 0, 255, -1)
                    
                    # Suavizar máscara
                    mascara = cv2.morphologyEx(mascara, cv2.MORPH_CLOSE, kernel, iterations=2)
                    
                    # Calcular estatísticas
                    area_mascara = cv2.countNonZero(mascara)
                    area_total = w * h
                    cobertura_percentual = (area_mascara / area_total) * 100 if area_total > 0 else 0
                    
                    # Atualizar informações do telhado
                    telhado.mascara_segmentacao = mascara
                    telhado.contorno = contorno_principal.tolist()
                    telhado.area_pixeis = area_mascara
                    
                    # Calcular índice de qualidade (baseado em contraste)
                    roi_cinza = gray[y:y+h, x:x+w]
                    indice_qualidade = min(1.0, np.std(roi_cinza) / 50.0)  # Normalizar
                    telhado.propriedades_adicionais['indice_qualidade'] = indice_qualidade
                    telhado.propriedades_adicionais['percentual_cobertura'] = cobertura_percentual
                    
                    telhados_segmentados.append(telhado)
                    logger.debug(f"Telhado {telhado.id_telhado}: "
                                f"cobertura={cobertura_percentual:.1f}%, qualidade={indice_qualidade:.2f}")
                
            except Exception as e:
                logger.warning(f"Erro ao segmentar telhado {telhado.id_telhado}: {e}")
                continue
        
        logger.info(f"Segmentados {len(telhados_segmentados)} de {len(telhados)} telhados")
        return telhados_segmentados
    
    # ============================================================================
    # PASSO 4: EXTRAÇÃO DE ROIs
    # ============================================================================
    
    def extrair_rois_telhados(self, imagem: np.ndarray,
                              telhados: List[TelhadoDetectado],
                              resolucao_m_por_pixel: float = 3.0,
                              padding_percentual: float = 0.1) -> List[TelhadoSegmentado]:
        """
        Extrai ROIs individuais de telhados para processamento posterior
        
        Args:
            imagem: Imagem original
            telhados: Telhados segmentados
            resolucao_m_por_pixel: Escala da imagem satélite
            padding_percentual: Padding em torno do telhado (10% = 0.1)
            
        Returns:
            Lista de TelhadoSegmentado com imagens extraídas
        """
        telhados_segmentados = []
        
        for telhado in telhados:
            try:
                x = int(telhado.bbox["x"])
                y = int(telhado.bbox["y"])
                w = int(telhado.bbox["w"])
                h = int(telhado.bbox["h"])
                
                # Aplicar padding
                padding_x = int(w * padding_percentual)
                padding_y = int(h * padding_percentual)
                
                x_start = max(0, x - padding_x)
                y_start = max(0, y - padding_y)
                x_end = min(imagem.shape[1], x + w + padding_x)
                y_end = min(imagem.shape[0], y + h + padding_y)
                
                # Extrair ROI
                roi_imagem = imagem[y_start:y_end, x_start:x_end]
                
                # Extrair e redimensionar máscara se existir
                roi_mascara = None
                if telhado.mascara_segmentacao is not None:
                    roi_mascara = telhado.mascara_segmentacao
                else:
                    # Criar máscara padrão
                    roi_mascara = np.ones_like(roi_imagem[:, :, 0], dtype=np.uint8) * 255
                
                # Calcular estatísticas de qualidade
                roi_cinza = cv2.cvtColor(roi_imagem, cv2.COLOR_BGR2GRAY)
                percentual_cobertura = np.sum(roi_mascara > 127) / roi_mascara.size * 100
                indice_qualidade = telhado.propriedades_adicionais.get('indice_qualidade', 0.5)
                
                # Criar objeto TelhadoSegmentado
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
                logger.debug(f"ROI extraída para {telhado.id_telhado}: "
                            f"tamanho={roi_imagem.shape}, qualidade={indice_qualidade:.2f}")
                
            except Exception as e:
                logger.error(f"Erro ao extrair ROI do telhado {telhado.id_telhado}: {e}")
                continue
        
        logger.info(f"Extraídas {len(telhados_segmentados)} ROIs")
        return telhados_segmentados
    
    # ============================================================================
    # PASSO 5: PIPELINE COMPLETO
    # ============================================================================
    
    def processar_telhados_lote(self, url_imagem: str,
                               id_subestacao: str,
                               id_imagem_satelite: str,
                               resolucao_m_por_pixel: float = 3.0,
                               confianca_minima: float = 0.5,
                               diretorio_saida: Optional[str] = None,
                               sem_autenticacao: bool = False) -> ResultadoProcessamentoTelhados:
        """
        Pipeline completo: download → detecção → segmentação → extração
        
        Args:
            url_imagem: URL da imagem satélite
            id_subestacao: ID da subestação
            id_imagem_satelite: ID da imagem
            resolucao_m_por_pixel: Escala da imagem
            confianca_minima: Confiança mínima para detecção
            diretorio_saida: Diretório para salvar ROIs (opcional)
            sem_autenticacao: Se True, baixa imagem sem autenticação Azure
            
        Returns:
            ResultadoProcessamentoTelhados com todos os dados
        """
        import time
        
        tempo_inicio = time.time()
        resultado = ResultadoProcessamentoTelhados(
            id_subestacao=id_subestacao,
            id_imagem_satelite=id_imagem_satelite
        )
        
        try:
            # Passo 1: Download
            logger.info(f"[1/4] Baixando imagem de {url_imagem}...")
            imagem = self.download_imagem_satelite(url_imagem, sem_autenticacao=sem_autenticacao)
            if imagem is None:
                logger.error("ERRO: Imagem não foi baixada ou convertida!")
                resultado.erros.append("Falha ao baixar imagem")
                return resultado
            
            logger.info(f"✓ Imagem baixada com sucesso: shape={imagem.shape}, dtype={imagem.dtype}")
            
            # 📸 [DEBUG] Salvar imagem após download
            try:
                diretorio_debug = os.path.join(os.path.dirname(__file__), "../../data/debug_imagens")
                os.makedirs(diretorio_debug, exist_ok=True)
                
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
                caminho_debug = os.path.join(diretorio_debug, f"imagem_apos_download_{id_subestacao}_{timestamp}.png")
                
                cv2.imwrite(caminho_debug, imagem)
                
                logger.info(f"\n{'='*80}")
                logger.info(f"📸 IMAGEM SALVA APÓS DOWNLOAD")
                logger.info(f"{'='*80}")
                logger.info(f"Arquivo: {caminho_debug}")
                logger.info(f"Shape: {imagem.shape}")
                logger.info(f"Dtype: {imagem.dtype}")
                logger.info(f"Min: {imagem.min()}, Max: {imagem.max()}")
                logger.info(f"Memória: {imagem.nbytes / (1024*1024):.2f} MB")
                logger.info(f"{'='*80}\n")
            except Exception as e:
                logger.warning(f"Erro ao salvar debug image: {e}")
            
            # Passo 2: Detecção
            logger.info("[2/4] Detectando telhados...")
            telhados_detectados = self.detectar_telhados(imagem, confianca_minima)
            
            if not telhados_detectados:
                resultado.avisos.append("Nenhum telhado detectado")
                return resultado
            
            # Atualizar IDs
            for telhado in telhados_detectados:
                telhado.id_subestacao = id_subestacao
                telhado.id_imagem_satelite = id_imagem_satelite
            
            resultado.telhados = telhados_detectados
            resultado.telhados_detectados = len(telhados_detectados)
            
            # Passo 3: Segmentação
            logger.info("[3/4] Segmentando telhados...")
            telhados_seg = self.segmentar_telhados(imagem, telhados_detectados)
            resultado.total_telhados_segmentados = len(telhados_seg)
            
            # Passo 4: Extração de ROIs
            logger.info("[4/4] Extraindo ROIs...")
            rois = self.extrair_rois_telhados(imagem, telhados_seg, resolucao_m_por_pixel)
            resultado.telhados_segmentados = rois
            resultado.total_telhados_segmentados = len(rois)
            
            # Salvar ROIs se diretório especificado
            if diretorio_saida:
                self._salvar_rois(rois, diretorio_saida, id_subestacao)
            
            # Estatísticas finais
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            logger.info(f"✓ Pipeline concluído em {resultado.tempo_processamento_segundos:.2f}s. "
                       f"Detectados: {resultado.telhados_detectados}, "
                       f"Segmentados: {resultado.total_telhados_segmentados}")
            
            return resultado
            
        except Exception as e:
            logger.error(f"Erro crítico no pipeline: {e}")
            resultado.erros.append(f"Erro crítico: {str(e)}")
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            return resultado
    
    # ============================================================================
    # UTILITÁRIOS
    # ============================================================================
    
    def _salvar_rois(self, rois: List[TelhadoSegmentado], 
                     diretorio_saida: str,
                     id_subestacao: str):
        """Salva ROIs em disco"""
        Path(diretorio_saida).mkdir(parents=True, exist_ok=True)
        
        for roi in rois:
            try:
                # Salvar imagem
                nome_arquivo = f"{id_subestacao}_{roi.id_telhado}.png"
                caminho = os.path.join(diretorio_saida, nome_arquivo)
                
                imagem_bgr = cv2.cvtColor(roi.imagem_roi, cv2.COLOR_BGR2RGB)
                cv2.imwrite(caminho, imagem_bgr)
                
                roi.caminho_arquivo = caminho
                logger.debug(f"ROI salva: {caminho}")
                
            except Exception as e:
                logger.error(f"Erro ao salvar ROI {roi.id_telhado}: {e}")
    
    def visualizar_deteccoes(self, imagem: np.ndarray,
                           telhados: List[TelhadoDetectado],
                           mostrar_confianca: bool = True) -> np.ndarray:
        """
        Cria imagem com visualização das detecções
        
        Returns:
            Imagem anotada com bounding boxes
        """
        imagem_anotada = imagem.copy()
        
        for telhado in telhados:
            x = int(telhado.bbox["x"])
            y = int(telhado.bbox["y"])
            w = int(telhado.bbox["w"])
            h = int(telhado.bbox["h"])
            
            # Cor baseada na confiança
            confianca = telhado.confianca
            cor = (0, int(255 * confianca), int(255 * (1 - confianca)))  # Verde↔Vermelho
            
            # Desenhar bounding box
            cv2.rectangle(imagem_anotada, (x, y), (x + w, y + h), cor, 2)
            
            # Desenhar texto com confiança
            if mostrar_confianca:
                texto = f"{confianca:.2f}"
                cv2.putText(imagem_anotada, texto, (x, y - 5),
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, cor, 2)
        
        return imagem_anotada
    
    def salvar_resultado_completo(self, resultado: ResultadoProcessamentoTelhados,
                                 caminho_json: str):
        """Salva resultado em JSON"""
        dados = {
            "id_subestacao": resultado.id_subestacao,
            "id_imagem_satelite": resultado.id_imagem_satelite,
            "timestamp_processamento": resultado.timestamp_processamento.isoformat(),
            "telhados_detectados": resultado.telhados_detectados,
            "telhados_segmentados": resultado.total_telhados_segmentados,
            "tempo_processamento_segundos": resultado.tempo_processamento_segundos,
            "telhados": [t.to_dict() for t in resultado.telhados],
            "erros": resultado.erros,
            "avisos": resultado.avisos
        }
        
        with open(caminho_json, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Resultado salvo em {caminho_json}")
