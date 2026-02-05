"""Telhado Detection Service (DDD Application Layer).

Serviço de aplicação para detecção e segmentação de telhados.
Agora contém TODA a lógica de negócio (refatoração completa do RoofService).

Architecture: Domain-Driven Design
Layer: Application Service
Responsabilidades:
- Orquestração de pipeline de detecção
- Operações CRUD de telhados
- Lógica de negócio e estatísticas
- Processamento por transformador/subestação
"""

import logging
import json
import time
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.infrastructure.ml import RoofDetectionService, TelhadoDetectado, TelhadoSegmentado
from src.domain.telhado.dto import (
    TelhadoTransformador,
    ResultadoProcessamentoTelhados,
    ResultadoDeteccaoTransformador,
)
from sqlalchemy import text
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    """Resultado de detecção de telhados."""
    
    total_detectados: int
    telhados: List[TelhadoDetectado]
    timestamp: datetime
    tempo_processamento_s: float
    modelo_utilizado: str


class TelhadoDetectionService:
    """Serviço de aplicação para detecção e gerenciamento de telhados (DDD).
    
    Responsabilidades:
    - Orquestração de pipeline ML (download → detecção → segmentação → ROI)
    - Processamento por transformador e subestação
    - CRUD de telhados (persistência)
    - Estatísticas e análises
    """
    
    def __init__(self, engine: Optional[Engine] = None, model_path: str = None, 
                 use_gpu: bool = True):
        """Inicializa o serviço.
        
        Args:
            engine: SQLAlchemy Engine para persistência
            model_path: Caminho para modelo YOLO
            use_gpu: Usar GPU se disponível
        """
        self.engine = engine
        self.logger = logger
        
        # Inicializar serviço de detecção ML (infraestrutura)
        self._detection_service = RoofDetectionService(
            model_path=model_path,
            use_gpu=use_gpu
        )
        
        # Repository para persistência
        self.repository = None
        if engine:
            from src.infrastructure.persistence.telhado.repository import SQLAlchemyTelhadoRepository
            self.repository = SQLAlchemyTelhadoRepository(engine)
    
    # ========================================================================
    # PASSO 1-4: PIPELINE COMPLETO (ML)
    # ========================================================================
    
    def processar_telhados_lote(
        self,
        url_imagem: str,
        id_subestacao: str,
        id_imagem_satelite: str,
        resolucao_m_por_pixel: float = 3.0,
        confianca_minima: float = 0.5,
        diretorio_saida: Optional[str] = None
    ) -> ResultadoProcessamentoTelhados:
        """Pipeline completo: download → detecção → segmentação → extração de ROI.
        
        Args:
            url_imagem: URL ou caminho da imagem
            id_subestacao: ID da subestação
            id_imagem_satelite: ID da imagem de satélite
            resolucao_m_por_pixel: Resolução em metros por pixel
            confianca_minima: Threshold mínimo de confiança
            diretorio_saida: Diretório para salvar ROIs (opcional)
            
        Returns:
            ResultadoProcessamentoTelhados com status e dados
        """
        import numpy as np
        import cv2
        from PIL import Image
        import requests
        from io import BytesIO
        
        tempo_inicio = time.time()
        resultado = ResultadoProcessamentoTelhados(
            id_subestacao=id_subestacao,
            id_imagem_satelite=id_imagem_satelite
        )
        
        try:
            self.logger.info(f"[1/4] Baixando imagem de {url_imagem}...")
            imagem = self._download_imagem_satelite(url_imagem)
            if imagem is None:
                resultado.erros.append("Falha ao baixar imagem")
                return resultado
            
            self.logger.info("[2/4] Detectando telhados...")
            deteccoes = self._detection_service.detectar_telhados(
                imagem, 
                confianca_minima=confianca_minima
            )
            
            if not deteccoes:
                resultado.avisos.append("Nenhum telhado detectado")
                return resultado
            
            # Converter para TelhadoDetectado
            telhados_detectados = []
            for det in deteccoes:
                telhado = TelhadoDetectado(
                    id_telhado=f"telhado_{len(telhados_detectados)}",
                    id_subestacao=id_subestacao,
                    id_imagem_satelite=id_imagem_satelite,
                    bbox=det['bbox'],
                    bbox_normalizado={},
                    centroide=det.get('centroide', {}),
                    lat=0.0,
                    lon=0.0,
                    area_pixeis=det.get('area_pixeis', 0),
                    area_m2=det.get('area_pixeis', 0) * (resolucao_m_por_pixel ** 2),
                    confianca=det.get('confianca', 0),
                    tipo_edificio="residencial"
                )
                telhados_detectados.append(telhado)
            
            resultado.telhados = telhados_detectados
            resultado.telhados_detectados = len(telhados_detectados)
            
            self.logger.info("[3/4] Segmentando telhados...")
            telhados_seg = self._detection_service.segmentar_telhados(imagem, deteccoes)
            resultado.total_telhados_segmentados = len(telhados_seg)
            
            self.logger.info("[4/4] Extraindo ROIs...")
            rois_info = self._detection_service.extrair_rois_telhados(
                imagem, 
                deteccoes,
                diretorio_saida=diretorio_saida
            )
            resultado.telhados_segmentados = telhados_seg
            resultado.total_telhados_segmentados = len(telhados_seg)
            
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            self.logger.info(
                f"✓ Pipeline concluído em {resultado.tempo_processamento_segundos:.2f}s"
            )
            
            return resultado
            
        except Exception as e:
            self.logger.error(f"Erro crítico no pipeline: {e}")
            resultado.erros.append(f"Erro crítico: {str(e)}")
            resultado.tempo_processamento_segundos = time.time() - tempo_inicio
            return resultado
    
    # ========================================================================
    # PROCESSAMENTO POR TRANSFORMADOR (ML + Persistência)
    # ========================================================================
    
    def detectar_telhados_transformador(
        self,
        transformador_id: int,
        imagem_path: str,
        fonte_imagem: str = "google_maps"
    ) -> ResultadoDeteccaoTransformador:
        """Detecta telhados em área de um transformador específico.
        
        Args:
            transformador_id: ID do transformador
            imagem_path: Caminho/URL da imagem
            fonte_imagem: Fonte da imagem (google_maps, sentinel2, etc)
            
        Returns:
            ResultadoDeteccaoTransformador com status e dados
        """
        tempo_inicio = datetime.now()
        
        try:
            trans_data = self._obter_transformador(transformador_id)
            if not trans_data:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    motivo=f"Transformador {transformador_id} não encontrado",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            subestacao_id = trans_data.get('subestacao_id', 0)
            
            imagem = self._carregar_imagem(imagem_path)
            if imagem is None:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    motivo=f"Não foi possível carregar imagem: {imagem_path}",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            # Detectar com ML
            deteccoes = self._detection_service.detectar_telhados(imagem, confianca_minima=0.5)
            
            # Converter para TelhadoTransformador
            telhados = []
            for i, det in enumerate(deteccoes):
                telhado = TelhadoTransformador(
                    id_telhado=f"trafo_{transformador_id}_telhado_{i}",
                    id_transformador=transformador_id,
                    id_subestacao=subestacao_id,
                    id_imagem_fonte=f"img_{transformador_id}",
                    bbox=det['bbox'],
                    centroide=det.get('centroide', {'x': 0, 'y': 0}),
                    latitude=trans_data.get('latitude', 0),
                    longitude=trans_data.get('longitude', 0),
                    area_pixeis=det.get('area_pixeis', 0),
                    area_m2=det.get('area_pixeis', 0) * 0.09,  # ~0.3m por pixel²
                    confianca=det.get('confianca', 0),
                    tipo_edificio="residencial",
                    fonte_imagem=fonte_imagem
                )
                telhados.append(telhado)
            
            area_total = sum(t.area_m2 for t in telhados)
            confianca_media = (
                sum(t.confianca for t in telhados) / len(telhados) if telhados else 0
            )
            
            tempo_ms = (datetime.now() - tempo_inicio).total_seconds() * 1000
            
            resultado = ResultadoDeteccaoTransformador(
                transformador_id=transformador_id,
                subestacao_id=subestacao_id,
                sucesso=True,
                total_telhados=len(telhados),
                telhados=telhados,
                area_total_m2=area_total,
                confianca_media=confianca_media,
                motivo="Sucesso",
                tempo_processamento_ms=tempo_ms,
                fonte_imagem=fonte_imagem
            )
            
            self.logger.info(
                f"✅ {len(telhados)} telhados detectados em transformador {transformador_id}"
            )
            return resultado
        
        except Exception as e:
            self.logger.error(f"❌ Erro na detecção: {e}", exc_info=True)
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
        """Detecta telhados para todos os transformadores de uma subestação.
        
        Args:
            subestacao_id: ID da subestação
            imagens_por_transformador: Dict {transformador_id: imagem_path}
            fonte_imagem: Fonte das imagens
            
        Returns:
            Lista de ResultadoDeteccaoTransformador
        """
        self.logger.info(f"🔍 Iniciando detecção para SE {subestacao_id}")
        
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
        
        self.logger.info(
            f"✅ Processamento concluído: {total_sucesso}/{len(resultados)} sucesso, "
            f"{total_telhados} telhados, {area_total:.0f} m²"
        )
        
        return resultados
    
    # ========================================================================
    # LÓGICA DE NEGÓCIO (CRUD e Estatísticas)
    # ========================================================================
    
    def listar_telhados(
        self,
        id_subestacao: Optional[str] = None,
        confianca_minima: float = 0.0,
        pagina: int = 1,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Lista telhados com filtros e paginação."""
        if not self.repository:
            return {'erro': 'Repository não configurado'}
        
        try:
            # Usar repository DDD para buscar
            offset = (pagina - 1) * limite
            resultado = self.repository.listar_paginados(offset, limite)
            
            self.logger.info(f"Listados {len(resultado)} telhados")
            return {
                'telhados': resultado,
                'pagina': pagina,
                'limite': limite,
                'total': len(resultado)
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao listar telhados: {e}")
            raise
    
    def obter_telhados_transformador(
        self,
        transformador_id: int,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Obtém telhados de um transformador específico."""
        if not self.repository:
            return {'erro': 'Repository não configurado'}
        
        try:
            telhados = self.repository.obter_por_transformador(transformador_id, limite)
            
            if not telhados:
                stats = {
                    'total': 0,
                    'area_total_m2': 0,
                    'confianca_media': 0
                }
            else:
                areas = [t.area_m2 for t in telhados if t.area_m2]
                confs = [t.confianca for t in telhados if t.confianca]
                stats = {
                    'total': len(telhados),
                    'area_total_m2': sum(areas) if areas else 0,
                    'confianca_media': (
                        sum(confs) / len(confs) if confs else 0
                    )
                }
            
            return {
                'transformador_id': transformador_id,
                'total': stats['total'],
                'area_total_m2': stats['area_total_m2'],
                'confianca_media': stats['confianca_media'],
                'telhados': [t.to_dict() if hasattr(t, 'to_dict') else t for t in telhados],
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            self.logger.error(
                f"Erro ao obter telhados do transformador {transformador_id}: {e}"
            )
            raise
    
    def obter_estatisticas_gerais(self) -> Dict[str, Any]:
        """Obtém estatísticas gerais de telhados."""
        if not self.repository:
            return {'erro': 'Repository não configurado'}
        
        try:
            todos_telhados = self.repository.listar_paginados(0, 10000)
            
            if not todos_telhados:
                return {
                    'total_telhados': 0,
                    'area_total_m2': 0,
                    'confianca_media': 0,
                    'confianca_minima': 0,
                    'confianca_maxima': 0,
                    'area_minima_m2': 0,
                    'area_maxima_m2': 0,
                    'timestamp': datetime.now().isoformat()
                }
            
            areas = [t.area_m2 for t in todos_telhados if hasattr(t, 'area_m2')]
            confs = [t.confianca for t in todos_telhados if hasattr(t, 'confianca')]
            
            return {
                'total_telhados': len(todos_telhados),
                'area_total_m2': sum(areas) if areas else 0,
                'area_media_m2': sum(areas) / len(areas) if areas else 0,
                'confianca_media': sum(confs) / len(confs) if confs else 0,
                'confianca_minima': min(confs) if confs else 0,
                'confianca_maxima': max(confs) if confs else 0,
                'area_minima_m2': min(areas) if areas else 0,
                'area_maxima_m2': max(areas) if areas else 0,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas gerais: {e}")
            raise
    
    def obter_telhado(self, telhado_id: int) -> Optional[Dict]:
        """Obtém detalhes de um telhado específico."""
        if not self.repository:
            return None
        
        try:
            telhado = self.repository.obter_por_id(telhado_id)
            return telhado.to_dict() if telhado and hasattr(telhado, 'to_dict') else telhado
        except Exception as e:
            self.logger.error(f"Erro ao obter telhado {telhado_id}: {e}")
            raise
    
    def salvar_deteccoes(
        self,
        resultado: ResultadoDeteccaoTransformador
    ) -> bool:
        """Salva detecções de telhados no banco de dados."""
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
            
            self.logger.info(f"✅ {len(resultado.telhados)} telhados salvos no banco")
            return True
        
        except Exception as e:
            self.logger.error(f"Erro ao salvar detecções: {e}")
            return False
    
    # ========================================================================
    # AUXILIARES PRIVADOS
    # ========================================================================
    
    def _download_imagem_satelite(self, url_imagem: str) -> Optional[Any]:
        """Baixa imagem de URL ou carrega arquivo local."""
        try:
            import numpy as np
            import cv2
            from PIL import Image
            import requests
            from io import BytesIO
            
            if url_imagem.startswith('./') or url_imagem.startswith('/') or ':\\' in url_imagem:
                self.logger.info(f"Carregando imagem local: {url_imagem}")
                imagem = Image.open(url_imagem)
                if imagem.mode != 'RGB':
                    imagem = imagem.convert('RGB')
                return cv2.cvtColor(np.array(imagem), cv2.COLOR_RGB2BGR)
            else:
                self.logger.info(f"Baixando imagem: {url_imagem[:80]}...")
                response = requests.get(url_imagem, timeout=30)
                response.raise_for_status()
                imagem = Image.open(BytesIO(response.content))
                if imagem.mode != 'RGB':
                    imagem = imagem.convert('RGB')
                return cv2.cvtColor(np.array(imagem), cv2.COLOR_RGB2BGR)
        
        except Exception as e:
            self.logger.error(f"Erro ao baixar imagem: {e}")
            return None
    
    def _carregar_imagem(self, imagem_path: str) -> Optional[Any]:
        """Carrega imagem de arquivo local ou URL."""
        try:
            import numpy as np
            import cv2
            from PIL import Image
            import requests
            from io import BytesIO
            
            if imagem_path.startswith('http'):
                response = requests.get(imagem_path)
                imagem = Image.open(BytesIO(response.content))
            else:
                imagem = Image.open(imagem_path)
            
            imagem_array = np.array(imagem)
            return cv2.cvtColor(imagem_array, cv2.COLOR_RGB2BGR)
        
        except Exception as e:
            self.logger.error(f"Erro ao carregar imagem: {e}")
            return None
    
    def _obter_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca dados do transformador no banco."""
        if not self.engine:
            return None
        
        try:
            with self.engine.connect() as conn:
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
            self.logger.error(f"Erro ao obter transformador: {e}")
            return None
    
    def _resultado_erro(
        self,
        transformador_id: int,
        motivo: str,
        tempo_ms: float,
        subestacao_id: int = 0,
        fonte_imagem: str = "google_maps"
    ) -> ResultadoDeteccaoTransformador:
        """Cria resultado de erro padronizado."""
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


__all__ = [
    "TelhadoDetectionService",
    "DetectionResult",
    "TelhadoDetectado",
    "TelhadoSegmentado",
    "TelhadoTransformador",
    "ResultadoProcessamentoTelhados",
    "ResultadoDeteccaoTransformador",
]
