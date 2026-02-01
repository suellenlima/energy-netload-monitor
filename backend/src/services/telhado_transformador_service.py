"""
Serviço de Detecção de Telhados por Transformador

Pipeline especializado para:
1. Buscar transformadores de uma subestação
2. Obter imagens via Google Maps ou CBERS-4A (por transformador)
3. Detectar telhados/edifícios (YOLOv8)
4. Segmentar telhados individuais
5. Extrair ROIs para análise de painéis solares

Diferenças vs TelhadoSegmentationService:
- Escopo: Transformador (1-2 km²) ao invés de subestação (5-100 km²)
- Resolução: Prioriza Google Maps (0.3m) ou CBERS-4A (2m)
- Processamento: Mais rápido, áreas menores
- Aplicação: Análise de potencial solar residencial/comercial

Author: Energy Netload Monitor
Date: 2026-01-31
"""

import os
import json
import logging
import requests
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path

import numpy as np
import cv2
from PIL import Image
from io import BytesIO
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None

logger = logging.getLogger(__name__)


@dataclass
class TelhadoTransformador:
    """Telhado detectado em contexto de transformador"""
    
    # IDs
    id_telhado: str
    id_transformador: int
    id_subestacao: int
    id_imagem_fonte: str  # Google Maps ou CBERS-4A
    
    # Localização na imagem
    bbox: Dict[str, float]  # {x, y, w, h} em pixels
    centroide: Dict[str, float]  # {x, y} do centro em pixels
    
    # Coordenadas geográficas (calculadas a partir da imagem)
    latitude: float
    longitude: float
    
    # Propriedades
    area_pixeis: int
    area_m2: float
    confianca: float
    
    # Tipologia
    tipo_edificio: str  # "residencial", "comercial", "industrial", "desconhecido"
    
    # Processamento
    timestamp_deteccao: datetime = field(default_factory=datetime.now)
    fonte_imagem: str = "google_maps"  # "google_maps" ou "cbers4a"
    resolucao_cm: float = 30.0  # 30cm para Google Maps, 200cm para CBERS-4A
    
    def to_dict(self) -> Dict:
        """Converte para dicionário"""
        data = asdict(self)
        data['timestamp_deteccao'] = self.timestamp_deteccao.isoformat()
        return data


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


class TelhadoTransformadorService:
    """Serviço de detecção de telhados por transformador"""
    
    def __init__(self, engine: Engine, modelo_yolo_path: str = None):
        """
        Inicializa o serviço
        
        Args:
            engine: SQLAlchemy engine
            modelo_yolo_path: Caminho para modelo YOLOv8 customizado
                             Se None, usa modelo treinado: notebooks/roof_dataset_yolo/trained_models/best.pt
        """
        self.engine = engine
        
        # Usar modelo treinado por padrão
        if modelo_yolo_path is None:
            # Caminho relativo ao workspace
            import os
            workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            modelo_yolo_path = os.path.join(workspace_root, 'notebooks', 'roof_dataset_yolo', 'trained_models', 'best.pt')
    
    def limpar_telhados_transformador(self, transformador_id: int) -> int:
        """
        Remove todos os telhados detectados anteriormente para o transformador
        
        Args:
            transformador_id: ID do transformador
        
        Returns:
            Número de telhados removidos
        """
        try:
            with self.engine.begin() as conn:
                # Verificar quantos telhados existem
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM telhados_detectados_transformador 
                    WHERE transformador_id = :trans_id
                """), {'trans_id': transformador_id})
                
                count = result.scalar()
                
                if count > 0:
                    # Deletar telhados antigos
                    conn.execute(text("""
                        DELETE FROM telhados_detectados_transformador 
                        WHERE transformador_id = :trans_id
                    """), {'trans_id': transformador_id})
                    
                    logger.info(f"🗑️ {count} telhados anteriores removidos do transformador {transformador_id}")
                    return count
                else:
                    logger.debug(f"✓ Nenhum telhado anterior encontrado para transformador {transformador_id}")
                    return 0
        
        except Exception as e:
            logger.error(f"❌ Erro ao limpar telhados do transformador {transformador_id}: {e}")
            return 0
            
            # Fallback para modelo pré-treinado se o treinado não existir
            if not os.path.exists(modelo_yolo_path):
                logger.warning(f"⚠️ Modelo treinado não encontrado em {modelo_yolo_path}")
                logger.warning(f"⚠️ Usando modelo pré-treinado yolov8n-seg.pt como fallback")
                modelo_yolo_path = "yolov8n-seg.pt"
        
        self.modelo_yolo_path = modelo_yolo_path
        self.modelo_yolo = None
        
        # Carregar modelo YOLO
        if YOLO is not None:
            try:
                self.modelo_yolo = YOLO(self.modelo_yolo_path)
                logger.info(f"✅ Modelo YOLO carregado: {self.modelo_yolo_path}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao carregar YOLO: {e}")
    
    # ========================================================================
    # OBTER TRANSFORMADORES DE SUBESTAÇÃO
    # ========================================================================
    
    def listar_transformadores_subestacao(self, subestacao_id: int) -> List[Dict]:
        """
        Lista todos os transformadores de uma subestação
        
        Args:
            subestacao_id: ID da subestação
        
        Returns:
            Lista de transformadores com coordenadas
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id,
                        nome,
                        latitude,
                        longitude,
                        potencia_kva,
                        codigo,
                        status
                    FROM transformadores
                    WHERE subestacao_id = :sub_id
                    AND status = 'ativo'
                    ORDER BY id
                """), {'sub_id': subestacao_id})
                
                transformadores = []
                for row in result:
                    transformadores.append({
                        'id': row[0],
                        'nome': row[1],
                        'latitude': float(row[2]),
                        'longitude': float(row[3]),
                        'potencia_kva': float(row[4]),
                        'codigo': row[5],
                        'status': row[6]
                    })
                
                logger.info(f"✅ {len(transformadores)} transformadores encontrados na SE {subestacao_id}")
                return transformadores
        
        except Exception as e:
            logger.error(f"❌ Erro ao listar transformadores: {e}")
            return []
    
    # ========================================================================
    # DETECÇÃO DE TELHADOS POR TRANSFORMADOR
    # ========================================================================
    
    def detectar_telhados_transformador(
        self,
        transformador_id: int,
        imagem_path: str,
        fonte_imagem: str = "google_maps"
    ) -> ResultadoDeteccaoTransformador:
        """
        Detecta telhados em área de um transformador
        
        Args:
            transformador_id: ID do transformador
            imagem_path: Caminho da imagem (local ou URL)
            fonte_imagem: "google_maps" ou "cbers4a"
        
        Returns:
            ResultadoDeteccaoTransformador com telhados encontrados
        """
        tempo_inicio = datetime.now()
        
        try:
            # 1. Buscar dados do transformador
            trans_data = self._obter_transformador(transformador_id)
            if not trans_data:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    motivo=f"Transformador {transformador_id} não encontrado",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            subestacao_id = trans_data['subestacao_id']
            
            # 2. Carregar imagem
            imagem = self._carregar_imagem(imagem_path)
            if imagem is None:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    motivo=f"Não foi possível carregar imagem: {imagem_path}",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            # 3. Detectar com YOLO
            if self.modelo_yolo is None:
                return self._resultado_erro(
                    transformador_id=transformador_id,
                    subestacao_id=subestacao_id,
                    motivo="Modelo YOLO não disponível",
                    tempo_ms=(datetime.now() - tempo_inicio).total_seconds() * 1000
                )
            
            deteccoes = self._detectar_com_yolo(imagem, trans_data)
            
            # 4. Calcular estatísticas
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
    
    # ========================================================================
    # PROCESSAMENTO EM LOTE
    # ========================================================================
    
    def detectar_telhados_subestacao(
        self,
        subestacao_id: int,
        imagens_por_transformador: Dict[int, str],
        fonte_imagem: str = "google_maps"
    ) -> List[ResultadoDeteccaoTransformador]:
        """
        Detecta telhados para todos os transformadores de uma subestação
        
        Args:
            subestacao_id: ID da subestação
            imagens_por_transformador: Dict com {transformador_id: caminho_imagem}
            fonte_imagem: "google_maps" ou "cbers4a"
        
        Returns:
            Lista de ResultadoDeteccaoTransformador
        """
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
    # AUXILIARES PRIVADOS
    # ========================================================================
    
    def _obter_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca dados do transformador no banco"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, nome, latitude, longitude, 
                        subestacao_id, potencia_kva, codigo
                    FROM transformadores
                    WHERE id = :id AND status = 'ativo'
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
                # Carregar de URL
                response = requests.get(imagem_path)
                imagem = Image.open(BytesIO(response.content))
            else:
                # Carregar de arquivo
                imagem = Image.open(imagem_path)
            
            # Converter para numpy array (RGB)
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
            # Executar YOLO
            resultados = self.modelo_yolo(imagem, conf=0.5)
            
            for i, resultado in enumerate(resultados):
                if resultado.boxes is None:
                    continue
                
                for j, box in enumerate(resultado.boxes):
                    # Extrair informações do box
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    confianca = float(box.conf[0])
                    
                    # Calcular propriedades
                    x = int(x1)
                    y = int(y1)
                    w = int(x2 - x1)
                    h = int(y2 - y1)
                    
                    area_pixeis = w * h
                    
                    # Calcular centroide
                    cx = x + w // 2
                    cy = y + h // 2
                    
                    # Estimar área em m² (assumindo resolução)
                    # Google Maps 0.3m/pixel, CBERS-4A 2m/pixel
                    resolucao_m = 0.3  # Google Maps default
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
                        tipo_edificio="residencial",  # Classificação simplificada
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
    
    # ========================================================================
    # PERSISTÊNCIA NO BANCO
    # ========================================================================
    
    def salvar_deteccoes(self, resultado: ResultadoDeteccaoTransformador) -> bool:
        """
        Salva detecções de telhados no banco de dados
        
        Args:
            resultado: ResultadoDeteccaoTransformador com as detecções
        
        Returns:
            True se sucesso, False caso contrário
        """
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


# Função auxiliar para imports
def criar_servico(engine: Engine, modelo_path: str = None) -> TelhadoTransformadorService:
    """Factory function para criar serviço"""
    return TelhadoTransformadorService(engine, modelo_path)
