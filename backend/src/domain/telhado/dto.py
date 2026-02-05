"""Data transfer objects and result types for Telhado domain.

Migrado de: src/services/roof_service.py (STEP 2 da refatoração)
Responsabilidade: Armazenar tipos usados pela aplicação
"""

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.infrastructure.ml import TelhadoDetectado, TelhadoSegmentado


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
