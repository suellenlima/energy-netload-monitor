"""Domain Entity - Subestacao"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal

from .value_objects import (
    CodigoSubestacao,
    NomeSubestacao,
    TensaoNominal,
    AreaCobertura,
)
from .errors import SubestacaoTensaoInvalidaError, SubestacaoPotenciaInvalidaError


@dataclass
class Subestacao:
    """Aggregate Root - Subestacao
    
    Entidade de domínio que representa uma subestação elétrica.
    Responsável pela lógica de negócio relacionada a subestações.
    """
    
    id: int
    codigo: CodigoSubestacao
    nome: NomeSubestacao
    tensao_nominal_kv: TensaoNominal
    potencia_nominal_mva: float
    area_cobertura_km2: AreaCobertura
    latitude: float
    longitude: float
    distribuidora_codigo: Optional[str] = None
    distribuidora_nome: Optional[str] = None
    ativo: bool = True
    timestamp_criacao: datetime = field(default_factory=datetime.now)
    timestamp_atualizacao: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validações do aggregate após criação"""
        if self.potencia_nominal_mva <= 0:
            raise SubestacaoPotenciaInvalidaError(self.potencia_nominal_mva)
    
    # ========================================================================
    # OPERAÇÕES DE NEGÓCIO
    # ========================================================================
    
    def ativar(self) -> None:
        """Ativa a subestação"""
        if self.ativo:
            return
        self.ativo = True
        self.timestamp_atualizacao = datetime.now()
    
    def desativar(self) -> None:
        """Desativa a subestação"""
        if not self.ativo:
            return
        self.ativo = False
        self.timestamp_atualizacao = datetime.now()
    
    def calcular_potencia_por_transformador(self, num_transformadores: int) -> float:
        """Calcula potência média por transformador.
        
        Args:
            num_transformadores: Número de transformadores
            
        Returns:
            Potência média em MVA por transformador
        """
        if num_transformadores <= 0:
            return 0
        return self.potencia_nominal_mva / num_transformadores
    
    def eh_alta_tensao(self) -> bool:
        """Verifica se é subestação de alta tensão (> 69kV)"""
        return self.tensao_nominal_kv.valor > 69
    
    def eh_media_tensao(self) -> bool:
        """Verifica se é subestação de média tensão (13-69kV)"""
        tensao = self.tensao_nominal_kv.valor
        return 13 <= tensao <= 69
    
    def eh_baixa_tensao(self) -> bool:
        """Verifica se é subestação de baixa tensão (< 13kV)"""
        return self.tensao_nominal_kv.valor < 13
    
    def obter_tipo_tensao(self) -> str:
        """Retorna tipo de tensão (AT/MT/BT)"""
        if self.eh_alta_tensao():
            return "AT"  # Alta Tensão
        elif self.eh_media_tensao():
            return "MT"  # Média Tensão
        else:
            return "BT"  # Baixa Tensão
    
    def atualizar_localizacao(self, latitude: float, longitude: float) -> None:
        """Atualiza coordenadas geográficas.
        
        Args:
            latitude: Latitude em graus
            longitude: Longitude em graus
        """
        if not (-90 <= latitude <= 90):
            raise ValueError(f"Latitude inválida: {latitude}")
        if not (-180 <= longitude <= 180):
            raise ValueError(f"Longitude inválida: {longitude}")
        
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp_atualizacao = datetime.now()
    
    def atualizar_area_cobertura(self, area_km2: float) -> None:
        """Atualiza área de cobertura.
        
        Args:
            area_km2: Área em km²
        """
        self.area_cobertura_km2 = AreaCobertura(area_km2)
        self.timestamp_atualizacao = datetime.now()
    
    # ========================================================================
    # CONVERSÃO
    # ========================================================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte entidade para dicionário"""
        return {
            'id': self.id,
            'codigo': str(self.codigo),
            'nome': str(self.nome),
            'tensao_nominal_kv': self.tensao_nominal_kv.valor,
            'potencia_nominal_mva': self.potencia_nominal_mva,
            'area_cobertura_km2': self.area_cobertura_km2.valor,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'distribuidora_codigo': self.distribuidora_codigo,
            'distribuidora_nome': self.distribuidora_nome,
            'tipo_tensao': self.obter_tipo_tensao(),
            'ativo': self.ativo,
            'timestamp_criacao': self.timestamp_criacao.isoformat(),
            'timestamp_atualizacao': self.timestamp_atualizacao.isoformat(),
        }
