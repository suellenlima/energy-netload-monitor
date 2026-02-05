"""Domain Value Objects - Subestacao"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CodigoSubestacao:
    """Código único de identificação da subestação"""
    
    valor: str
    
    def __post_init__(self):
        if not self.valor or len(self.valor.strip()) == 0:
            raise ValueError("Código de subestação não pode ser vazio")
    
    def __str__(self):
        return self.valor


@dataclass(frozen=True)
class NomeSubestacao:
    """Nome descritivo da subestação"""
    
    valor: str
    
    def __post_init__(self):
        if not self.valor or len(self.valor.strip()) == 0:
            raise ValueError("Nome de subestação não pode ser vazio")
    
    def __str__(self):
        return self.valor


@dataclass(frozen=True)
class TensaoNominal:
    """Tensão nominal de operação em kV"""
    
    valor: float
    
    def __post_init__(self):
        if self.valor is None or self.valor <= 0:
            raise ValueError(f"Tensão nominal deve ser > 0, obtido: {self.valor}")
    
    def __str__(self):
        return f"{self.valor} kV"


@dataclass(frozen=True)
class AreaCobertura:
    """Área de cobertura da subestação em km²"""
    
    valor: float
    
    def __post_init__(self):
        if self.valor is None or self.valor < 0:
            raise ValueError(f"Área de cobertura deve ser >= 0, obtido: {self.valor}")
    
    def __str__(self):
        return f"{self.valor} km²"
