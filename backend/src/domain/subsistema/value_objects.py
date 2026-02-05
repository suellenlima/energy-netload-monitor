"""Value Objects for Subsistema domain."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Subsistema:
    """Subsistema elétrico do Brasil (ONS)."""
    
    subsistema: str  # Ex: "norte", "nordeste", "sudeste/centro-oeste", "sul"
    codigo: str  # Ex: "NO", "NE", "SE/CO", "S"
    nome_completo: str  # Ex: "Subsistema Norte"
    regiao: str  # Ex: "Norte"
    descricao: str | None = None
    ativo: bool = True
    
    def __str__(self) -> str:
        return f"{self.subsistema} ({self.codigo})"
    
    def __repr__(self) -> str:
        return f"Subsistema(subsistema='{self.subsistema}', codigo='{self.codigo}')"
