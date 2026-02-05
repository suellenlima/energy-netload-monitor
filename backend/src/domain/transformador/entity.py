"""Transformador domain entity.

A Transformador represents an electrical transformer in the ANEEL network.
It's the root aggregate for the transformador bounded context.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ..comum.value_objects import Localizacao, Potencia
from .errors import InvalidTransformadorError
from .value_objects import (
    AreaCobertura,
    CodigoTransformador,
    NomeTransformador,
    TensaoTipo,
)


@dataclass
class Transformador:
    """
    Domain entity representing an electrical transformer.

    A transformador is an immutable aggregate root that contains all business logic
    related to transformers in the ANEEL network.
    """

    # Identity
    id: int

    # Core attributes as Value Objects
    codigo: CodigoTransformador
    nome: NomeTransformador
    potencia: Potencia
    localizacao: Localizacao
    tipo_tensao: TensaoTipo

    # Optional attributes
    subestacao_codigo: Optional[str] = None
    distribuidora: Optional[str] = None
    area_cobertura: Optional[AreaCobertura] = None
    ativo: bool = True
    criado_em: Optional[datetime] = None
    atualizado_em: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Validate entity invariants after initialization."""
        self._validar_invariantes()

    def _validar_invariantes(self) -> None:
        """
        Validate business rules invariants.

        These rules must always be true for a valid Transformador.
        """
        if self.id <= 0:
            raise InvalidTransformadorError("Transformador ID must be positive.")

        if not self.ativo and self.area_cobertura:
            raise InvalidTransformadorError(
                "Inactive transformador cannot have active coverage area."
            )

    def muda_status_ativacao(self, ativo: bool) -> "Transformador":
        """
        Change activation status of transformador.

        Args:
            ativo: New activation status

        Returns:
            New Transformador instance with updated status
        """
        if self.ativo == ativo:
            return self

        # Create new instance with updated status
        novo_transformador = Transformador(
            id=self.id,
            codigo=self.codigo,
            nome=self.nome,
            potencia=self.potencia,
            localizacao=self.localizacao,
            tipo_tensao=self.tipo_tensao,
            subestacao_codigo=self.subestacao_codigo,
            distribuidora=self.distribuidora,
            area_cobertura=self.area_cobertura if ativo else None,
            ativo=ativo,
            criado_em=self.criado_em,
            atualizado_em=datetime.now(),
        )
        return novo_transformador

    def associa_area_cobertura(self, area: AreaCobertura) -> "Transformador":
        """
        Associate a coverage area with this transformador.

        Args:
            area: Coverage area to associate

        Returns:
            New Transformador instance with coverage area

        Raises:
            InvalidTransformadorError: If transformador is inactive
        """
        if not self.ativo:
            raise InvalidTransformadorError(
                "Cannot associate coverage area to inactive transformador."
            )

        novo_transformador = Transformador(
            id=self.id,
            codigo=self.codigo,
            nome=self.nome,
            potencia=self.potencia,
            localizacao=self.localizacao,
            tipo_tensao=self.tipo_tensao,
            subestacao_codigo=self.subestacao_codigo,
            distribuidora=self.distribuidora,
            area_cobertura=area,
            ativo=self.ativo,
            criado_em=self.criado_em,
            atualizado_em=datetime.now(),
        )
        return novo_transformador

    def remove_area_cobertura(self) -> "Transformador":
        """
        Remove coverage area from this transformador.

        Returns:
            New Transformador instance without coverage area
        """
        if not self.area_cobertura:
            return self

        novo_transformador = Transformador(
            id=self.id,
            codigo=self.codigo,
            nome=self.nome,
            potencia=self.potencia,
            localizacao=self.localizacao,
            tipo_tensao=self.tipo_tensao,
            subestacao_codigo=self.subestacao_codigo,
            distribuidora=self.distribuidora,
            area_cobertura=None,
            ativo=self.ativo,
            criado_em=self.criado_em,
            atualizado_em=datetime.now(),
        )
        return novo_transformador

    def distancia_para(self, outra_localizacao: Localizacao) -> float:
        """
        Calculate distance to another location using simplified formula.

        This is a simplified calculation. For production, use Haversine formula.

        Args:
            outra_localizacao: Target location

        Returns:
            Approximate distance in kilometers
        """
        from math import sqrt

        # Simplified: 1 degree ≈ 111 km
        dlat = (outra_localizacao.latitude - self.localizacao.latitude) * 111
        dlon = (
            (outra_localizacao.longitude - self.localizacao.longitude)
            * 111
            * abs(((self.localizacao.latitude + outra_localizacao.latitude) / 2))
        )
        return sqrt(dlat**2 + dlon**2)

    def __str__(self) -> str:
        """Return string representation."""
        return f"Transformador(id={self.id}, codigo={self.codigo}, nome={self.nome})"

    def __repr__(self) -> str:
        """Return detailed representation."""
        return (
            f"Transformador(id={self.id}, codigo={self.codigo!r}, "
            f"nome={self.nome!r}, potencia={self.potencia!r}, "
            f"localizacao={self.localizacao!r}, ativo={self.ativo})"
        )
