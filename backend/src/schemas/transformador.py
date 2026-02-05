"""Transformador API response schemas (DTOs).

These Pydantic models define the structure of API responses.
They are separate from domain entities to allow the API contract
to evolve independently from domain logic.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TransformadorListResponse(BaseModel):
    """Transformador response for list endpoints."""

    id: int = Field(..., description="Transformador ID")
    codigo: str = Field(..., description="ANEEL transformador code")
    nome: str = Field(..., description="Transformador name")
    latitude: float = Field(..., description="Geographic latitude")
    longitude: float = Field(..., description="Geographic longitude")
    potencia_kva: float = Field(..., description="Power in kVA")
    tipo_tensao: str = Field(..., description="Voltage type (Alta, Média, Baixa)")
    subestacao_codigo: Optional[str] = Field(
        None, description="Associated substation code"
    )
    distribuidora: Optional[str] = Field(
        None, description="Distribution company name"
    )
    ativo: bool = Field(True, description="Is transformador active")

    class Config:
        """Pydantic config."""

        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "codigo": "TRANS001",
                "nome": "Transformador Centro",
                "latitude": -23.5505,
                "longitude": -46.6333,
                "potencia_kva": 300.0,
                "tipo_tensao": "Média",
                "subestacao_codigo": "SUB001",
                "distribuidora": "AES Eletropaulo",
                "ativo": True,
            }
        }


class TransformadorDetailResponse(BaseModel):
    """Transformador response for detail endpoints."""

    id: int = Field(..., description="Transformador ID")
    codigo: str = Field(..., description="ANEEL transformador code")
    nome: str = Field(..., description="Transformador name")
    latitude: float = Field(..., description="Geographic latitude")
    longitude: float = Field(..., description="Geographic longitude")
    potencia_kva: float = Field(..., description="Power in kVA")
    potencia_mva: float = Field(..., description="Power in MVA")
    potencia_w: float = Field(..., description="Power in Watts")
    tipo_tensao: str = Field(..., description="Voltage type (Alta, Média, Baixa)")
    subestacao_codigo: Optional[str] = Field(
        None, description="Associated substation code"
    )
    distribuidora: Optional[str] = Field(
        None, description="Distribution company name"
    )
    ativo: bool = Field(True, description="Is transformador active")
    criado_em: Optional[datetime] = Field(None, description="Creation timestamp")
    atualizado_em: Optional[datetime] = Field(None, description="Update timestamp")
    area_cobertura_geojson: Optional[str] = Field(
        None, description="Coverage area in GeoJSON format"
    )

    class Config:
        """Pydantic config."""

        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "codigo": "TRANS001",
                "nome": "Transformador Centro",
                "latitude": -23.5505,
                "longitude": -46.6333,
                "potencia_kva": 300.0,
                "potencia_mva": 0.3,
                "potencia_w": 300000.0,
                "tipo_tensao": "Média",
                "subestacao_codigo": "SUB001",
                "distribuidora": "AES Eletropaulo",
                "ativo": True,
                "criado_em": "2024-01-01T10:00:00",
                "atualizado_em": "2024-01-15T15:30:00",
                "area_cobertura_geojson": '{"type": "Polygon", "coordinates": [...]}',
            }
        }
