"""Schemas comuns reutilizáveis."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QueryParams(BaseModel):
    """Parâmetros de query comuns."""

    limite: int = Field(default=100, ge=1, le=1000, description="Máximo de registros")
    distribuidora: str | None = Field(default=None, description="Filtrar por distribuidora")


class GeoJSONGeometry(BaseModel):
    """Geometria GeoJSON."""

    type: str = "Point"
    coordinates: list[float]


class GeoJSONFeature(BaseModel):
    """Feature GeoJSON."""

    type: str = "Feature"
    geometry: GeoJSONGeometry
    properties: dict[str, Any]


class GeoJSONResponse(BaseModel):
    """FeatureCollection GeoJSON."""

    type: str = "FeatureCollection"
    features: list[GeoJSONFeature] = Field(default_factory=list)
