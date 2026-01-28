from __future__ import annotations

import json
import logging

import geopandas as gpd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def fetch_usinas_geojson(engine: Engine, limit: int = 100) -> dict:
    """
    Busca usinas em formato GeoJSON.

    Args:
        engine: Engine SQLAlchemy
        limit: Máximo de registros (default 100, max 1000)

    Returns:
        GeoJSON FeatureCollection
    """
    # Validar e limitar o parâmetro limit para evitar abusos
    safe_limit = max(1, min(limit, 1000))

    query = text(
        "SELECT nome, fonte, potencia_kw, geom FROM usinas_siga LIMIT :limit"
    )

    try:
        gdf = gpd.read_postgis(query, engine, geom_col="geom", params={"limit": safe_limit})
        return json.loads(gdf.to_json())
    except Exception as exc:
        logger.error(f"Erro ao buscar GeoJSON de usinas: {exc}", exc_info=True)
        return {"type": "FeatureCollection", "features": []}
