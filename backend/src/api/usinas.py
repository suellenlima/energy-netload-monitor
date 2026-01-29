import logging

from fastapi import APIRouter

from ..core.database import get_engine
from ..services.geospatial import fetch_usinas_geojson

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/usinas")


@router.get("/geo")
def get_usinas_geojson(limite: int = 100):
    """Retorna usinas em formato GeoJSON."""
    try:
        engine = get_engine()
        return fetch_usinas_geojson(engine, limite)
    except Exception as exc:
        logger.error(f"Erro no GeoJSON: {exc}", exc_info=True)
        return {"type": "FeatureCollection", "features": []}