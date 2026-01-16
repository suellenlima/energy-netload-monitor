from fastapi import APIRouter

from ..core.database import get_engine
from ..services.geospatial import fetch_usinas_geojson

router = APIRouter(prefix="/usinas")


@router.get("/geo")
def get_usinas_geojson(limite: int = 100):
    try:
        engine = get_engine()
        return fetch_usinas_geojson(engine, limite)
    except Exception as exc:
        print(f"Erro no GeoJSON: {exc}")
        return {}