from __future__ import annotations

import json

import geopandas as gpd
from sqlalchemy.engine import Engine

def fetch_usinas_geojson(engine: Engine, limit: int = 100) -> dict:
	sql = f"SELECT nome, fonte, potencia_kw, geom FROM usinas_siga LIMIT {limit}"
	gdf = gpd.read_postgis(sql, engine, geom_col="geom")
	return json.loads(gdf.to_json())
