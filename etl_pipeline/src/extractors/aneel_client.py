import io
import logging
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import (
    create_db_engine,
    create_session,
    delete_all_rows,
    load_settings,
    request,
    table_exists,
)

SIGA_URL = (
    "https://dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao-da-aneel/"
    "resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv"
)


def extract_siga_csv(session, settings, logger: logging.Logger) -> bytes:
    response = request(session, "GET", SIGA_URL, settings=settings.http, logger=logger)
    response.raise_for_status()
    return response.content


def transform_siga_csv(content: bytes, logger: logging.Logger) -> gpd.GeoDataFrame:
    df = pd.read_csv(io.BytesIO(content), sep=";", encoding="ISO-8859-1", decimal=",")
    df_clean = df.rename(
        columns={
            "IdeNucleoCEG": "ceg",
            "NomEmpreendimento": "nome",
            "SigTipoGeracao": "fonte",
            "DscOrigemCombustivel": "combustivel",
            "MdaPotenciaOutorgadaKw": "potencia_kw",
            "NumCoordNEmpreendimento": "latitude",
            "NumCoordEEmpreendimento": "longitude",
        }
    )

    columns = ["ceg", "nome", "fonte", "combustivel", "potencia_kw", "latitude", "longitude"]
    df_clean = df_clean[columns].dropna(subset=["latitude", "longitude", "potencia_kw"])
    df_clean["potencia_kw"] = pd.to_numeric(df_clean["potencia_kw"], errors="coerce")
    df_clean["latitude"] = pd.to_numeric(df_clean["latitude"], errors="coerce")
    df_clean["longitude"] = pd.to_numeric(df_clean["longitude"], errors="coerce")

    gdf = gpd.GeoDataFrame(
        df_clean,
        geometry=gpd.points_from_xy(df_clean.longitude, df_clean.latitude),
        crs="EPSG:4326",
    )
    logger.info("Preparadas %s linhas para carga.", len(gdf))
    return gdf


def _has_registered_srid(engine, table: str, column: str = "geometry", schema: str = "public") -> bool:
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT Find_SRID(:schema, :table, :column)"),
                {"schema": schema, "table": table, "column": column},
            ).scalar()
        return bool(result)
    except Exception:
        return False


def load_siga_data(gdf: gpd.GeoDataFrame, engine, logger: logging.Logger) -> int:
    if gdf.empty:
        logger.info("Sem linhas para carregar.")
        return 0

    if not table_exists(engine, "usinas_siga") or not _has_registered_srid(engine, "usinas_siga"):
        gdf.to_postgis("usinas_siga", engine, if_exists="replace", index=False)
        logger.info("Tabela usinas_siga criada/recriada com metadata PostGIS.")
    else:
        delete_all_rows(engine, "usinas_siga")
        gdf.to_postgis("usinas_siga", engine, if_exists="append", index=False)

    logger.info("Carregadas %s linhas em usinas_siga.", len(gdf))
    return int(len(gdf))


def run_extraction(session=None, engine=None, settings=None, logger=None) -> int:
    logger = logger or logging.getLogger("etl.aneel")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL nao esta configurada.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(settings.http, logger=logger)

    logger.info("Iniciando extracao ANEEL SIGA.")
    content = extract_siga_csv(session, settings, logger)
    gdf = transform_siga_csv(content, logger)
    return load_siga_data(gdf, engine, logger)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.aneel")
    try:
        run_extraction(logger=logger)
    except Exception:
        logger.exception("Falha na extracao ANEEL.")
        raise


if __name__ == "__main__":
    main()
