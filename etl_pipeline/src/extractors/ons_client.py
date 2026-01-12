import io
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, create_session, delete_time_window, load_settings, request

CKAN_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=carga-energia"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def find_carga_column(columns: Iterable[str]) -> Optional[str]:
    candidates = [
        "val_cargaenergiamw",
        "val_cargaenergiamediamw",
        "val_cargaeneergiamwmed",
        "val_cargaenergiammwmed",
        "val_cargaenergiamwmed",
    ]
    for name in candidates:
        if name in columns:
            return name

    for col in columns:
        if col.startswith("val_carga"):
            return col

    return None


def get_dynamic_url(session, settings, logger: logging.Logger) -> Optional[str]:
    logger.info("Consultando API CKAN do ONS.")
    try:
        response = request(session, "GET", CKAN_API_URL, settings=settings.http, logger=logger)
        response.raise_for_status()
        data = response.json()
        resources = data["result"]["resources"]
        current_year = str(datetime.now().year)
        previous_year = str(datetime.now().year - 1)

        for res in resources:
            name = res.get("name", "")
            if current_year in name and res.get("format", "").upper() == "CSV":
                return res.get("url")
        for res in resources:
            name = res.get("name", "")
            if previous_year in name and res.get("format", "").upper() == "CSV":
                return res.get("url")
        return None
    except Exception as exc:
        logger.warning("Erro na API do ONS: %s", exc)
        return None


def transform_carga_ons_csv(content: bytes, logger: logging.Logger) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content), sep=";", decimal=",")
    df = _normalize_columns(df)
    df = df.rename(
        columns={
            "din_instante": "time",
            "nom_subsistema": "subsistema",
            "subsistema": "subsistema",
            "time": "time",
        }
    )

    carga_col = find_carga_column(df.columns)
    if not carga_col:
        logger.warning("Coluna de carga nao encontrada. Colunas: %s", list(df.columns))
        return pd.DataFrame(columns=["time", "subsistema", "carga_mw"])

    df = df.rename(columns={carga_col: "carga_mw"})
    df["time"] = pd.to_datetime(df["time"])
    df["carga_mw"] = pd.to_numeric(df["carga_mw"], errors="coerce")

    df_final = df[["time", "subsistema", "carga_mw"]].dropna()
    return df_final.drop_duplicates(subset=["time", "subsistema"])


def load_carga_ons(df: pd.DataFrame, engine, logger: logging.Logger) -> int:
    if df.empty:
        logger.info("Sem linhas para carregar.")
        return 0

    min_time = df["time"].min()
    max_time = df["time"].max()
    subsistemas = df["subsistema"].dropna().unique().tolist()

    if subsistemas:
        delete_time_window(
            engine,
            "carga_ons",
            "time",
            min_time,
            max_time,
            filters={"subsistema": subsistemas},
        )

    df.to_sql(
        "carga_ons",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )
    logger.info("Carregadas %s linhas em carga_ons.", len(df))
    return int(len(df))


def run_extraction(session=None, engine=None, settings=None, logger=None) -> int:
    logger = logger or logging.getLogger("etl.ons")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL is not configured.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(settings.http, logger=logger)

    logger.info("Iniciando extracao ONS.")
    target_url = get_dynamic_url(session, settings, logger)
    if not target_url:
        logger.warning("Nenhum link CSV encontrado.")
        return 0

    response = request(session, "GET", target_url, settings=settings.http, logger=logger)
    response.raise_for_status()

    df_final = transform_carga_ons_csv(response.content, logger)
    return load_carga_ons(df_final, engine, logger)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.ons")
    try:
        run_extraction(logger=logger)
    except Exception:
        logger.exception("Falha na extracao ONS.")
        raise


if __name__ == "__main__":
    main()