import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, create_session, delete_time_window, load_settings, request

OFFSET_ANOS = 2
DIAS_ATRAS = 7

REGIOES = {
    "SUDESTE": {"lat": -23.55, "lon": -46.63},
    "SUL": {"lat": -30.03, "lon": -51.22},
    "NORDESTE": {"lat": -8.04, "lon": -34.87},
    "NORTE": {"lat": -1.45, "lon": -48.50},
}


def create_table_if_not_exists(engine) -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS clima_real (
                time TIMESTAMPTZ NOT NULL,
                subsistema VARCHAR(20),
                irradiancia_wm2 FLOAT,
                temperatura_c FLOAT,
                CONSTRAINT clima_real_unique UNIQUE (time, subsistema)
            );
        """
            )
        )
        try:
            conn.execute(text("SELECT create_hypertable('clima_real', 'time', if_not_exists => TRUE);"))
        except Exception:
            pass
        conn.commit()


def _shift_year_safe(dt: datetime, years: int) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        return dt.replace(year=dt.year + years, day=28)


def compute_date_window(
    now: datetime, dias_atras: int, offset_anos: int
) -> Tuple[datetime, datetime, str, str]:
    data_fim_sim = now
    data_inicio_sim = now - timedelta(days=dias_atras)
    real_start = _shift_year_safe(data_inicio_sim, -offset_anos)
    real_end = _shift_year_safe(data_fim_sim, -offset_anos)
    return (
        data_inicio_sim,
        data_fim_sim,
        real_start.strftime("%Y-%m-%d"),
        real_end.strftime("%Y-%m-%d"),
    )


def fetch_weather_payload(
    session,
    settings,
    coords: Dict[str, float],
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> Dict:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "shortwave_radiation,temperature_2m",
        "timezone": "America/Sao_Paulo",
    }
    response = request(session, "GET", url, params=params, settings=settings.http, logger=logger)
    response.raise_for_status()
    return response.json()


def transform_weather_payload(payload: Dict, subsistema: str, offset_anos: int) -> pd.DataFrame:
    if "hourly" not in payload:
        return pd.DataFrame(columns=["time", "subsistema", "irradiancia_wm2", "temperatura_c"])

    df = pd.DataFrame(
        {
            "time": payload["hourly"]["time"],
            "irradiancia_wm2": payload["hourly"]["shortwave_radiation"],
            "temperatura_c": payload["hourly"]["temperature_2m"],
        }
    )
    df["time"] = pd.to_datetime(df["time"])
    df["time"] = df["time"].apply(lambda x: _shift_year_safe(x, offset_anos))
    df["subsistema"] = subsistema
    df_final = df[["time", "subsistema", "irradiancia_wm2", "temperatura_c"]].dropna()
    return df_final


def load_weather_data(
    df: pd.DataFrame,
    engine,
    *,
    subsistema: str,
    start_time: datetime,
    end_time: datetime,
    logger: logging.Logger,
) -> int:
    if df.empty:
        logger.info("%s: sem linhas para carregar.", subsistema)
        return 0

    delete_time_window(
        engine,
        "clima_real",
        "time",
        start_time,
        end_time,
        filters={"subsistema": subsistema},
    )
    df.to_sql("clima_real", engine, if_exists="append", index=False, method="multi")
    logger.info("%s: carregadas %s linhas.", subsistema, len(df))
    return int(len(df))


def run_extraction(session=None, engine=None, settings=None, logger=None, now=None) -> int:
    logger = logger or logging.getLogger("etl.weather")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL is not configured.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(settings.http, logger=logger)
    create_table_if_not_exists(engine)

    now = now or datetime.now()
    data_inicio_sim, data_fim_sim, real_start_date, real_end_date = compute_date_window(
        now, DIAS_ATRAS, OFFSET_ANOS
    )
    logger.info(
        "Janela %s a %s (API %s a %s).",
        data_inicio_sim,
        data_fim_sim,
        real_start_date,
        real_end_date,
    )

    total_rows = 0
    for nome_sub, coords in REGIOES.items():
        try:
            payload = fetch_weather_payload(session, settings, coords, real_start_date, real_end_date, logger)
            df_final = transform_weather_payload(payload, nome_sub, OFFSET_ANOS)
            total_rows += load_weather_data(
                df_final,
                engine,
                subsistema=nome_sub,
                start_time=data_inicio_sim,
                end_time=data_fim_sim,
                logger=logger,
            )
        except Exception as exc:
            logger.warning("Erro ao carregar %s: %s", nome_sub, exc)

    return total_rows


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.weather")
    try:
        run_extraction(logger=logger)
    except Exception:
        logger.exception("Falha na extracao de clima.")
        raise


if __name__ == "__main__":
    main()