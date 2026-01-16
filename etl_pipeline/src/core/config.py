from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class HttpSettings:
    timeout_s: int
    retries: int
    backoff_factor: float
    log_level: str


@dataclass(frozen=True)
class DatabaseSettings:
    url: str


@dataclass(frozen=True)
class PathsSettings:
    data_dir: Path
    raw_dir: Path


@dataclass(frozen=True)
class Settings:
    http: HttpSettings
    database: DatabaseSettings
    paths: PathsSettings


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "")
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "")
    return float(value) if value else default


def load_settings() -> Settings:
    data_dir = Path(os.getenv("ETL_DATA_DIR", "/app/data"))
    raw_dir = Path(os.getenv("ETL_RAW_DIR", str(data_dir / "raw")))
    http = HttpSettings(
        timeout_s=_env_int("ETL_HTTP_TIMEOUT", 60),
        retries=_env_int("ETL_HTTP_RETRIES", 3),
        backoff_factor=_env_float("ETL_HTTP_BACKOFF", 0.5),
        log_level=os.getenv("ETL_HTTP_LOG_LEVEL", "INFO"),
    )
    database = DatabaseSettings(url=os.getenv("DATABASE_URL", ""))
    paths = PathsSettings(data_dir=data_dir, raw_dir=raw_dir)
    return Settings(http=http, database=database, paths=paths)