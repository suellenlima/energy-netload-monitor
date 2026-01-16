import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extractors.inpe_weather_client import transform_weather_payload


def test_transform_weather_payload_shifts_year_safely():
    payload = {
        "hourly": {
            "time": ["2024-02-29T00:00", "2024-03-01T00:00"],
            "shortwave_radiation": [100.0, 200.0],
            "temperature_2m": [25.0, 26.0],
        }
    }
    df = transform_weather_payload(payload, "SUDESTE", offset_anos=2)

    assert list(df.columns) == ["time", "subsistema", "irradiancia_wm2", "temperatura_c"]
    assert len(df) == 2
    assert df["subsistema"].unique().tolist() == ["SUDESTE"]
    assert df["time"].iloc[0] == datetime(2026, 2, 28, 0, 0)
    assert df["time"].iloc[1] == datetime(2026, 3, 1, 0, 0)