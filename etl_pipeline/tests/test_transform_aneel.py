import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extractors.aneel_client import transform_siga_csv


def test_transform_siga_csv_filters_and_geometry():
    data_path = ROOT / "tests" / "data" / "aneel_sample.csv"
    content = data_path.read_bytes()
    logger = logging.getLogger("test.aneel")

    gdf = transform_siga_csv(content, logger)

    assert list(gdf.columns) == [
        "ceg",
        "nome",
        "fonte",
        "combustivel",
        "potencia_kw",
        "latitude",
        "longitude",
        "geometry",
    ]
    assert len(gdf) == 1
    row = gdf.iloc[0]
    assert row["ceg"] == "CEG1"
    assert float(row["potencia_kw"]) == 1000.5
    assert round(row.geometry.x, 4) == -50.0
    assert round(row.geometry.y, 4) == -10.0