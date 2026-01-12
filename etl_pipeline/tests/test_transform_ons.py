import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extractors.ons_client import transform_carga_ons_csv


def test_transform_carga_ons_csv_dedupes():
    data_path = ROOT / "tests" / "data" / "ons_sample.csv"
    content = data_path.read_bytes()
    logger = logging.getLogger("test.ons")

    df = transform_carga_ons_csv(content, logger)

    assert list(df.columns) == ["time", "subsistema", "carga_mw"]
    assert len(df) == 2
    assert set(df["subsistema"]) == {"SUDESTE", "SUL"}