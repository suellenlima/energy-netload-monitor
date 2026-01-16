import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extractors.gd_client import build_gd_dataframe, iter_gd_chunks, transform_gd_chunks


def test_transform_gd_chunks_aggregates_solar():
    data_path = ROOT / "tests" / "data" / "gd_sample.csv"
    logger = logging.getLogger("test.gd")

    aggregated = transform_gd_chunks(iter_gd_chunks(data_path, chunk_size=2), logger)
    df = build_gd_dataframe(aggregated)

    assert set(df.columns) == {"distribuidora", "classe", "sigla_uf", "fonte", "potencia_mw"}
    assert len(df) == 2

    row = df[(df["distribuidora"] == "DISTRIBUIDORA A") & (df["sigla_uf"] == "SP")].iloc[0]
    assert round(float(row["potencia_mw"]), 4) == 1.5

    row_b = df[(df["distribuidora"] == "DISTRIBUIDORA B") & (df["sigla_uf"] == "RJ")].iloc[0]
    assert round(float(row_b["potencia_mw"]), 4) == 0.25