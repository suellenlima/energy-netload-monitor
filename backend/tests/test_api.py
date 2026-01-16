import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend" / "src"))

import main as app_main

client = TestClient(app_main.app)


def reset_engine(url):
    app_main.DATABASE_URL = url
    app_main._ENGINE = None


def test_root_ok():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "message" in resp.json()


def test_health_error_shape():
    reset_engine(None)
    resp = client.get("/health")
    assert resp.status_code == 500
    body = resp.json()
    assert body.get("status") == "error"
    assert "detail" in body


def test_health_ok_sqlite():
    reset_engine("sqlite+pysqlite:///:memory:")
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json().get("db_response") == 1