from fastapi import APIRouter
from sqlalchemy import text

from ..core.database import get_engine

router = APIRouter()


@router.get("/")
def read_root():
    return {"message": "Energy Monitor Online ⚡"}


@router.get("/health")
def health_check():
    try:
        engine = get_engine()
    except RuntimeError as exc:
        return {"status": "error", "detail": str(exc)}

    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1")).scalar()
        return {"status": "ok", "db_response": result}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}