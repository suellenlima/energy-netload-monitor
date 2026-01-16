from fastapi import APIRouter

from ..core.database import get_engine
from ..services.load_calc import list_distribuidoras

router = APIRouter(prefix="/auxiliar")


@router.get("/distribuidoras")
def get_lista_distribuidoras():
    engine = get_engine()
    return list_distribuidoras(engine)