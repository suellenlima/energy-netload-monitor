from fastapi import FastAPI

from api.analise import router as analise_router
from api.auxiliar import router as auxiliar_router
from api.health import router as health_router
from api.usinas import router as usinas_router

app = FastAPI(title="Energy Netload Monitor API")

app.include_router(health_router)
app.include_router(usinas_router)
app.include_router(analise_router)
app.include_router(auxiliar_router)