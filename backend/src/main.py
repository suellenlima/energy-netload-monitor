from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.analise import router as analise_router
from .api.auxiliar import router as auxiliar_router
from .api.health import router as health_router
from .api.realtime_estimation import router as realtime_estimation_router
from .api.load_calculation import router as load_calculation_router
from .api.satelite import router as satelite_router
from .api.subestacoes import router as subestacoes_router, router_ddd as subestacoes_router_ddd
from .api.transformadores import router as transformadores_router
from .api.telhados import router as telhados_router
from .core import (
    AppException,
    app_exception_handler,
    generic_exception_handler,
    get_settings,
    setup_logging,
)

# Carregar configurações
settings = get_settings()

# Configurar logging estruturado
setup_logging(settings.log_level)

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handlers
app.add_exception_handler(AppException, app_exception_handler)
app.add_exception_handler(Exception, generic_exception_handler)

# Routers
app.include_router(health_router)
app.include_router(analise_router)
app.include_router(auxiliar_router)
app.include_router(realtime_estimation_router)  # DDD API for RealTimeEstimation
app.include_router(load_calculation_router)  # DDD API for LoadCalculation
app.include_router(subestacoes_router)
app.include_router(subestacoes_router_ddd)  # DDD API for Subestacao
app.include_router(transformadores_router)  # DDD API
app.include_router(telhados_router)  # DDD API
app.include_router(satelite_router)