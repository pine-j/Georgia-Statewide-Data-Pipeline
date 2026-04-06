from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware

from app.core.settings import get_settings
from app.routers.analytics import router as analytics_router
from app.routers.geospatial import router as geospatial_router
from app.routers.layers import router as layers_router
from app.routers.meta import router as meta_router
from app.schemas import HealthResponse


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


@app.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(status="ok", environment=settings.app_env)


app.include_router(meta_router, prefix=settings.api_prefix)
app.include_router(analytics_router, prefix=settings.api_prefix)
app.include_router(layers_router, prefix=settings.api_prefix)
app.include_router(geospatial_router, prefix=settings.api_prefix)
