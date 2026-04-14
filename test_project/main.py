"""
src/main.py

FastAPI application initialization, middleware configuration, and startup hooks.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time

from src.api.endpoints import router
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

app = FastAPI(
    title="DataBridge API",
    description="Internal service for aggregating and processing external data feeds.",
    version="1.4.2",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every incoming request and its response time."""
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        f"Request {request.method} {request.url.path} "
        f"completed in {duration_ms:.1f}ms → HTTP {response.status_code}"
    )
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all handler so unhandled exceptions return structured JSON."""
    logger.error(f"Unhandled exception on {request.url.path}: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


app.include_router(router, prefix="/api/v1")


@app.get("/health", tags=["ops"])
async def health_check() -> dict:
    """Liveness probe endpoint."""
    return {"status": "ok", "version": app.version}
