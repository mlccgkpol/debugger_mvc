"""
src/main.py

PulseMetrics — real-time analytics ingestion API.
Initialises the FastAPI application, registers middleware, and mounts routers.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time

from src.api.ingest import router as ingest_router
from src.api.query import router as query_router
from src.api.auth import router as auth_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
db     = Database()
cache  = CacheClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PulseMetrics starting up — initialising infrastructure.")
    await db.connect()
    await cache.connect()
    logger.info("Infrastructure ready. Accepting requests.")
    yield
    logger.info("PulseMetrics shutting down.")
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(
    title="PulseMetrics API",
    description="High-throughput real-time analytics ingestion and query service.",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://dashboard.internal"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    """Attach a request ID, measure latency, and log every response."""
    import uuid
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    start = time.perf_counter()

    logger.debug(
        f"[{request_id}] Incoming {request.method} {request.url.path} "
        f"from {request.client.host if request.client else 'unknown'}"
    )

    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000

    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(
        level,
        f"[{request_id}] {request.method} {request.url.path} "
        f"→ HTTP {response.status_code} in {duration_ms:.1f}ms"
    )
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "?")
    logger.error(
        f"[{rid}] Unhandled {type(exc).__name__} on {request.url.path}: {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "request_id": rid},
    )


app.include_router(ingest_router, prefix="/api/v2/ingest", tags=["ingest"])
app.include_router(query_router,  prefix="/api/v2/query",  tags=["query"])
app.include_router(auth_router,   prefix="/api/v2/auth",   tags=["auth"])


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "db": await db.ping(),
        "cache": await cache.ping(),
    }
