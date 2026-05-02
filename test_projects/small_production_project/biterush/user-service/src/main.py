"""
user-service/src/main.py

BiteRush User Service.
Handles registration, authentication (JWT), profile management, and addresses.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time, uuid

from src.api.users import router as users_router
from src.api.addresses import router as addresses_router
from src.api.auth import router as auth_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)
db = Database()
cache = CacheClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("UserService starting up.")
    await db.connect()
    await cache.connect()
    logger.info("UserService ready.")
    yield
    logger.info("UserService shutting down.")
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(title="BiteRush User Service", version="2.3.0", lifespan=lifespan)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    started_at = time.perf_counter()
    logger.debug(f"[{request_id}] Incoming {request.method} {request.url.path} from {request.client.host if request.client else 'unknown'}")
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started_at) * 1000
    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(level, f"[{request_id}] {request.method} {request.url.path} -> HTTP {response.status_code} in {duration_ms:.1f}ms")
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "?")
    logger.error(f"[{request_id}] Unhandled {type(exc).__name__}: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


app.include_router(auth_router, prefix="/api/v1/auth", tags=["auth"])
app.include_router(users_router, prefix="/api/v1/users", tags=["users"])
app.include_router(addresses_router, prefix="/api/v1/addresses", tags=["addresses"])


@app.get("/health")
async def health():
    return {"service": "user-service", "db": await db.ping(), "cache": await cache.ping()}
