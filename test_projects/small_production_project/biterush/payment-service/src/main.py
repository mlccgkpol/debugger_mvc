"""
payment-service/src/main.py

BiteRush Payment Service.
Handles payment initiation, webhook processing, and refunds.
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time, uuid

from src.api.payments import router as payments_router
from src.api.refunds import router as refunds_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from src.consumers.order_consumer import OrderPaymentConsumer
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
db = Database()
cache = CacheClient()
producer = KafkaProducer()
order_consumer = OrderPaymentConsumer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PaymentService starting up.")
    await db.connect()
    await cache.connect()
    await producer.start()
    asyncio.create_task(order_consumer.run())
    logger.info("PaymentService ready.")
    yield
    logger.info("PaymentService shutting down.")
    await producer.stop()
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(title="BiteRush Payment Service", version="1.8.0", lifespan=lifespan)


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


app.include_router(payments_router, prefix="/api/v1/payments", tags=["payments"])
app.include_router(refunds_router, prefix="/api/v1/refunds", tags=["refunds"])


@app.get("/health")
async def health():
    return {"service": "payment-service", "db": await db.ping(), "cache": await cache.ping()}
