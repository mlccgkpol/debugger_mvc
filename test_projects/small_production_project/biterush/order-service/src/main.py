"""
order-service/src/main.py

BiteRush Order Service.
Manages order lifecycle: placement -> payment -> fulfillment -> delivery.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time
import uuid

from src.api.orders import router as orders_router
from src.api.cart import router as cart_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from src.consumers.payment_consumer import PaymentEventConsumer
from src.consumers.inventory_consumer import InventoryEventConsumer
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
db = Database()
cache = CacheClient()
producer = KafkaProducer()
payment_consumer = PaymentEventConsumer()
inventory_consumer = InventoryEventConsumer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("OrderService starting up - initialising infrastructure.")
    await db.connect()
    await cache.connect()
    await producer.start()
    asyncio.create_task(payment_consumer.run())
    asyncio.create_task(inventory_consumer.run())
    logger.info("OrderService ready. Accepting requests.")
    yield
    logger.info("OrderService shutting down.")
    await producer.stop()
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(
    title="BiteRush Order Service",
    description="Order lifecycle management for food & grocery delivery.",
    version="3.2.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.biterush.io", "https://partner.biterush.io"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    started_at = time.perf_counter()

    logger.debug(
        f"[{request_id}] Incoming {request.method} {request.url.path} "
        f"from {request.client.host if request.client else 'unknown'}"
    )

    response = await call_next(request)
    duration_ms = (time.perf_counter() - started_at) * 1000

    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(
        level,
        f"[{request_id}] {request.method} {request.url.path} "
        f"-> HTTP {response.status_code} in {duration_ms:.1f}ms",
    )
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "?")
    logger.error(
        f"[{request_id}] Unhandled {type(exc).__name__} on {request.url.path}: {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "request_id": request_id},
    )


app.include_router(orders_router, prefix="/api/v1/orders", tags=["orders"])
app.include_router(cart_router, prefix="/api/v1/cart", tags=["cart"])


@app.get("/health")
async def health():
    return {
        "service": "order-service",
        "db": await db.ping(),
        "cache": await cache.ping(),
    }
