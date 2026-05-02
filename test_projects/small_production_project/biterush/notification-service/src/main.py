"""
notification-service/src/main.py

BiteRush Notification Service.
Sends SMS, email, and push notifications triggered by Kafka events.
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time, uuid

from src.consumers.order_consumer import OrderNotificationConsumer
from src.consumers.payment_consumer import PaymentNotificationConsumer
from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)
cache = CacheClient()
order_consumer = OrderNotificationConsumer()
payment_consumer = PaymentNotificationConsumer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NotificationService starting up.")
    await cache.connect()
    asyncio.create_task(order_consumer.run())
    asyncio.create_task(payment_consumer.run())
    logger.info("NotificationService ready.")
    yield
    logger.info("NotificationService shutting down.")
    await cache.disconnect()


app = FastAPI(title="BiteRush Notification Service", version="1.5.2", lifespan=lifespan)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    started_at = time.perf_counter()
    logger.debug(f"[{request_id}] Incoming {request.method} {request.url.path}")
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started_at) * 1000
    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(level, f"[{request_id}] {request.method} {request.url.path} -> HTTP {response.status_code} in {duration_ms:.1f}ms")
    return response


@app.get("/health")
async def health():
    return {"service": "notification-service", "cache": await cache.ping()}
