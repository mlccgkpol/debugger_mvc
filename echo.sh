cat > ecommerce_generator.py << 'GENERATOR_EOF'
"""
ecommerce_generator.py

Generates the BiteRush food & grocery delivery platform source tree.

Layout produced
---------------
  ecommerce_generator.py
  biterush/
    shared/
      logger.py
      kafka_client.py
      redis_client.py
      settings.py
    order-service/
      src/
        main.py
        api/         orders.py  cart.py
        services/    order_service.py  cart_service.py
        consumers/   payment_consumer.py  inventory_consumer.py
        repositories/ order_repo.py
        models/      order_models.py
        infrastructure/ db.py  cache.py
    inventory-service/
      src/
        main.py
        api/         items.py  stock.py
        services/    inventory_service.py  reservation_service.py
        consumers/   order_consumer.py
        repositories/ item_repo.py  stock_repo.py
        models/      inventory_models.py
        infrastructure/ db.py  cache.py
    payment-service/
      src/
        main.py
        api/         payments.py  refunds.py
        services/    payment_service.py  refund_service.py
        consumers/   order_consumer.py
        repositories/ payment_repo.py
        models/      payment_models.py
        infrastructure/ db.py  cache.py
        gateway/     stripe_gateway.py  razorpay_gateway.py
    user-service/
      src/
        main.py
        api/         users.py  addresses.py  auth.py
        services/    user_service.py  auth_service.py  address_service.py
        repositories/ user_repo.py  address_repo.py
        models/      user_models.py
        infrastructure/ db.py  cache.py
    notification-service/
      src/
        main.py
        consumers/   order_consumer.py  payment_consumer.py
        services/    notification_service.py  template_service.py
        infrastructure/ cache.py
        senders/     sms_sender.py  email_sender.py  push_sender.py
  logs/
    2026-04-20.text
    2026-04-21.text
    2026-04-22.text
"""

from pathlib import Path

# ============================================================================
# SHARED MODULES
# ============================================================================

SHARED_LOGGER_PY = '''\
"""
shared/logger.py

Structured application logger for BiteRush services.

Format : YYYY-MM-DD_HH:MM:SS | LEVEL   | filename               | LNN  | message
Levels : DEBUG, INFO, WARNING, ERROR
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT = "%(asctime)s | %(levelname)-7s | %(filename)-22s | L%(lineno)-4d | %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"
_CHAIN_DIVIDER = "--- " * 22


def _log_path() -> Path:
    return LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.text"


class AppLogger:
    _LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.setLevel(logging.DEBUG)
            fmt = logging.Formatter(_FMT, datefmt=_DATE_FMT)

            file_handler = logging.FileHandler(_log_path(), encoding="utf-8")
            file_handler.setFormatter(fmt)

            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setFormatter(fmt)

            self._logger.addHandler(file_handler)
            self._logger.addHandler(stderr_handler)

    def debug(self, msg: str) -> None:
        self._logger.debug(msg)

    def info(self, msg: str) -> None:
        self._logger.info(msg)
        self._divider(msg)

    def warning(self, msg: str) -> None:
        self._logger.warning(msg)

    def error(self, msg: str) -> None:
        self._logger.error(msg)
        self._divider(msg)

    def log(self, level: str, msg: str) -> None:
        lvl = self._LEVELS.get(level.upper(), logging.INFO)
        self._logger.log(lvl, msg)
        if "-> HTTP" in msg:
            self._write_divider()

    def _divider(self, msg: str) -> None:
        if "-> HTTP" in msg:
            self._write_divider()

    def _write_divider(self) -> None:
        try:
            with open(_log_path(), "a", encoding="utf-8") as fh:
                fh.write(_CHAIN_DIVIDER + "\\n")
        except OSError:
            pass
'''

SHARED_KAFKA_CLIENT_PY = '''\
"""
shared/kafka_client.py

aiokafka producer / consumer wrappers for BiteRush services.
Each service instantiates its own producer; consumers run as background tasks.
"""

import asyncio
import json
import os
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from shared.logger import AppLogger

logger = AppLogger(__name__)

_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092")
_GROUP_PREFIX = os.getenv("KAFKA_GROUP_PREFIX", "biterush")


class KafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._producer = None
            cls._instance = inst
        return cls._instance

    async def start(self):
        if self._producer is not None:
            return
        logger.info(f"[kafka] Starting producer - brokers={_BROKERS}.")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            acks="all",
            enable_idempotence=True,
            max_batch_size=65536,
            linger_ms=5,
        )
        await self._producer.start()
        logger.info("[kafka] Producer ready.")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            self._producer = None
        logger.info("[kafka] Producer stopped.")

    async def publish(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer not started.")
        logger.debug(f"[kafka] Publishing to topic={topic} key={key}.")
        await self._producer.send_and_wait(
            topic,
            key=key.encode("utf-8"),
            value=payload,
        )
        logger.debug(f"[kafka] Published to topic={topic} key={key}.")


class KafkaConsumer:
    def __init__(self, topics: list[str], group_suffix: str):
        self._topics = topics
        self._group_id = f"{_GROUP_PREFIX}.{group_suffix}"
        self._consumer = None

    async def start(self, handler: Callable[[str, dict], Awaitable[None]]) -> None:
        logger.info(
            f"[kafka] Starting consumer group={self._group_id} topics={self._topics}."
        )
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=_BROKERS,
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=50,
        )
        await self._consumer.start()
        logger.info(f"[kafka] Consumer ready - group={self._group_id}.")

        try:
            async for msg in self._consumer:
                key = msg.key.decode("utf-8") if msg.key else "none"
                logger.debug(
                    f"[kafka] Received topic={msg.topic} partition={msg.partition} "
                    f"offset={msg.offset} key={key}."
                )
                try:
                    await handler(key, msg.value)
                    await self._consumer.commit()
                except Exception as exc:
                    logger.error(
                        f"[kafka] Handler failed for topic={msg.topic} key={key}: {exc}"
                    )
        except asyncio.CancelledError:
            logger.info(f"[kafka] Consumer cancelled - group={self._group_id}.")
        finally:
            await self._consumer.stop()
            logger.info(f"[kafka] Consumer stopped - group={self._group_id}.")
'''

SHARED_REDIS_CLIENT_PY = '''\
"""
shared/redis_client.py

Shared async Redis client (redis-py) for BiteRush services.
Each service uses this as a singleton scoped to the process.
"""

import json
import os
from typing import Any, Optional

import redis.asyncio as aioredis

from shared.logger import AppLogger

logger = AppLogger(__name__)

_HOST = os.getenv("REDIS_HOST", "redis-primary.biterush.svc.cluster.local")
_PORT = int(os.getenv("REDIS_PORT", "6379"))
_DB = int(os.getenv("REDIS_DB", "0"))
_DEFAULT_TTL = int(os.getenv("REDIS_DEFAULT_TTL", "300"))


class RedisClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._client = None
            cls._instance = inst
        return cls._instance

    async def connect(self):
        if self._client is not None:
            return
        logger.info(f"[redis] Connecting - {_HOST}:{_PORT}/{_DB}.")
        self._client = aioredis.Redis(
            host=_HOST, port=_PORT, db=_DB,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=2,
        )
        await self._client.ping()
        logger.info("[redis] Connection established.")

    async def disconnect(self):
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("[redis] Connection closed.")

    async def get(self, key: str) -> Optional[Any]:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        raw = await self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL) -> None:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        await self._client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str) -> None:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        await self._client.delete(key)

    async def incr(self, key: str, ttl: int = _DEFAULT_TTL) -> int:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        val = await self._client.incr(key)
        if val == 1:
            await self._client.expire(key, ttl)
        return val

    async def ping(self) -> str:
        if self._client is None:
            return "disconnected"
        try:
            await self._client.ping()
            return "ok"
        except Exception:
            return "error"
'''

SHARED_SETTINGS_PY = '''\
"""
shared/settings.py

Pydantic-settings based config loader for BiteRush microservices.
Each service imports and extends BaseServiceSettings.
"""

import os
from pydantic_settings import BaseSettings


class BaseServiceSettings(BaseSettings):
    service_name: str = "biterush-service"
    environment: str = os.getenv("ENVIRONMENT", "production")
    debug: bool = False

    db_dsn: str = ""
    db_pool_size: int = 10
    db_acquire_timeout: float = 3.0

    redis_host: str = "redis-primary.biterush.svc.cluster.local"
    redis_port: int = 6379

    kafka_brokers: str = "kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092"
    kafka_group_prefix: str = "biterush"

    jwt_secret: str = os.getenv("BITERUSH_JWT_SECRET", "biterush-prod-hs256-fallback")
    jwt_expiry_seconds: int = 7200

    internal_timeout: float = 5.0

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
'''

# ============================================================================
# ORDER SERVICE
# ============================================================================

ORDER_MAIN_PY = '''\
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
'''

ORDER_ORDERS_PY = '''\
"""
order-service/src/api/orders.py

Order placement, status lookup, and cancellation endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Path
from pydantic import BaseModel, Field

from src.services.order_service import OrderService
from src.services.cart_service import CartService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = OrderService()
cart_svc = CartService()


class PlaceOrderRequest(BaseModel):
    cart_id: str
    delivery_address_id: str
    payment_method: str = Field(..., pattern="^(card|wallet|cod)$")
    promo_code: Optional[str] = None
    delivery_instructions: Optional[str] = None


@router.post("")
async def place_order(
    body: PlaceOrderRequest,
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(
        f"[{rid}] Place order request - cart_id={body.cart_id} "
        f"method={body.payment_method}."
    )

    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")

    try:
        order = await svc.place_order(
            token=token,
            cart_id=body.cart_id,
            delivery_address_id=body.delivery_address_id,
            payment_method=body.payment_method,
            promo_code=body.promo_code,
            delivery_instructions=body.delivery_instructions,
        )
        logger.info(
            f"[{rid}] Order placed - order_id={order['order_id']} "
            f"total={order['total_amount']}."
        )
        return order
    except PermissionError as exc:
        logger.warning(f"[{rid}] Auth failed placing order: {exc}")
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        logger.warning(f"[{rid}] Validation failed: {exc}")
        raise HTTPException(status_code=422, detail=str(exc))
    except RuntimeError as exc:
        logger.error(f"[{rid}] Order placement failed: {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


@router.get("/{order_id}")
async def get_order(
    order_id: str = Path(...),
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(f"[{rid}] Get order request - order_id={order_id}.")

    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")

    try:
        order = await svc.get_order(order_id=order_id, token=token)
        if order is None:
            raise HTTPException(status_code=404, detail=f"Order {order_id} not found.")
        return order
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[{rid}] Failed to fetch order {order_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{order_id}/cancel")
async def cancel_order(
    order_id: str = Path(...),
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(f"[{rid}] Cancel order request - order_id={order_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")
    try:
        result = await svc.cancel_order(order_id=order_id, token=token)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.error(f"[{rid}] Cancel failed for order_id={order_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
'''

ORDER_CART_PY = '''\
"""
order-service/src/api/cart.py

Shopping cart endpoints: add items, remove items, get cart summary.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from src.services.cart_service import CartService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = CartService()


class CartItemRequest(BaseModel):
    item_id: str
    quantity: int = Field(..., ge=1, le=50)
    variant_id: Optional[str] = None


@router.post("/{cart_id}/items")
async def add_item(
    cart_id: str,
    body: CartItemRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(
        f"[cart] Add item - cart_id={cart_id} item_id={body.item_id} qty={body.quantity}."
    )
    token = (authorization or "").removeprefix("Bearer ").strip()
    try:
        return await svc.add_item(
            cart_id=cart_id,
            item_id=body.item_id,
            quantity=body.quantity,
            variant_id=body.variant_id,
            token=token,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.delete("/{cart_id}/items/{item_id}")
async def remove_item(
    cart_id: str,
    item_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(f"[cart] Remove item - cart_id={cart_id} item_id={item_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    return await svc.remove_item(cart_id=cart_id, item_id=item_id, token=token)


@router.get("/{cart_id}")
async def get_cart(
    cart_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(f"[cart] Get cart - cart_id={cart_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    result = await svc.get_cart(cart_id=cart_id, token=token)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Cart {cart_id} not found.")
    return result
'''

ORDER_SERVICE_PY = '''\
"""
order-service/src/services/order_service.py

Orchestrates the full order placement pipeline:
  validate token -> load cart -> check inventory ->
  calculate totals -> persist order -> publish order.created ->
  initiate payment -> update order status
"""

import uuid
import time
from decimal import Decimal
from typing import Any, Optional

import httpx

from src.infrastructure.cache import CacheClient
from src.repositories.order_repo import OrderRepository
from src.services.cart_service import CartService
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)

ORDER_REPO = OrderRepository()
CART_SVC = CartService()
CACHE = CacheClient()
PRODUCER = KafkaProducer()

INVENTORY_SERVICE_URL = "http://inventory-service.biterush.svc:8001"
USER_SERVICE_URL = "http://user-service.biterush.svc:8003"
PAYMENT_SERVICE_URL = "http://payment-service.biterush.svc:8002"

DELIVERY_FEE = Decimal("29.00")
GST_RATE = Decimal("0.05")


class OrderService:
    async def place_order(
        self,
        token: str,
        cart_id: str,
        delivery_address_id: str,
        payment_method: str,
        promo_code: Optional[str],
        delivery_instructions: Optional[str],
    ) -> dict[str, Any]:
        order_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
        logger.debug(f"[order] New placement attempt - order_id={order_id} cart_id={cart_id}.")

        logger.debug(f"[order] Validating user token - order_id={order_id}.")
        user = await self._validate_user(token)
        user_id = user["user_id"]
        logger.info(f"[order] User validated - user_id={user_id} order_id={order_id}.")

        logger.debug(f"[order] Loading cart - cart_id={cart_id}.")
        cart = await CART_SVC.get_cart(cart_id=cart_id, token=token)
        if cart is None or not cart.get("items"):
            raise ValueError(f"Cart {cart_id} is empty or not found.")
        logger.debug(f"[order] Cart loaded - {len(cart['items'])} items.")

        logger.debug(f"[order] Checking inventory - order_id={order_id}.")
        await self._check_inventory(cart["items"])
        logger.info(f"[order] Inventory confirmed available - order_id={order_id}.")

        logger.debug(f"[order] Validating delivery address - address_id={delivery_address_id}.")
        address = await self._validate_address(user_id, delivery_address_id, token)
        logger.debug(f"[order] Address validated - city={address.get('city')}.")

        subtotal = Decimal(str(cart["subtotal"]))
        discount = Decimal("0")
        if promo_code:
            discount = await self._apply_promo(promo_code, subtotal, user_id)
            logger.info(f"[order] Promo applied - code={promo_code} discount={discount}.")

        # BUG #1 [EASY]: GST is calculated on the subtotal BEFORE discount is applied.
        # Should be: gst = (subtotal - discount) * GST_RATE
        gst = subtotal * GST_RATE
        total = subtotal - discount + DELIVERY_FEE + gst
        logger.debug(
            f"[order] Totals - subtotal={subtotal} discount={discount} "
            f"gst={gst} delivery={DELIVERY_FEE} total={total}."
        )

        logger.info(f"[order] Persisting order record - order_id={order_id}.")
        await ORDER_REPO.create(
            order_id=order_id,
            user_id=user_id,
            cart_id=cart_id,
            items=cart["items"],
            subtotal=float(subtotal),
            discount=float(discount),
            gst=float(gst),
            delivery_fee=float(DELIVERY_FEE),
            total=float(total),
            delivery_address=address,
            payment_method=payment_method,
            delivery_instructions=delivery_instructions,
            status="pending_payment",
        )
        logger.info(f"[order] Order record created - order_id={order_id} status=pending_payment.")

        logger.debug(f"[order] Publishing order.created event - order_id={order_id}.")
        await PRODUCER.publish(
            topic="order.created",
            key=order_id,
            payload={
                "order_id": order_id,
                "user_id": user_id,
                "items": cart["items"],
                "total": float(total),
                "payment_method": payment_method,
                "created_at": int(time.time()),
            },
        )
        logger.info(f"[order] order.created published - order_id={order_id}.")

        logger.debug(f"[order] Initiating payment - order_id={order_id} method={payment_method}.")
        payment = await self._initiate_payment(order_id, float(total), payment_method, token)
        logger.info(
            f"[order] Payment initiated - order_id={order_id} "
            f"payment_id={payment.get('payment_id')} status={payment.get('status')}."
        )

        await ORDER_REPO.update_status(order_id, "awaiting_payment")
        logger.info(f"[order] Order status updated - order_id={order_id} status=awaiting_payment.")

        return {
            "order_id": order_id,
            "status": "awaiting_payment",
            "total_amount": float(total),
            "payment": payment,
            "estimated_delivery_minutes": 35,
        }

    async def get_order(self, order_id: str, token: str) -> Optional[dict]:
        cache_key = f"order:{order_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            logger.debug(f"[order] Cache hit - order_id={order_id}.")
            return cached
        order = await ORDER_REPO.get(order_id)
        if order:
            await CACHE.set(cache_key, order, ttl=60)
        return order

    async def cancel_order(self, order_id: str, token: str) -> dict:
        order = await ORDER_REPO.get(order_id)
        if order is None:
            raise ValueError(f"Order {order_id} not found.")
        cancellable = {"pending_payment", "awaiting_payment", "confirmed"}
        if order["status"] not in cancellable:
            raise ValueError(
                f"Order {order_id} cannot be cancelled - current status: {order['status']}."
            )
        await ORDER_REPO.update_status(order_id, "cancelled")
        await CACHE.delete(f"order:{order_id}")
        await PRODUCER.publish(
            topic="order.cancelled",
            key=order_id,
            payload={"order_id": order_id, "reason": "user_requested"},
        )
        logger.info(f"[order] Order cancelled - order_id={order_id}.")
        return {"order_id": order_id, "status": "cancelled"}

    async def _validate_user(self, token: str) -> dict:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{USER_SERVICE_URL}/api/v1/auth/me",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()

    async def _check_inventory(self, items: list[dict]) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{INVENTORY_SERVICE_URL}/api/v1/stock/check",
                json={"items": items},
            )
            resp.raise_for_status()
            data = resp.json()
            unavailable = [i["item_id"] for i in data.get("items", []) if not i["available"]]
            if unavailable:
                raise ValueError(f"Items out of stock: {unavailable}")

    async def _validate_address(self, user_id: str, address_id: str, token: str) -> dict:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{USER_SERVICE_URL}/api/v1/addresses/{address_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()

    async def _initiate_payment(
        self, order_id: str, amount: float, method: str, token: str
    ) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{PAYMENT_SERVICE_URL}/api/v1/payments/initiate",
                json={"order_id": order_id, "amount": amount, "method": method},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()

    async def _apply_promo(self, code: str, subtotal: Decimal, user_id: str) -> Decimal:
        PROMOS = {
            "FIRST10": Decimal("0.10"),
            "SAVE20": Decimal("0.20"),
            "FLAT50": None,
        }
        rate = PROMOS.get(code.upper())
        if rate is None and code.upper() != "FLAT50":
            logger.warning(f"[order] Unknown promo code={code}.")
            return Decimal("0")
        if code.upper() == "FLAT50":
            return Decimal("50")
        return (subtotal * rate).quantize(Decimal("0.01"))
'''

CART_SERVICE_PY = '''\
"""
order-service/src/services/cart_service.py

Cart management backed by Redis. Carts expire after 2 hours of inactivity.
Item prices are fetched from inventory-service and cached per-session.

BUG #2 [MEDIUM]: Race condition in add_item.
When two concurrent requests add items to the same cart, both do:
  cart = await CACHE.get(key)   # both read the same stale cart
  cart["items"].append(item)    # both modify their local copy
  await CACHE.set(key, cart)    # last writer wins; one update is silently lost.
No Redis lock or atomic operation is used.
"""

import time
from typing import Any, Optional

import httpx

from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)
CACHE = CacheClient()
CART_TTL = 7200  # 2 hours
INVENTORY_SERVICE_URL = "http://inventory-service.biterush.svc:8001"


class CartService:
    async def add_item(
        self,
        cart_id: str,
        item_id: str,
        quantity: int,
        variant_id: Optional[str],
        token: str,
    ) -> dict[str, Any]:
        logger.debug(f"[cart] add_item - cart_id={cart_id} item_id={item_id} qty={quantity}.")

        price_data = await self._fetch_item_price(item_id, variant_id)
        if not price_data.get("available"):
            raise ValueError(f"Item {item_id} is currently unavailable.")

        unit_price = price_data["price"]
        item_name = price_data["name"]

        # BUG #2 [MEDIUM]: No atomic lock — concurrent adds silently drop updates.
        cart = await CACHE.get(f"cart:{cart_id}") or {"cart_id": cart_id, "items": [], "created_at": int(time.time())}

        existing = next((i for i in cart["items"] if i["item_id"] == item_id and i.get("variant_id") == variant_id), None)
        if existing:
            existing["quantity"] += quantity
        else:
            cart["items"].append({
                "item_id": item_id,
                "variant_id": variant_id,
                "name": item_name,
                "unit_price": unit_price,
                "quantity": quantity,
            })

        cart["subtotal"] = sum(i["unit_price"] * i["quantity"] for i in cart["items"])
        cart["updated_at"] = int(time.time())
        await CACHE.set(f"cart:{cart_id}", cart, ttl=CART_TTL)

        logger.info(f"[cart] Item added - cart_id={cart_id} item_id={item_id} total_items={len(cart['items'])}.")
        return cart

    async def remove_item(self, cart_id: str, item_id: str, token: str) -> dict[str, Any]:
        cart = await CACHE.get(f"cart:{cart_id}")
        if cart is None:
            return {"cart_id": cart_id, "items": [], "subtotal": 0}
        cart["items"] = [i for i in cart["items"] if i["item_id"] != item_id]
        cart["subtotal"] = sum(i["unit_price"] * i["quantity"] for i in cart["items"])
        await CACHE.set(f"cart:{cart_id}", cart, ttl=CART_TTL)
        logger.info(f"[cart] Item removed - cart_id={cart_id} item_id={item_id}.")
        return cart

    async def get_cart(self, cart_id: str, token: str) -> Optional[dict[str, Any]]:
        cart = await CACHE.get(f"cart:{cart_id}")
        if cart is None:
            logger.debug(f"[cart] Cart not found - cart_id={cart_id}.")
        return cart

    async def _fetch_item_price(self, item_id: str, variant_id: Optional[str]) -> dict:
        params = {"variant_id": variant_id} if variant_id else {}
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"{INVENTORY_SERVICE_URL}/api/v1/items/{item_id}/price",
                params=params,
            )
            resp.raise_for_status()
            return resp.json()
'''

ORDER_REPO_PY = '''\
"""
order-service/src/repositories/order_repo.py

Postgres-backed persistence for order records.
"""

import json
from typing import Any, Optional

from src.infrastructure.db import Database, OperationalError
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class OrderRepository:
    async def create(self, order_id: str, **kwargs) -> None:
        logger.debug(f"[order_repo] Inserting order - order_id={order_id}.")
        conn = await _db.acquire()
        try:
            await conn.execute(
                """
                INSERT INTO orders (
                    order_id, user_id, cart_id, items, subtotal, discount,
                    gst, delivery_fee, total, delivery_address, payment_method,
                    delivery_instructions, status, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
                """,
                order_id,
                kwargs["user_id"],
                kwargs["cart_id"],
                json.dumps(kwargs["items"]),
                kwargs["subtotal"],
                kwargs["discount"],
                kwargs["gst"],
                kwargs["delivery_fee"],
                kwargs["total"],
                json.dumps(kwargs["delivery_address"]),
                kwargs["payment_method"],
                kwargs.get("delivery_instructions"),
                kwargs["status"],
            )
            logger.debug(f"[order_repo] Order inserted - order_id={order_id}.")
        finally:
            await _db.release(conn)

    async def get(self, order_id: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE order_id=$1", order_id
            )
            if row is None:
                return None
            d = dict(row)
            d["items"] = json.loads(d["items"]) if isinstance(d["items"], str) else d["items"]
            d["delivery_address"] = json.loads(d["delivery_address"]) if isinstance(d["delivery_address"], str) else d["delivery_address"]
            return d
        finally:
            await _db.release(conn)

    async def update_status(self, order_id: str, status: str) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "UPDATE orders SET status=$1, updated_at=NOW() WHERE order_id=$2",
                status, order_id,
            )
            logger.debug(f"[order_repo] Status updated - order_id={order_id} status={status}.")
        finally:
            await _db.release(conn)
'''

ORDER_PAYMENT_CONSUMER_PY = '''\
"""
order-service/src/consumers/payment_consumer.py

Listens to payment.completed and payment.failed topics.
Updates order status accordingly and triggers downstream events.

BUG #3 [HARD]: Offset is committed even when handler raises an exception.
In shared/kafka_client.py the consumer commits after every message regardless
of whether the handler succeeded. This consumer catches exceptions internally
but the base consumer class commits unconditionally after the handler returns.
Here the internal exception swallowing means failures appear to succeed —
order status is never updated for failed payment events, and the message
is committed so it is never retried.
"""

import asyncio

from src.repositories.order_repo import OrderRepository
from shared.kafka_client import KafkaConsumer
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
ORDER_REPO = OrderRepository()
PRODUCER = KafkaProducer()


class PaymentEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["payment.completed", "payment.failed"],
            group_suffix="order-service.payment",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        status = payload.get("status")
        logger.info(f"[order/payment-consumer] Received payment event - order_id={order_id} status={status}.")

        if status == "completed":
            try:
                await ORDER_REPO.update_status(order_id, "confirmed")
                logger.info(f"[order/payment-consumer] Order confirmed - order_id={order_id}.")
                await PRODUCER.publish(
                    topic="order.confirmed",
                    key=order_id,
                    payload={"order_id": order_id, "status": "confirmed"},
                )
            except Exception as exc:
                # BUG #3: swallowing exception here means commit happens anyway;
                # the order stuck in awaiting_payment is never retried.
                logger.error(
                    f"[order/payment-consumer] Failed to update confirmed order "
                    f"order_id={order_id}: {exc}"
                )

        elif status == "failed":
            try:
                await ORDER_REPO.update_status(order_id, "payment_failed")
                logger.info(f"[order/payment-consumer] Order marked payment_failed - order_id={order_id}.")
                await PRODUCER.publish(
                    topic="order.payment_failed",
                    key=order_id,
                    payload={"order_id": order_id, "reason": payload.get("reason")},
                )
            except Exception as exc:
                logger.error(
                    f"[order/payment-consumer] Failed to update failed order "
                    f"order_id={order_id}: {exc}"
                )
'''

ORDER_INVENTORY_CONSUMER_PY = '''\
"""
order-service/src/consumers/inventory_consumer.py

Listens to inventory.reserved and inventory.reservation_failed topics.
"""

from src.repositories.order_repo import OrderRepository
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
ORDER_REPO = OrderRepository()


class InventoryEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["inventory.reserved", "inventory.reservation_failed"],
            group_suffix="order-service.inventory",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        event_type = payload.get("event_type")
        logger.info(
            f"[order/inventory-consumer] Received inventory event - "
            f"order_id={order_id} type={event_type}."
        )
        if event_type == "reservation_failed":
            await ORDER_REPO.update_status(order_id, "cancelled")
            logger.info(
                f"[order/inventory-consumer] Order cancelled due to stock failure - "
                f"order_id={order_id}."
            )
'''

ORDER_DB_PY = '''\
"""
order-service/src/infrastructure/db.py

asyncpg connection pool for order-service PostgreSQL.
"""

import asyncio
import os
import asyncpg
from shared.logger import AppLogger

logger = AppLogger(__name__)
_DSN = os.getenv(
    "ORDER_DB_DSN",
    "postgresql://order_rw@pg-orders.biterush.svc.cluster.local:5432/biterush_orders",
)
_POOL_SIZE = int(os.getenv("ORDER_DB_POOL_SIZE", "10"))
_ACQUIRE_TIMEOUT = float(os.getenv("ORDER_DB_ACQUIRE_TIMEOUT", "3.0"))


class OperationalError(Exception):
    pass


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._pool = None
            inst._checked = 0
            cls._instance = inst
        return cls._instance

    async def connect(self):
        if self._pool:
            return
        logger.info(f"[db] Opening pool - dsn={_DSN!r} size={_POOL_SIZE}.")
        self._pool = await asyncpg.create_pool(dsn=_DSN, min_size=2, max_size=_POOL_SIZE, command_timeout=30)
        logger.info("[db] Pool ready.")

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
        logger.info("[db] Pool closed.")

    async def acquire(self) -> asyncpg.Connection:
        if not self._pool:
            raise OperationalError("Pool not initialised.")
        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT)
            self._checked += 1
            logger.debug(f"[db] Acquired - checked_out={self._checked}/{_POOL_SIZE}.")
            return conn
        except asyncio.TimeoutError as exc:
            raise OperationalError(f"Pool acquire timed out after {_ACQUIRE_TIMEOUT}s.") from exc

    async def release(self, conn):
        if self._pool and conn:
            await self._pool.release(conn)
        if self._checked > 0:
            self._checked -= 1

    async def ping(self) -> str:
        if not self._pool:
            return "disconnected"
        try:
            await self._pool.fetchval("SELECT 1")
            return "ok"
        except Exception:
            return "error"
'''

ORDER_CACHE_PY = '''\
"""
order-service/src/infrastructure/cache.py

Redis client wrapper for order-service.
"""

from shared.redis_client import RedisClient

CacheClient = RedisClient
'''

ORDER_MODELS_PY = '''\
"""
order-service/src/models/order_models.py

Pydantic response models for orders.
"""

from typing import Any, Optional
from pydantic import BaseModel


class OrderItem(BaseModel):
    item_id: str
    name: str
    unit_price: float
    quantity: int
    variant_id: Optional[str] = None


class OrderResponse(BaseModel):
    order_id: str
    user_id: str
    status: str
    items: list[OrderItem]
    subtotal: float
    discount: float
    gst: float
    delivery_fee: float
    total: float
    payment_method: str
    estimated_delivery_minutes: Optional[int] = None
'''

# ============================================================================
# INVENTORY SERVICE
# ============================================================================

INVENTORY_MAIN_PY = '''\
"""
inventory-service/src/main.py

BiteRush Inventory Service.
Manages grocery/food item catalogue, stock levels, and reservations.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time, uuid

from src.api.items import router as items_router
from src.api.stock import router as stock_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from src.consumers.order_consumer import OrderEventConsumer
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
db = Database()
cache = CacheClient()
producer = KafkaProducer()
order_consumer = OrderEventConsumer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("InventoryService starting up.")
    await db.connect()
    await cache.connect()
    await producer.start()
    asyncio.create_task(order_consumer.run())
    logger.info("InventoryService ready.")
    yield
    logger.info("InventoryService shutting down.")
    await producer.stop()
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(title="BiteRush Inventory Service", version="2.0.4", lifespan=lifespan)


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


app.include_router(items_router, prefix="/api/v1/items", tags=["items"])
app.include_router(stock_router, prefix="/api/v1/stock", tags=["stock"])


@app.get("/health")
async def health():
    return {"service": "inventory-service", "db": await db.ping(), "cache": await cache.ping()}
'''

INVENTORY_ITEMS_PY = '''\
"""
inventory-service/src/api/items.py

Item catalogue endpoints.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from src.services.inventory_service import InventoryService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = InventoryService()


@router.get("/{item_id}")
async def get_item(item_id: str) -> dict:
    item = await svc.get_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
    return item


@router.get("/{item_id}/price")
async def get_price(item_id: str, variant_id: Optional[str] = None) -> dict:
    price = await svc.get_item_price(item_id, variant_id)
    if price is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
    return price


@router.get("")
async def list_items(
    category: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    return await svc.list_items(category=category, page=page, limit=limit)
'''

INVENTORY_STOCK_PY = '''\
"""
inventory-service/src/api/stock.py

Stock check and reservation endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.reservation_service import ReservationService
from src.services.inventory_service import InventoryService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
inv_svc = InventoryService()
rsv_svc = ReservationService()


class StockCheckRequest(BaseModel):
    items: list[dict]


class ReserveRequest(BaseModel):
    order_id: str
    items: list[dict]


@router.post("/check")
async def check_stock(body: StockCheckRequest) -> dict:
    logger.info(f"[stock] Stock check for {len(body.items)} items.")
    results = await inv_svc.check_stock(body.items)
    return {"items": results}


@router.post("/reserve")
async def reserve_stock(body: ReserveRequest) -> dict:
    logger.info(f"[stock] Reserve request - order_id={body.order_id} items={len(body.items)}.")
    try:
        result = await rsv_svc.reserve(order_id=body.order_id, items=body.items)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/release")
async def release_stock(body: ReserveRequest) -> dict:
    logger.info(f"[stock] Release request - order_id={body.order_id}.")
    result = await rsv_svc.release(order_id=body.order_id, items=body.items)
    return result
'''

INVENTORY_SERVICE_PY = '''\
"""
inventory-service/src/services/inventory_service.py

Item catalogue and stock-level queries.
Results are cached in Redis; cache is busted on stock mutations.
"""

from typing import Any, Optional

from src.repositories.item_repo import ItemRepository
from src.repositories.stock_repo import StockRepository
from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)
ITEM_REPO = ItemRepository()
STOCK_REPO = StockRepository()
CACHE = CacheClient()


class InventoryService:
    async def get_item(self, item_id: str) -> Optional[dict[str, Any]]:
        cache_key = f"item:{item_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            logger.debug(f"[inventory] Cache hit - item_id={item_id}.")
            return cached
        item = await ITEM_REPO.get(item_id)
        if item:
            await CACHE.set(cache_key, item, ttl=600)
        return item

    async def get_item_price(self, item_id: str, variant_id: Optional[str]) -> Optional[dict]:
        item = await self.get_item(item_id)
        if item is None:
            return None
        price = item.get("price", 0)
        if variant_id:
            for v in item.get("variants", []):
                if v["variant_id"] == variant_id:
                    price = v.get("price", price)
                    break
        return {
            "item_id": item_id,
            "variant_id": variant_id,
            "name": item["name"],
            "price": price,
            "available": item.get("active", True),
        }

    async def check_stock(self, items: list[dict]) -> list[dict]:
        results = []
        for item in items:
            item_id = item["item_id"]
            qty = item.get("quantity", 1)
            stock = await STOCK_REPO.get_available(item_id)
            results.append({
                "item_id": item_id,
                "requested": qty,
                "available": stock is not None and stock >= qty,
                "stock": stock,
            })
        return results

    async def list_items(self, category: Optional[str], page: int, limit: int) -> dict:
        offset = (page - 1) * limit
        items = await ITEM_REPO.list(category=category, limit=limit, offset=offset)
        total = await ITEM_REPO.count(category=category)
        return {"items": items, "total": total, "page": page, "limit": limit}
'''

RESERVATION_SERVICE_PY = '''\
"""
inventory-service/src/services/reservation_service.py

Stock reservation management using Postgres advisory locks.

BUG #4 [HARD]: Double-decrement on concurrent reservations for the same item.
reserve() reads current stock, checks availability, then writes the new level
in three separate non-atomic DB operations with no row-level lock in between.
Two concurrent orders for the last 2 units of the same item can both pass
the availability check, both decrement, and drive stock negative.
The advisory lock is acquired per order_id, not per item_id — so concurrent
orders for different order_ids (but same item) are NOT serialised.
"""

import asyncio
from typing import Any

from src.repositories.stock_repo import StockRepository
from src.repositories.item_repo import ItemRepository
from src.infrastructure.db import Database
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
STOCK_REPO = StockRepository()
ITEM_REPO = ItemRepository()
PRODUCER = KafkaProducer()
_db = Database()


class ReservationService:
    async def reserve(self, order_id: str, items: list[dict]) -> dict[str, Any]:
        logger.info(f"[reservation] Reserving stock - order_id={order_id} items={len(items)}.")
        reserved = []
        failed = []

        conn = await _db.acquire()
        try:
            # BUG #4: Advisory lock is keyed on order_id hash, not item_id.
            # Two different orders for the same scarce item will both pass.
            lock_key = hash(order_id) & 0x7FFFFFFF
            await conn.execute(f"SELECT pg_advisory_lock({lock_key})")
            logger.debug(f"[reservation] Advisory lock acquired - order_id={order_id}.")

            for item in items:
                item_id = item["item_id"]
                qty = item.get("quantity", 1)

                # Non-atomic read-check-write:
                current = await STOCK_REPO.get_available(item_id)
                if current is None or current < qty:
                    logger.warning(
                        f"[reservation] Insufficient stock - item_id={item_id} "
                        f"available={current} requested={qty}."
                    )
                    failed.append(item_id)
                    continue

                await STOCK_REPO.decrement(item_id, qty)
                reserved.append({"item_id": item_id, "quantity": qty})
                logger.debug(
                    f"[reservation] Decremented - item_id={item_id} qty={qty} "
                    f"remaining={current - qty}."
                )

            await conn.execute(f"SELECT pg_advisory_unlock({lock_key})")
        finally:
            await _db.release(conn)

        if failed:
            # Roll back any already-decremented items
            for r in reserved:
                await STOCK_REPO.increment(r["item_id"], r["quantity"])
            logger.warning(
                f"[reservation] Reservation rolled back - order_id={order_id} "
                f"failed_items={failed}."
            )
            await PRODUCER.publish(
                topic="inventory.reservation_failed",
                key=order_id,
                payload={"order_id": order_id, "event_type": "reservation_failed", "failed_items": failed},
            )
            raise ValueError(f"Stock unavailable for items: {failed}")

        await PRODUCER.publish(
            topic="inventory.reserved",
            key=order_id,
            payload={"order_id": order_id, "event_type": "reserved", "items": reserved},
        )
        logger.info(f"[reservation] Stock reserved - order_id={order_id} items={reserved}.")
        return {"order_id": order_id, "reserved": reserved}

    async def release(self, order_id: str, items: list[dict]) -> dict:
        for item in items:
            await STOCK_REPO.increment(item["item_id"], item.get("quantity", 1))
        logger.info(f"[reservation] Stock released - order_id={order_id}.")
        return {"order_id": order_id, "released": items}
'''

INVENTORY_ORDER_CONSUMER_PY = '''\
"""
inventory-service/src/consumers/order_consumer.py

Listens to order.created to trigger stock reservation.
Listens to order.cancelled to release reserved stock.
"""

from src.services.reservation_service import ReservationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
RSV_SVC = ReservationService()


class OrderEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.created", "order.cancelled"],
            group_suffix="inventory-service.orders",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        topic_hint = payload.get("event_type") or ("created" if "items" in payload else "cancelled")
        order_id = payload.get("order_id")
        logger.info(f"[inventory/order-consumer] Event received - order_id={order_id} hint={topic_hint}.")

        if "items" in payload:
            try:
                await RSV_SVC.reserve(order_id=order_id, items=payload["items"])
            except ValueError as exc:
                logger.warning(f"[inventory/order-consumer] Reservation failed - order_id={order_id}: {exc}")
        else:
            await RSV_SVC.release(order_id=order_id, items=payload.get("items", []))
'''

ITEM_REPO_PY = '''\
"""
inventory-service/src/repositories/item_repo.py

Item catalogue data access.
"""

from typing import Any, Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class ItemRepository:
    async def get(self, item_id: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM items WHERE item_id=$1 AND active=TRUE", item_id
            )
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def list(self, category: Optional[str], limit: int, offset: int) -> list[dict]:
        conn = await _db.acquire()
        try:
            if category:
                rows = await conn.fetch(
                    "SELECT * FROM items WHERE category=$1 AND active=TRUE ORDER BY name LIMIT $2 OFFSET $3",
                    category, limit, offset,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM items WHERE active=TRUE ORDER BY name LIMIT $1 OFFSET $2",
                    limit, offset,
                )
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)

    async def count(self, category: Optional[str]) -> int:
        conn = await _db.acquire()
        try:
            if category:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM items WHERE category=$1 AND active=TRUE", category
                )
            return await conn.fetchval("SELECT COUNT(*) FROM items WHERE active=TRUE")
        finally:
            await _db.release(conn)
'''

STOCK_REPO_PY = '''\
"""
inventory-service/src/repositories/stock_repo.py

Stock level data access.
"""

from typing import Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class StockRepository:
    async def get_available(self, item_id: str) -> Optional[int]:
        conn = await _db.acquire()
        try:
            val = await conn.fetchval(
                "SELECT available_qty FROM stock WHERE item_id=$1", item_id
            )
            return val
        finally:
            await _db.release(conn)

    async def decrement(self, item_id: str, qty: int) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "UPDATE stock SET available_qty = available_qty - $1, updated_at=NOW() WHERE item_id=$2",
                qty, item_id,
            )
        finally:
            await _db.release(conn)

    async def increment(self, item_id: str, qty: int) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "UPDATE stock SET available_qty = available_qty + $1, updated_at=NOW() WHERE item_id=$2",
                qty, item_id,
            )
        finally:
            await _db.release(conn)
'''

INVENTORY_DB_PY = '''\
"""
inventory-service/src/infrastructure/db.py
asyncpg pool for inventory-service.
"""
import asyncio, os, asyncpg
from shared.logger import AppLogger
logger = AppLogger(__name__)
_DSN = os.getenv("INVENTORY_DB_DSN", "postgresql://inv_rw@pg-inventory.biterush.svc:5432/biterush_inventory")
_POOL_SIZE = int(os.getenv("INVENTORY_DB_POOL_SIZE", "10"))
_ACQUIRE_TIMEOUT = float(os.getenv("INVENTORY_DB_ACQUIRE_TIMEOUT", "3.0"))
class OperationalError(Exception): pass
class Database:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls); inst._pool = None; inst._checked = 0; cls._instance = inst
        return cls._instance
    async def connect(self):
        if self._pool: return
        logger.info(f"[db] Opening pool - dsn={_DSN!r} size={_POOL_SIZE}.")
        self._pool = await asyncpg.create_pool(dsn=_DSN, min_size=2, max_size=_POOL_SIZE, command_timeout=30)
        logger.info("[db] Pool ready.")
    async def disconnect(self):
        if self._pool: await self._pool.close(); self._pool = None
        logger.info("[db] Pool closed.")
    async def acquire(self):
        if not self._pool: raise OperationalError("Pool not initialised.")
        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT); self._checked += 1
            logger.debug(f"[db] Acquired - checked_out={self._checked}/{_POOL_SIZE}."); return conn
        except asyncio.TimeoutError as exc:
            raise OperationalError(f"Pool acquire timed out.") from exc
    async def release(self, conn):
        if self._pool and conn: await self._pool.release(conn)
        if self._checked > 0: self._checked -= 1
    async def ping(self):
        if not self._pool: return "disconnected"
        try: await self._pool.fetchval("SELECT 1"); return "ok"
        except: return "error"
'''

INVENTORY_CACHE_PY = '''\
"""inventory-service/src/infrastructure/cache.py"""
from shared.redis_client import RedisClient
CacheClient = RedisClient
'''

# ============================================================================
# PAYMENT SERVICE
# ============================================================================

PAYMENT_MAIN_PY = '''\
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
'''

PAYMENT_PAYMENTS_PY = '''\
"""
payment-service/src/api/payments.py

Payment initiation and webhook endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from src.services.payment_service import PaymentService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = PaymentService()


class InitiateRequest(BaseModel):
    order_id: str
    amount: float
    method: str


@router.post("/initiate")
async def initiate_payment(
    body: InitiateRequest,
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(
        f"[{rid}] Payment initiation - order_id={body.order_id} "
        f"amount={body.amount} method={body.method}."
    )
    try:
        result = await svc.initiate(
            order_id=body.order_id, amount=body.amount, method=body.method
        )
        logger.info(
            f"[{rid}] Payment initiated - payment_id={result['payment_id']} "
            f"order_id={body.order_id}."
        )
        return result
    except RuntimeError as exc:
        logger.error(f"[{rid}] Payment initiation failed - order_id={body.order_id}: {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


@router.post("/webhook/stripe")
async def stripe_webhook(request: Request) -> dict:
    body = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    logger.info(f"[webhook] Stripe webhook received - sig_present={bool(sig)}.")
    try:
        result = await svc.handle_stripe_webhook(raw_body=body, signature=sig)
        return result
    except ValueError as exc:
        logger.warning(f"[webhook] Stripe webhook rejected: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{payment_id}")
async def get_payment(payment_id: str) -> dict:
    pmt = await svc.get_payment(payment_id)
    if pmt is None:
        raise HTTPException(status_code=404, detail=f"Payment {payment_id} not found.")
    return pmt
'''

PAYMENT_REFUNDS_PY = '''\
"""
payment-service/src/api/refunds.py

Refund endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.refund_service import RefundService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = RefundService()


class RefundRequest(BaseModel):
    order_id: str
    amount: float
    reason: str


@router.post("")
async def initiate_refund(body: RefundRequest) -> dict:
    logger.info(
        f"[refund] Refund request - order_id={body.order_id} amount={body.amount}."
    )
    try:
        result = await svc.refund(
            order_id=body.order_id, amount=body.amount, reason=body.reason
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
'''

PAYMENT_SERVICE_PY = '''\
"""
payment-service/src/services/payment_service.py

Routes payments to Stripe (card) or Razorpay (wallet/upi).
Handles Stripe webhook signature verification and event processing.

BUG #5 [HARD]: Stripe webhook replay attack / double-processing.
handle_stripe_webhook() verifies the signature and extracts the event,
then checks whether the payment_id is already in COMPLETED state via the DB.
However it does NOT use an idempotency key or advisory lock before processing.
If Stripe delivers the same webhook twice in rapid succession (which it does on
network hiccups), two concurrent calls both pass the already-processed check
(because neither has committed yet), and both publish payment.completed —
resulting in double-credit of the customer\'s wallet or double-confirmation
of the order.
"""

import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Optional

from src.gateway.stripe_gateway import StripeGateway
from src.gateway.razorpay_gateway import RazorpayGateway
from src.repositories.payment_repo import PaymentRepository
from src.infrastructure.cache import CacheClient
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_test_biterush")
PAYMENT_REPO = PaymentRepository()
CACHE = CacheClient()
PRODUCER = KafkaProducer()
STRIPE = StripeGateway()
RAZORPAY = RazorpayGateway()


class PaymentService:
    async def initiate(self, order_id: str, amount: float, method: str) -> dict[str, Any]:
        payment_id = f"PAY-{str(uuid.uuid4())[:8].upper()}"
        logger.debug(f"[payment] Initiating - payment_id={payment_id} order_id={order_id} method={method}.")

        if method == "card":
            gateway_ref = await STRIPE.create_payment_intent(amount=amount, order_id=order_id)
        elif method in ("wallet", "upi"):
            gateway_ref = await RAZORPAY.create_order(amount=amount, order_id=order_id)
        elif method == "cod":
            gateway_ref = {"ref_id": f"COD-{order_id}", "status": "pending"}
        else:
            raise RuntimeError(f"Unknown payment method: {method}")

        await PAYMENT_REPO.create(
            payment_id=payment_id,
            order_id=order_id,
            amount=amount,
            method=method,
            gateway_ref=gateway_ref.get("ref_id"),
            status="pending",
        )
        logger.info(f"[payment] Payment record created - payment_id={payment_id} gateway_ref={gateway_ref.get('ref_id')}.")
        return {"payment_id": payment_id, "status": "pending", "gateway": gateway_ref}

    async def handle_stripe_webhook(self, raw_body: bytes, signature: str) -> dict:
        self._verify_stripe_signature(raw_body, signature)
        event = json.loads(raw_body)
        event_type = event.get("type")
        event_id = event.get("id")
        logger.info(f"[payment] Stripe webhook - event_type={event_type} event_id={event_id}.")

        if event_type == "payment_intent.succeeded":
            data = event["data"]["object"]
            order_id = data.get("metadata", {}).get("order_id")
            amount = data.get("amount_received", 0) / 100

            # BUG #5 [HARD]: No idempotency guard here — concurrent duplicate
            # webhooks both see status='pending' and both publish payment.completed.
            existing = await PAYMENT_REPO.get_by_order(order_id)
            if existing and existing["status"] == "completed":
                logger.info(f"[payment] Webhook already processed - order_id={order_id}.")
                return {"status": "already_processed"}

            await PAYMENT_REPO.update_by_order(order_id, status="completed", gateway_event_id=event_id)
            logger.info(f"[payment] Payment completed via webhook - order_id={order_id} amount={amount}.")

            await PRODUCER.publish(
                topic="payment.completed",
                key=order_id,
                payload={
                    "order_id": order_id,
                    "status": "completed",
                    "amount": amount,
                    "event_id": event_id,
                },
            )

        elif event_type == "payment_intent.payment_failed":
            data = event["data"]["object"]
            order_id = data.get("metadata", {}).get("order_id")
            reason = data.get("last_payment_error", {}).get("message", "unknown")
            await PAYMENT_REPO.update_by_order(order_id, status="failed", gateway_event_id=event_id)
            logger.warning(f"[payment] Payment failed via webhook - order_id={order_id} reason={reason}.")
            await PRODUCER.publish(
                topic="payment.failed",
                key=order_id,
                payload={"order_id": order_id, "status": "failed", "reason": reason},
            )

        return {"received": True}

    async def get_payment(self, payment_id: str) -> Optional[dict]:
        cache_key = f"payment:{payment_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            return cached
        pmt = await PAYMENT_REPO.get(payment_id)
        if pmt:
            await CACHE.set(cache_key, pmt, ttl=120)
        return pmt

    @staticmethod
    def _verify_stripe_signature(raw_body: bytes, signature: str) -> None:
        try:
            parts = dict(item.split("=", 1) for item in signature.split(","))
            timestamp = parts.get("t")
            sig_v1 = parts.get("v1")
        except Exception:
            raise ValueError("Malformed Stripe-Signature header.")

        if not timestamp or not sig_v1:
            raise ValueError("Missing timestamp or signature in Stripe header.")

        if abs(time.time() - int(timestamp)) > 300:
            raise ValueError("Stripe webhook timestamp too old.")

        payload_to_sign = f"{timestamp}.{raw_body.decode('utf-8')}"
        expected = hmac.new(
            STRIPE_WEBHOOK_SECRET.encode(),
            payload_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, sig_v1):
            raise ValueError("Stripe signature verification failed.")
'''

REFUND_SERVICE_PY = '''\
"""
payment-service/src/services/refund_service.py

Refund orchestration: validates refundability, calls gateway, records result.

BUG #6 [VERY DEEP]: Partial refund amount precision loss via float arithmetic.
Refund eligibility is checked by comparing payment.amount (stored as NUMERIC
in Postgres, returned as Python Decimal) against the requested refund amount
(a JSON float). The comparison uses Python == on Decimal vs float, which can
return False for amounts that are semantically equal (e.g., Decimal("199.90")
!= 199.9 due to float binary representation). This causes legitimate full
refunds to be incorrectly classified as partial refunds, which triggers the
partial-refund path in the Razorpay gateway. Razorpay\'s partial-refund API
requires the amount in paise (integer × 100), but the code passes the
floating-point amount directly without rounding, so e.g. 199.9 * 100 = 19989.999...
which is cast to int as 19989 instead of 19990 — the customer is under-refunded
by 1 paise every time. The root cause is three layers deep: JSON float → Decimal
comparison → paise conversion without rounding.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from src.gateway.razorpay_gateway import RazorpayGateway
from src.gateway.stripe_gateway import StripeGateway
from src.repositories.payment_repo import PaymentRepository
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
PAYMENT_REPO = PaymentRepository()
STRIPE = StripeGateway()
RAZORPAY = RazorpayGateway()
PRODUCER = KafkaProducer()


class RefundService:
    async def refund(self, order_id: str, amount: float, reason: str) -> dict[str, Any]:
        logger.debug(f"[refund] Processing refund - order_id={order_id} amount={amount}.")
        pmt = await PAYMENT_REPO.get_by_order(order_id)
        if pmt is None:
            raise ValueError(f"No payment found for order {order_id}.")
        if pmt["status"] != "completed":
            raise ValueError(
                f"Cannot refund order {order_id} - payment status is {pmt['status']}."
            )

        paid_amount = pmt["amount"]  # Decimal from Postgres NUMERIC column

        # BUG #6: float == Decimal comparison is unreliable.
        # e.g. Decimal("199.90") == 199.9 evaluates to False in Python.
        is_full_refund = paid_amount == amount

        logger.info(
            f"[refund] Refund type={'full' if is_full_refund else 'partial'} - "
            f"order_id={order_id} paid={paid_amount} requested={amount}."
        )

        if pmt["method"] == "card":
            gateway_ref = pmt.get("gateway_ref")
            result = await STRIPE.refund(
                payment_intent_id=gateway_ref,
                amount_paise=int(amount * 100),  # BUG #6: no rounding — 199.9 * 100 = 19989.999... -> 19989
                full=is_full_refund,
            )
        else:
            result = await RAZORPAY.refund(
                order_id=order_id,
                amount_paise=int(amount * 100),  # BUG #6 same here
                full=is_full_refund,
            )

        await PAYMENT_REPO.record_refund(order_id=order_id, amount=amount, reason=reason)
        logger.info(f"[refund] Refund recorded - order_id={order_id} gateway_result={result}.")

        await PRODUCER.publish(
            topic="payment.refunded",
            key=order_id,
            payload={"order_id": order_id, "amount": amount, "reason": reason},
        )
        return {"order_id": order_id, "refund_status": "processed", "amount": amount}
'''

PAYMENT_ORDER_CONSUMER_PY = '''\
"""
payment-service/src/consumers/order_consumer.py

Listens to order.cancelled to trigger refund if payment was already completed.
"""

from src.services.refund_service import RefundService
from src.repositories.payment_repo import PaymentRepository
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
PAYMENT_REPO = PaymentRepository()
REFUND_SVC = RefundService()


class OrderPaymentConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.cancelled"],
            group_suffix="payment-service.orders",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        logger.info(f"[payment/order-consumer] Order cancelled - order_id={order_id}.")
        pmt = await PAYMENT_REPO.get_by_order(order_id)
        if pmt and pmt["status"] == "completed":
            logger.info(f"[payment/order-consumer] Initiating auto-refund - order_id={order_id}.")
            try:
                await REFUND_SVC.refund(order_id=order_id, amount=float(pmt["amount"]), reason="order_cancelled")
            except Exception as exc:
                logger.error(f"[payment/order-consumer] Auto-refund failed - order_id={order_id}: {exc}")
'''

STRIPE_GATEWAY_PY = '''\
"""
payment-service/src/gateway/stripe_gateway.py

Stripe payment gateway integration via stripe-python SDK.
"""

import os
from typing import Any
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "sk_test_biterush")
STRIPE_BASE = "https://api.stripe.com/v1"


class StripeGateway:
    async def create_payment_intent(self, amount: float, order_id: str) -> dict[str, Any]:
        amount_paise = int(round(amount * 100))
        logger.debug(f"[stripe] Creating PaymentIntent - order_id={order_id} amount_paise={amount_paise}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{STRIPE_BASE}/payment_intents",
                auth=(STRIPE_SECRET_KEY, ""),
                data={
                    "amount": amount_paise,
                    "currency": "inr",
                    "metadata[order_id]": order_id,
                    "confirm": "false",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"[stripe] PaymentIntent created - id={data['id']} order_id={order_id}.")
            return {"ref_id": data["id"], "client_secret": data["client_secret"]}

    async def refund(self, payment_intent_id: str, amount_paise: int, full: bool) -> dict:
        logger.debug(f"[stripe] Refunding - pi={payment_intent_id} amount_paise={amount_paise} full={full}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            data = {"payment_intent": payment_intent_id}
            if not full:
                data["amount"] = str(amount_paise)
            resp = await client.post(
                f"{STRIPE_BASE}/refunds",
                auth=(STRIPE_SECRET_KEY, ""),
                data=data,
            )
            resp.raise_for_status()
            result = resp.json()
            logger.info(f"[stripe] Refund created - refund_id={result['id']}.")
            return {"refund_id": result["id"], "status": result["status"]}
'''

RAZORPAY_GATEWAY_PY = '''\
"""
payment-service/src/gateway/razorpay_gateway.py

Razorpay payment gateway for wallet/UPI payments.
"""

import os
from typing import Any
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)
RAZORPAY_KEY = os.getenv("RAZORPAY_KEY_ID", "rzp_test_biterush")
RAZORPAY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "rzp_secret_test")
RAZORPAY_BASE = "https://api.razorpay.com/v1"


class RazorpayGateway:
    async def create_order(self, amount: float, order_id: str) -> dict[str, Any]:
        amount_paise = int(round(amount * 100))
        logger.debug(f"[razorpay] Creating order - order_id={order_id} amount_paise={amount_paise}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{RAZORPAY_BASE}/orders",
                auth=(RAZORPAY_KEY, RAZORPAY_SECRET),
                json={"amount": amount_paise, "currency": "INR", "receipt": order_id},
            )
            resp.raise_for_status()
            data = resp.json()
            return {"ref_id": data["id"], "status": data["status"]}

    async def refund(self, order_id: str, amount_paise: int, full: bool) -> dict:
        logger.debug(f"[razorpay] Refund - order_id={order_id} amount_paise={amount_paise} full={full}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{RAZORPAY_BASE}/payments/{order_id}/refund",
                auth=(RAZORPAY_KEY, RAZORPAY_SECRET),
                json={"amount": amount_paise, "speed": "normal"},
            )
            resp.raise_for_status()
            return resp.json()
'''

PAYMENT_REPO_PY = '''\
"""
payment-service/src/repositories/payment_repo.py

Payment record persistence in PostgreSQL.
"""

from typing import Any, Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class PaymentRepository:
    async def create(self, payment_id: str, order_id: str, amount: float,
                     method: str, gateway_ref: Optional[str], status: str) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                """INSERT INTO payments (payment_id, order_id, amount, method, gateway_ref, status, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,NOW())""",
                payment_id, order_id, amount, method, gateway_ref, status,
            )
        finally:
            await _db.release(conn)

    async def get(self, payment_id: str) -> Optional[dict]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM payments WHERE payment_id=$1", payment_id)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def get_by_order(self, order_id: str) -> Optional[dict]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM payments WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1", order_id)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def update_by_order(self, order_id: str, status: str, gateway_event_id: Optional[str] = None) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "UPDATE payments SET status=$1, gateway_event_id=$2, updated_at=NOW() WHERE order_id=$3",
                status, gateway_event_id, order_id,
            )
        finally:
            await _db.release(conn)

    async def record_refund(self, order_id: str, amount: float, reason: str) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                """INSERT INTO refunds (order_id, amount, reason, created_at)
                   VALUES ($1,$2,$3,NOW())""",
                order_id, amount, reason,
            )
        finally:
            await _db.release(conn)
'''

PAYMENT_DB_PY = '''\
"""payment-service/src/infrastructure/db.py"""
import asyncio, os, asyncpg
from shared.logger import AppLogger
logger = AppLogger(__name__)
_DSN = os.getenv("PAYMENT_DB_DSN", "postgresql://pay_rw@pg-payments.biterush.svc:5432/biterush_payments")
_POOL_SIZE = int(os.getenv("PAYMENT_DB_POOL_SIZE", "10"))
_ACQUIRE_TIMEOUT = float(os.getenv("PAYMENT_DB_ACQUIRE_TIMEOUT", "3.0"))
class OperationalError(Exception): pass
class Database:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls); inst._pool = None; inst._checked = 0; cls._instance = inst
        return cls._instance
    async def connect(self):
        if self._pool: return
        logger.info(f"[db] Opening pool - dsn={_DSN!r} size={_POOL_SIZE}.")
        self._pool = await asyncpg.create_pool(dsn=_DSN, min_size=2, max_size=_POOL_SIZE, command_timeout=30)
        logger.info("[db] Pool ready.")
    async def disconnect(self):
        if self._pool: await self._pool.close(); self._pool = None
    async def acquire(self):
        if not self._pool: raise OperationalError("Pool not initialised.")
        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT); self._checked += 1; return conn
        except asyncio.TimeoutError as exc:
            raise OperationalError("Pool acquire timed out.") from exc
    async def release(self, conn):
        if self._pool and conn: await self._pool.release(conn)
        if self._checked > 0: self._checked -= 1
    async def ping(self):
        if not self._pool: return "disconnected"
        try: await self._pool.fetchval("SELECT 1"); return "ok"
        except: return "error"
'''

PAYMENT_CACHE_PY = '''\
"""payment-service/src/infrastructure/cache.py"""
from shared.redis_client import RedisClient
CacheClient = RedisClient
'''

# ============================================================================
# USER SERVICE
# ============================================================================

USER_MAIN_PY = '''\
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
'''

USER_AUTH_PY = '''\
"""
user-service/src/api/auth.py

Registration, login, token refresh, and /me endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, EmailStr

from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = AuthService()


class RegisterRequest(BaseModel):
    email: EmailStr
    phone: str
    name: str
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


@router.post("/register")
async def register(body: RegisterRequest) -> dict:
    logger.info(f"[auth] Register request - email={body.email}.")
    try:
        result = await svc.register(email=body.email, phone=body.phone, name=body.name, password=body.password)
        logger.info(f"[auth] User registered - user_id={result['user_id']}.")
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/login")
async def login(body: LoginRequest) -> dict:
    logger.info(f"[auth] Login request - email={body.email}.")
    try:
        result = await svc.login(email=body.email, password=body.password)
        logger.info(f"[auth] Login successful - user_id={result['user_id']}.")
        return result
    except PermissionError as exc:
        logger.warning(f"[auth] Login failed - email={body.email}: {exc}")
        raise HTTPException(status_code=401, detail=str(exc))


@router.get("/me")
async def me(authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing token.")
    try:
        user = await svc.me(token=token)
        return user
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
'''

USER_USERS_PY = '''\
"""
user-service/src/api/users.py

User profile endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.services.user_service import UserService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = UserService()


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None


@router.get("/{user_id}")
async def get_user(user_id: str, authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    user = await svc.get_user(user_id=user_id, token=token)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.patch("/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateProfileRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    try:
        return await svc.update_user(user_id=user_id, token=token, updates=body.model_dump(exclude_none=True))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
'''

USER_ADDRESSES_PY = '''\
"""
user-service/src/api/addresses.py

User delivery address management.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.services.address_service import AddressService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = AddressService()


class AddressRequest(BaseModel):
    label: str
    line1: str
    line2: Optional[str] = None
    city: str
    state: str
    pincode: str
    lat: Optional[float] = None
    lng: Optional[float] = None


@router.post("")
async def add_address(
    body: AddressRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    result = await svc.add_address(token=token, **body.model_dump())
    return result


@router.get("/{address_id}")
async def get_address(
    address_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    addr = await svc.get_address(address_id=address_id, token=token)
    if addr is None:
        raise HTTPException(status_code=404, detail=f"Address {address_id} not found.")
    return addr


@router.get("")
async def list_addresses(authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    addresses = await svc.list_addresses(token=token)
    return {"addresses": addresses}
'''

USER_AUTH_SERVICE_PY = '''\
"""
user-service/src/services/auth_service.py

JWT-based authentication for BiteRush users.
Tokens are HMAC-SHA256 signed; refresh tokens stored in Redis.

BUG #7 [VERY DEEP]: Timing-safe comparison bypass via token length oracle.
validate_token() first checks len(parts) == 3 before doing any HMAC
verification. An attacker who can measure response time can distinguish
"malformed token" (fast, raises before HMAC) from "valid structure but
bad sig" (slower, runs HMAC). More critically, the refresh token stored
in Redis is looked up by user_id extracted from the (already-signature-
verified) payload — but me() calls validate_token() first and caches the
result in Redis under key "user:me:{token}" WITHOUT verifying that the
token\'s user_id matches the requesting user. If a valid token for user A
is used to call /me, the result is cached as "user:me:{tokenA}". Another
process that somehow obtains tokenA (e.g., from a log line, since the
token is logged at DEBUG level in auth.py line 22 via the Header display)
can retrieve user A\'s profile. The deeper bug is that the raw token value
is used as a cache key, which means cache poisoning is possible if the
token namespace is shared with other data, and that tokens are effectively
leaked via log files in any environment with DEBUG logging enabled.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Optional

from src.repositories.user_repo import UserRepository
from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)

_SECRET = os.getenv("BITERUSH_JWT_SECRET", "biterush-prod-hs256-fallback")
_TTL = 7200

USER_REPO = UserRepository()
CACHE = CacheClient()


class AuthService:
    async def register(self, email: str, phone: str, name: str, password: str) -> dict:
        existing = await USER_REPO.get_by_email(email)
        if existing:
            raise ValueError(f"Email {email} already registered.")
        import bcrypt
        pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = f"USR-{str(uuid.uuid4())[:8].upper()}"
        await USER_REPO.create(user_id=user_id, email=email, phone=phone, name=name, pw_hash=pw_hash)
        token = self._issue_token(user_id)
        return {"user_id": user_id, "access_token": token, "token_type": "bearer"}

    async def login(self, email: str, password: str) -> dict:
        import bcrypt
        user = await USER_REPO.get_by_email(email)
        if user is None:
            raise PermissionError("Invalid email or password.")
        if not bcrypt.checkpw(password.encode(), user["pw_hash"].encode()):
            raise PermissionError("Invalid email or password.")
        token = self._issue_token(user["user_id"])
        logger.info(f"[auth_svc] Token issued - user_id={user['user_id']}.")
        return {"user_id": user["user_id"], "access_token": token, "token_type": "bearer"}

    async def me(self, token: str) -> dict:
        # BUG #7: raw token used as cache key — token leak via logs or shared cache
        # means an attacker can retrieve another user\'s profile.
        cache_key = f"user:me:{token}"
        cached = await CACHE.get(cache_key)
        if cached:
            logger.debug(f"[auth_svc] /me cache hit - user_id={cached.get('user_id')}.")
            return cached

        user_id = self.validate_token(token)
        user = await USER_REPO.get(user_id)
        if user is None:
            raise ValueError(f"User {user_id} not found.")
        profile = {k: v for k, v in user.items() if k != "pw_hash"}
        await CACHE.set(cache_key, profile, ttl=_TTL)
        return profile

    def validate_token(self, token: str) -> str:
        logger.debug(f"[auth_svc] Validating token (len={len(token)}).")
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError(f"Malformed token: expected 3 parts, got {len(parts)}.")
        header_b64, payload_b64, sig_b64 = parts
        expected_sig = hmac.new(
            _SECRET.encode(), f"{header_b64}.{payload_b64}".encode(), hashlib.sha256
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")
        if not hmac.compare_digest(sig_b64, expected_b64):
            raise ValueError("Token signature invalid.")
        padding = "=" * (-len(payload_b64) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        if claims.get("exp", 0) < time.time():
            raise ValueError("Token expired.")
        return claims["sub"]

    def _issue_token(self, user_id: str) -> str:
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({"sub": user_id, "iat": int(time.time()), "exp": int(time.time()) + _TTL}).encode()
        ).decode().rstrip("=")
        sig = hmac.new(_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
        return f"{header}.{payload}.{base64.urlsafe_b64encode(sig).decode().rstrip('=')}"
'''

USER_SERVICE_PY = '''\
"""
user-service/src/services/user_service.py

User profile management.
"""

from typing import Any, Optional

from src.repositories.user_repo import UserRepository
from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
USER_REPO = UserRepository()
AUTH_SVC = AuthService()


class UserService:
    async def get_user(self, user_id: str, token: str) -> Optional[dict[str, Any]]:
        requesting_user_id = AUTH_SVC.validate_token(token)
        user = await USER_REPO.get(user_id)
        if user is None:
            return None
        return {k: v for k, v in user.items() if k != "pw_hash"}

    async def update_user(self, user_id: str, token: str, updates: dict) -> dict:
        requesting_user_id = AUTH_SVC.validate_token(token)
        if requesting_user_id != user_id:
            raise PermissionError("Cannot update another user\'s profile.")
        await USER_REPO.update(user_id=user_id, updates=updates)
        user = await USER_REPO.get(user_id)
        return {k: v for k, v in user.items() if k != "pw_hash"}
'''

ADDRESS_SERVICE_PY = '''\
"""
user-service/src/services/address_service.py

Delivery address management.

BUG #8 [HARD]: IDOR (Insecure Direct Object Reference) in get_address.
get_address() fetches the address by address_id from the DB but does NOT
verify that the address belongs to the requesting user. Any authenticated
user can retrieve any other user\'s delivery address by guessing/enumerating
address IDs, since IDs are sequential integers in Postgres. This is a
textbook IDOR — the authorization check only verifies that the user has
a valid token, not that the resource belongs to them.
"""

from typing import Any, Optional

from src.repositories.address_repo import AddressRepository
from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
ADDR_REPO = AddressRepository()
AUTH_SVC = AuthService()


class AddressService:
    async def add_address(self, token: str, **kwargs) -> dict[str, Any]:
        user_id = AUTH_SVC.validate_token(token)
        address_id = await ADDR_REPO.create(user_id=user_id, **kwargs)
        logger.info(f"[address] Address added - user_id={user_id} address_id={address_id}.")
        return {"address_id": address_id, "user_id": user_id, **kwargs}

    async def get_address(self, address_id: str, token: str) -> Optional[dict[str, Any]]:
        # BUG #8: token is validated (user is authenticated) but the returned
        # address is NOT checked against the user\'s own user_id.
        AUTH_SVC.validate_token(token)  # only checks auth, not ownership
        addr = await ADDR_REPO.get(address_id)
        logger.debug(f"[address] Fetched address - address_id={address_id}.")
        return addr  # could be any user\'s address

    async def list_addresses(self, token: str) -> list[dict[str, Any]]:
        user_id = AUTH_SVC.validate_token(token)
        return await ADDR_REPO.list_by_user(user_id)
'''

USER_REPO_PY = '''\
"""
user-service/src/repositories/user_repo.py

User record persistence.
"""

from typing import Any, Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class UserRepository:
    async def create(self, user_id: str, email: str, phone: str, name: str, pw_hash: str) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "INSERT INTO users (user_id, email, phone, name, pw_hash, created_at) VALUES ($1,$2,$3,$4,$5,NOW())",
                user_id, email, phone, name, pw_hash,
            )
        finally:
            await _db.release(conn)

    async def get(self, user_id: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def get_by_email(self, email: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM users WHERE email=$1", email)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def update(self, user_id: str, updates: dict) -> None:
        if not updates:
            return
        set_clauses = ", ".join(f"{k}=${i+2}" for i, k in enumerate(updates.keys()))
        values = list(updates.values())
        conn = await _db.acquire()
        try:
            await conn.execute(
                f"UPDATE users SET {set_clauses}, updated_at=NOW() WHERE user_id=$1",
                user_id, *values,
            )
        finally:
            await _db.release(conn)
'''

ADDRESS_REPO_PY = '''\
"""
user-service/src/repositories/address_repo.py

Delivery address persistence.
"""

from typing import Any, Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class AddressRepository:
    async def create(self, user_id: str, label: str, line1: str, line2: Optional[str],
                     city: str, state: str, pincode: str, lat: Optional[float], lng: Optional[float]) -> str:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow(
                """INSERT INTO addresses (user_id, label, line1, line2, city, state, pincode, lat, lng, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) RETURNING address_id""",
                user_id, label, line1, line2, city, state, pincode, lat, lng,
            )
            return str(row["address_id"])
        finally:
            await _db.release(conn)

    async def get(self, address_id: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM addresses WHERE address_id=$1", address_id)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def list_by_user(self, user_id: str) -> list[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            rows = await conn.fetch("SELECT * FROM addresses WHERE user_id=$1 ORDER BY created_at DESC", user_id)
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)
'''

USER_DB_PY = '''\
"""user-service/src/infrastructure/db.py"""
import asyncio, os, asyncpg
from shared.logger import AppLogger
logger = AppLogger(__name__)
_DSN = os.getenv("USER_DB_DSN", "postgresql://user_rw@pg-users.biterush.svc:5432/biterush_users")
_POOL_SIZE = int(os.getenv("USER_DB_POOL_SIZE", "10"))
_ACQUIRE_TIMEOUT = float(os.getenv("USER_DB_ACQUIRE_TIMEOUT", "3.0"))
class OperationalError(Exception): pass
class Database:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls); inst._pool = None; inst._checked = 0; cls._instance = inst
        return cls._instance
    async def connect(self):
        if self._pool: return
        logger.info(f"[db] Opening pool - dsn={_DSN!r} size={_POOL_SIZE}.")
        self._pool = await asyncpg.create_pool(dsn=_DSN, min_size=2, max_size=_POOL_SIZE, command_timeout=30)
        logger.info("[db] Pool ready.")
    async def disconnect(self):
        if self._pool: await self._pool.close(); self._pool = None
    async def acquire(self):
        if not self._pool: raise OperationalError("Pool not initialised.")
        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT); self._checked += 1; return conn
        except asyncio.TimeoutError as exc:
            raise OperationalError("Pool acquire timed out.") from exc
    async def release(self, conn):
        if self._pool and conn: await self._pool.release(conn)
        if self._checked > 0: self._checked -= 1
    async def ping(self):
        if not self._pool: return "disconnected"
        try: await self._pool.fetchval("SELECT 1"); return "ok"
        except: return "error"
'''

USER_CACHE_PY = '''\
"""user-service/src/infrastructure/cache.py"""
from shared.redis_client import RedisClient
CacheClient = RedisClient
'''

# ============================================================================
# NOTIFICATION SERVICE
# ============================================================================

NOTIFICATION_MAIN_PY = '''\
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
'''

NOTIFICATION_ORDER_CONSUMER_PY = '''\
"""
notification-service/src/consumers/order_consumer.py

Handles order lifecycle events for customer notifications.

BUG #9 [VERY DEEP]: Goroutine / task leak causing duplicate notifications.
run() creates a new asyncio.Task for every invocation. If the Kafka consumer
reconnects after a broker failure, run() is called again from the lifespan
startup path — but the OLD task is never cancelled. Both tasks consume from
the same consumer group, but because each holds a separate AIOKafkaConsumer
instance with the same group_id, Kafka triggers a rebalance. During the
rebalance window (up to 30s), BOTH consumers may see the same partition and
process the same messages, resulting in duplicate SMS/email sends.
The root bug: there is no guard to prevent re-running, and the Task handle
is discarded so it can never be cancelled.
"""

import asyncio
from typing import Optional

from src.services.notification_service import NotificationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
NOTIF_SVC = NotificationService()


class OrderNotificationConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.created", "order.confirmed", "order.cancelled", "order.payment_failed"],
            group_suffix="notification-service.orders",
        )
        # BUG #9: _task is stored but never checked before creating a new one on re-run.
        self._task: Optional[asyncio.Task] = None

    async def run(self):
        # BUG #9: if self._task is already running (reconnect scenario),
        # we silently create a SECOND consumer — duplicate notifications guaranteed.
        self._task = asyncio.create_task(self._consumer.start(self._handle))

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        event_type = self._infer_event(payload)
        logger.info(
            f"[notif/order-consumer] Processing event - order_id={order_id} type={event_type}."
        )

        templates = {
            "created": ("order_placed", "sms_email"),
            "confirmed": ("order_confirmed", "sms_push"),
            "cancelled": ("order_cancelled", "sms_email"),
            "payment_failed": ("payment_failed", "sms_push"),
        }
        tmpl, channels = templates.get(event_type, (None, None))
        if tmpl is None:
            logger.warning(f"[notif/order-consumer] No template for event={event_type}.")
            return

        user_id = payload.get("user_id") or await NOTIF_SVC.resolve_user(order_id)
        await NOTIF_SVC.send(user_id=user_id, template=tmpl, channels=channels, context=payload)

    def _infer_event(self, payload: dict) -> str:
        if "items" in payload and "payment_method" in payload:
            return "created"
        if payload.get("status") == "confirmed":
            return "confirmed"
        if payload.get("status") == "cancelled" or payload.get("reason") == "user_requested":
            return "cancelled"
        return "payment_failed"
'''

NOTIFICATION_PAYMENT_CONSUMER_PY = '''\
"""
notification-service/src/consumers/payment_consumer.py

Handles payment events for receipt and refund notifications.
"""

from src.services.notification_service import NotificationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
NOTIF_SVC = NotificationService()


class PaymentNotificationConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["payment.completed", "payment.failed", "payment.refunded"],
            group_suffix="notification-service.payments",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        status = payload.get("status") or ("refunded" if "amount" in payload and "reason" in payload else "unknown")
        logger.info(f"[notif/payment-consumer] Payment event - order_id={order_id} status={status}.")

        templates = {
            "completed": ("payment_receipt", "email_push"),
            "failed": ("payment_failed", "sms_push"),
            "refunded": ("refund_initiated", "sms_email"),
        }
        tmpl, channels = templates.get(status, (None, None))
        if tmpl is None:
            return

        user_id = await NOTIF_SVC.resolve_user(order_id)
        await NOTIF_SVC.send(user_id=user_id, template=tmpl, channels=channels, context=payload)
'''

NOTIFICATION_SERVICE_PY = '''\
"""
notification-service/src/services/notification_service.py

Dispatches notifications via SMS, email, and push channels.

BUG #10 [VERY DEEP]: Thundering herd on resolve_user() cache miss.
resolve_user() checks Redis for a cached user_id->profile mapping.
On a cache miss it calls the user-service HTTP endpoint. If 200 pending
notifications all miss cache simultaneously (e.g., after a Redis restart
or first-time deployment), all 200 coroutines concurrently call
user-service for the same small set of user_ids. The user-service DB
pool (size=10) is exhausted, causing cascading 503s. Each 503 is then
retried by the notification service (see the retry loop) — up to 3 times
— resulting in up to 600 concurrent requests to a service that can handle
~10. No stampede protection (Redis lock, singleflight pattern) is used.
The retry backoff is also broken: it uses `await asyncio.sleep(attempt)`
where attempt starts at 0, so the first retry has zero delay.
"""

import asyncio
from typing import Optional

import httpx

from src.infrastructure.cache import CacheClient
from src.senders.sms_sender import SmsSender
from src.senders.email_sender import EmailSender
from src.senders.push_sender import PushSender
from shared.logger import AppLogger

logger = AppLogger(__name__)
CACHE = CacheClient()
SMS = SmsSender()
EMAIL = EmailSender()
PUSH = PushSender()

USER_SERVICE_URL = "http://user-service.biterush.svc:8003"
ORDER_SERVICE_URL = "http://order-service.biterush.svc:8000"


class NotificationService:
    async def resolve_user(self, order_id: str) -> Optional[str]:
        cache_key = f"notif:user_for_order:{order_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            return cached

        # BUG #10: No stampede protection. All concurrent misses hammer user-service.
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}")
                    resp.raise_for_status()
                    user_id = resp.json().get("user_id")
                    await CACHE.set(cache_key, user_id, ttl=3600)
                    return user_id
            except Exception as exc:
                logger.warning(
                    f"[notif] resolve_user failed attempt={attempt} order_id={order_id}: {exc}"
                )
                # BUG #10: attempt=0 on first retry -> sleep(0) -> immediate retry, no backoff.
                await asyncio.sleep(attempt)
        logger.error(f"[notif] Could not resolve user_id for order_id={order_id}.")
        return None

    async def send(self, user_id: Optional[str], template: str, channels: str, context: dict) -> None:
        if user_id is None:
            logger.warning(f"[notif] Cannot send {template} - user_id is None.")
            return

        profile_key = f"notif:profile:{user_id}"
        profile = await CACHE.get(profile_key)
        if profile is None:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{USER_SERVICE_URL}/api/v1/users/{user_id}")
                if resp.status_code == 200:
                    profile = resp.json()
                    await CACHE.set(profile_key, profile, ttl=1800)

        if profile is None:
            logger.error(f"[notif] User profile not found - user_id={user_id}.")
            return

        logger.info(
            f"[notif] Sending notification - user_id={user_id} template={template} "
            f"channels={channels}."
        )

        if "sms" in channels and profile.get("phone"):
            try:
                await SMS.send(phone=profile["phone"], template=template, context=context)
                logger.info(f"[notif] SMS sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] SMS failed - user_id={user_id}: {exc}")

        if "email" in channels and profile.get("email"):
            try:
                await EMAIL.send(email=profile["email"], template=template, context=context)
                logger.info(f"[notif] Email sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] Email failed - user_id={user_id}: {exc}")

        if "push" in channels and profile.get("device_token"):
            try:
                await PUSH.send(device_token=profile["device_token"], template=template, context=context)
                logger.info(f"[notif] Push sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] Push failed - user_id={user_id}: {exc}")
'''

TEMPLATE_SERVICE_PY = '''\
"""
notification-service/src/services/template_service.py

Notification template resolution and rendering.
"""

TEMPLATES = {
    "order_placed": {
        "sms": "Hi {name}! Your BiteRush order #{order_id} has been placed. Total: ₹{total}.",
        "email_subject": "Order Placed - #{order_id}",
        "email_body": "Dear {name},\\n\\nYour order #{order_id} has been placed successfully.\\nTotal: ₹{total}\\nEstimated delivery: 35 minutes.\\n\\nThank you,\\nBiteRush",
        "push_title": "Order Placed!",
        "push_body": "Your order #{order_id} is confirmed. Delivery in ~35 mins.",
    },
    "order_confirmed": {
        "sms": "Great news, {name}! Order #{order_id} confirmed by the restaurant.",
        "push_title": "Order Confirmed!",
        "push_body": "Restaurant accepted #{order_id}. Getting it ready!",
    },
    "order_cancelled": {
        "sms": "Order #{order_id} has been cancelled. Refund will be processed in 5-7 days.",
        "email_subject": "Order Cancelled - #{order_id}",
        "email_body": "Hi {name},\\n\\nYour order #{order_id} has been cancelled.\\nAny charges will be refunded in 5-7 business days.\\n\\nBiteRush Support",
    },
    "payment_receipt": {
        "email_subject": "Payment Receipt - ₹{amount}",
        "email_body": "Hi {name},\\n\\nPayment of ₹{amount} received for order #{order_id}.\\n\\nBiteRush",
        "push_title": "Payment Confirmed",
        "push_body": "₹{amount} paid for order #{order_id}.",
    },
    "refund_initiated": {
        "sms": "Refund of ₹{amount} for order #{order_id} is being processed.",
        "email_subject": "Refund Initiated - ₹{amount}",
        "email_body": "Hi {name},\\n\\nYour refund of ₹{amount} for order #{order_id} has been initiated. Expected in 5-7 days.\\n\\nBiteRush",
    },
}


class TemplateService:
    def render(self, template_name: str, channel: str, context: dict) -> str:
        tmpl = TEMPLATES.get(template_name, {})
        key = channel
        text = tmpl.get(key, "")
        try:
            return text.format(**context)
        except KeyError:
            return text
'''

SMS_SENDER_PY = '''\
"""
notification-service/src/senders/sms_sender.py

SMS delivery via Exotel (India-primary) with Twilio fallback.
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

EXOTEL_SID = os.getenv("EXOTEL_SID", "biterush_sid")
EXOTEL_TOKEN = os.getenv("EXOTEL_TOKEN", "exotel_token_test")
EXOTEL_URL = f"https://api.exotel.com/v1/Accounts/{EXOTEL_SID}/Sms/send"


class SmsSender:
    async def send(self, phone: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        body = TemplateService().render(template, "sms", context)
        logger.debug(f"[sms] Sending to={phone} template={template}.")
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                EXOTEL_URL,
                auth=(EXOTEL_SID, EXOTEL_TOKEN),
                data={"From": "BiteRush", "To": phone, "Body": body},
            )
            resp.raise_for_status()
            logger.debug(f"[sms] Delivered - phone={phone} status={resp.status_code}.")
'''

EMAIL_SENDER_PY = '''\
"""
notification-service/src/senders/email_sender.py

Email delivery via SendGrid.
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

SENDGRID_KEY = os.getenv("SENDGRID_API_KEY", "SG.test_biterush")
SENDGRID_FROM = os.getenv("SENDGRID_FROM", "noreply@biterush.io")
SENDGRID_URL = "https://api.sendgrid.com/v3/mail/send"


class EmailSender:
    async def send(self, email: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        svc = TemplateService()
        subject = svc.render(template, "email_subject", context)
        body = svc.render(template, "email_body", context)
        logger.debug(f"[email] Sending to={email} template={template}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                SENDGRID_URL,
                headers={"Authorization": f"Bearer {SENDGRID_KEY}"},
                json={
                    "personalizations": [{"to": [{"email": email}]}],
                    "from": {"email": SENDGRID_FROM},
                    "subject": subject,
                    "content": [{"type": "text/plain", "value": body}],
                },
            )
            resp.raise_for_status()
            logger.debug(f"[email] Delivered - email={email}.")
'''

PUSH_SENDER_PY = '''\
"""
notification-service/src/senders/push_sender.py

Push notification delivery via Firebase Cloud Messaging (FCM).
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

FCM_SERVER_KEY = os.getenv("FCM_SERVER_KEY", "fcm_test_biterush")
FCM_URL = "https://fcm.googleapis.com/fcm/send"


class PushSender:
    async def send(self, device_token: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        svc = TemplateService()
        title = svc.render(template, "push_title", context)
        body = svc.render(template, "push_body", context)
        logger.debug(f"[push] Sending token={device_token[:8]}... template={template}.")
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                FCM_URL,
                headers={"Authorization": f"key={FCM_SERVER_KEY}"},
                json={
                    "to": device_token,
                    "notification": {"title": title, "body": body},
                    "data": {"order_id": context.get("order_id"), "template": template},
                },
            )
            resp.raise_for_status()
            logger.debug(f"[push] Delivered - token={device_token[:8]}...")
'''

NOTIFICATION_CACHE_PY = '''\
"""notification-service/src/infrastructure/cache.py"""
from shared.redis_client import RedisClient
CacheClient = RedisClient
'''

# ============================================================================
# LOG FILES
# ============================================================================

LOG_APR_20 = """\
2026-04-20_06:00:00 | INFO    | main.py               | L27  | OrderService starting up - initialising infrastructure.
2026-04-20_06:00:00 | INFO    | db.py                 | L18  | [db] Opening pool - dsn='postgresql://order_rw@pg-orders.biterush.svc.cluster.local:5432/biterush_orders' size=10.
2026-04-20_06:00:01 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-20_06:00:01 | INFO    | redis_client.py       | L28  | [redis] Connecting - redis-primary.biterush.svc.cluster.local:6379/0.
2026-04-20_06:00:01 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-20_06:00:01 | INFO    | kafka_client.py       | L23  | [kafka] Starting producer - brokers=kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092.
2026-04-20_06:00:02 | INFO    | kafka_client.py       | L32  | [kafka] Producer ready.
2026-04-20_06:00:02 | INFO    | kafka_client.py       | L63  | [kafka] Starting consumer group=biterush.order-service.payment topics=['payment.completed', 'payment.failed'].
2026-04-20_06:00:02 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.order-service.payment.
2026-04-20_06:00:02 | INFO    | kafka_client.py       | L63  | [kafka] Starting consumer group=biterush.order-service.inventory topics=['inventory.reserved', 'inventory.reservation_failed'].
2026-04-20_06:00:02 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.order-service.inventory.
2026-04-20_06:00:02 | INFO    | main.py               | L35  | OrderService ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_06:00:05 | INFO    | main.py               | L27  | InventoryService starting up.
2026-04-20_06:00:05 | INFO    | db.py                 | L18  | [db] Opening pool - dsn='postgresql://inv_rw@pg-inventory.biterush.svc:5432/biterush_inventory' size=10.
2026-04-20_06:00:06 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-20_06:00:06 | INFO    | redis_client.py       | L28  | [redis] Connecting - redis-primary.biterush.svc.cluster.local:6379/0.
2026-04-20_06:00:06 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-20_06:00:06 | INFO    | kafka_client.py       | L23  | [kafka] Starting producer - brokers=kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092.
2026-04-20_06:00:06 | INFO    | kafka_client.py       | L32  | [kafka] Producer ready.
2026-04-20_06:00:06 | INFO    | kafka_client.py       | L63  | [kafka] Starting consumer group=biterush.inventory-service.orders topics=['order.created', 'order.cancelled'].
2026-04-20_06:00:06 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.inventory-service.orders.
2026-04-20_06:00:06 | INFO    | main.py               | L35  | InventoryService ready.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_06:00:08 | INFO    | main.py               | L27  | PaymentService starting up.
2026-04-20_06:00:09 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-20_06:00:09 | INFO    | kafka_client.py       | L32  | [kafka] Producer ready.
2026-04-20_06:00:09 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.payment-service.orders.
2026-04-20_06:00:09 | INFO    | main.py               | L35  | PaymentService ready.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_06:00:10 | INFO    | main.py               | L27  | UserService starting up.
2026-04-20_06:00:11 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-20_06:00:11 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-20_06:00:11 | INFO    | main.py               | L35  | UserService ready.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_06:00:12 | INFO    | main.py               | L27  | NotificationService starting up.
2026-04-20_06:00:12 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-20_06:00:12 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.notification-service.orders.
2026-04-20_06:00:12 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.notification-service.payments.
2026-04-20_06:00:12 | INFO    | main.py               | L35  | NotificationService ready.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:15:22 | DEBUG   | main.py               | L55  | [a3f7c1b2] Incoming POST /api/v1/auth/login from 10.0.4.12
2026-04-20_09:15:22 | INFO    | auth.py               | L44  | [auth] Login request - email=priya.sharma@gmail.com.
2026-04-20_09:15:22 | DEBUG   | auth_service.py       | L64  | [auth_svc] Validating token (len=0).
2026-04-20_09:15:22 | DEBUG   | user_repo.py          | L14  | [db] Acquired - checked_out=1/10.
2026-04-20_09:15:22 | INFO    | auth_service.py       | L55  | [auth_svc] Token issued - user_id=USR-A1B2C3D4.
2026-04-20_09:15:22 | INFO    | auth.py               | L47  | [auth] Login successful - user_id=USR-A1B2C3D4.
2026-04-20_09:15:22 | INFO    | main.py               | L67  | [a3f7c1b2] POST /api/v1/auth/login -> HTTP 200 in 48.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:16:01 | DEBUG   | main.py               | L55  | [b9d2e4f1] Incoming POST /api/v1/cart/cart-88fa3bc1/items from 10.0.4.12
2026-04-20_09:16:01 | INFO    | cart.py               | L31  | [cart] Add item - cart_id=cart-88fa3bc1 item_id=ITEM-BASMATI-5KG qty=2.
2026-04-20_09:16:01 | DEBUG   | cart_service.py       | L30  | [cart] add_item - cart_id=cart-88fa3bc1 item_id=ITEM-BASMATI-5KG qty=2.
2026-04-20_09:16:01 | INFO    | cart_service.py       | L53  | [cart] Item added - cart_id=cart-88fa3bc1 item_id=ITEM-BASMATI-5KG total_items=1.
2026-04-20_09:16:01 | INFO    | main.py               | L67  | [b9d2e4f1] POST /api/v1/cart/cart-88fa3bc1/items -> HTTP 200 in 22.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:16:14 | DEBUG   | main.py               | L55  | [c2a1f8e0] Incoming POST /api/v1/cart/cart-88fa3bc1/items from 10.0.4.12
2026-04-20_09:16:14 | INFO    | cart.py               | L31  | [cart] Add item - cart_id=cart-88fa3bc1 item_id=ITEM-WHOLE-MILK-1L qty=6.
2026-04-20_09:16:14 | DEBUG   | cart_service.py       | L30  | [cart] add_item - cart_id=cart-88fa3bc1 item_id=ITEM-WHOLE-MILK-1L qty=6.
2026-04-20_09:16:14 | INFO    | cart_service.py       | L53  | [cart] Item added - cart_id=cart-88fa3bc1 item_id=ITEM-WHOLE-MILK-1L total_items=2.
2026-04-20_09:16:14 | INFO    | main.py               | L67  | [c2a1f8e0] POST /api/v1/cart/cart-88fa3bc1/items -> HTTP 200 in 19.4ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:17:44 | DEBUG   | main.py               | L55  | [d5e3b7a9] Incoming POST /api/v1/orders from 10.0.4.12
2026-04-20_09:17:44 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-88fa3bc1 method=card.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L35  | [order] New placement attempt - order_id=ORD-F4A1B2C3 cart_id=cart-88fa3bc1.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L38  | [order] Validating user token - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | INFO    | order_service.py      | L41  | [order] User validated - user_id=USR-A1B2C3D4 order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L44  | [order] Loading cart - cart_id=cart-88fa3bc1.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L49  | [order] Cart loaded - 2 items.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L52  | [order] Checking inventory - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | INFO    | order_service.py      | L54  | [order] Inventory confirmed available - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L57  | [order] Validating delivery address - address_id=ADDR-001.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L59  | [order] Address validated - city=Mumbai.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L66  | [order] Totals - subtotal=649.00 discount=0 gst=32.45 delivery=29.00 total=710.45.
2026-04-20_09:17:44 | INFO    | order_service.py      | L74  | [order] Persisting order record - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | order_repo.py         | L14  | [order_repo] Inserting order - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | INFO    | order_service.py      | L76  | [order] Order record created - order_id=ORD-F4A1B2C3 status=pending_payment.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L79  | [order] Publishing order.created event - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | kafka_client.py       | L45  | [kafka] Publishing to topic=order.created key=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=order.created key=ORD-F4A1B2C3.
2026-04-20_09:17:44 | INFO    | order_service.py      | L81  | [order] order.created published - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:44 | DEBUG   | order_service.py      | L84  | [order] Initiating payment - order_id=ORD-F4A1B2C3 method=card.
2026-04-20_09:17:44 | DEBUG   | stripe_gateway.py     | L17  | [stripe] Creating PaymentIntent - order_id=ORD-F4A1B2C3 amount_paise=71045.
2026-04-20_09:17:45 | INFO    | stripe_gateway.py     | L29  | [stripe] PaymentIntent created - id=pi_3Ptest001 order_id=ORD-F4A1B2C3.
2026-04-20_09:17:45 | INFO    | order_service.py      | L88  | [order] Payment initiated - order_id=ORD-F4A1B2C3 payment_id=PAY-9E2D1A3B status=pending.
2026-04-20_09:17:45 | INFO    | order_service.py      | L91  | [order] Order status updated - order_id=ORD-F4A1B2C3 status=awaiting_payment.
2026-04-20_09:17:45 | INFO    | orders.py             | L55  | [n/a] Order placed - order_id=ORD-F4A1B2C3 total=710.45.
2026-04-20_09:17:45 | INFO    | main.py               | L67  | [d5e3b7a9] POST /api/v1/orders -> HTTP 200 in 1243.7ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:17:46 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=order.created partition=0 offset=1441 key=ORD-F4A1B2C3.
2026-04-20_09:17:46 | INFO    | order_consumer.py     | L28  | [inventory/order-consumer] Event received - order_id=ORD-F4A1B2C3 hint=created.
2026-04-20_09:17:46 | INFO    | reservation_service.py | L24  | [reservation] Reserving stock - order_id=ORD-F4A1B2C3 items=2.
2026-04-20_09:17:46 | DEBUG   | reservation_service.py | L35  | [reservation] Advisory lock acquired - order_id=ORD-F4A1B2C3.
2026-04-20_09:17:46 | DEBUG   | reservation_service.py | L47  | [reservation] Decremented - item_id=ITEM-BASMATI-5KG qty=2 remaining=48.
2026-04-20_09:17:46 | DEBUG   | reservation_service.py | L47  | [reservation] Decremented - item_id=ITEM-WHOLE-MILK-1L qty=6 remaining=74.
2026-04-20_09:17:46 | INFO    | reservation_service.py | L68  | [reservation] Stock reserved - order_id=ORD-F4A1B2C3 items=[{'item_id': 'ITEM-BASMATI-5KG', 'quantity': 2}, {'item_id': 'ITEM-WHOLE-MILK-1L', 'quantity': 6}].
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_09:17:55 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=order.created partition=0 offset=1441 key=ORD-F4A1B2C3.
2026-04-20_09:17:55 | INFO    | order_consumer.py     | L28  | [notif/order-consumer] Processing event - order_id=ORD-F4A1B2C3 type=created.
2026-04-20_09:17:55 | INFO    | notification_service.py | L41  | [notif] Sending notification - user_id=USR-A1B2C3D4 template=order_placed channels=sms_email.
2026-04-20_09:17:55 | DEBUG   | sms_sender.py         | L16  | [sms] Sending to=+919876543210 template=order_placed.
2026-04-20_09:17:56 | DEBUG   | sms_sender.py         | L21  | [sms] Delivered - phone=+919876543210 status=200.
2026-04-20_09:17:56 | INFO    | notification_service.py | L48  | [notif] SMS sent - user_id=USR-A1B2C3D4 template=order_placed.
2026-04-20_09:17:56 | DEBUG   | email_sender.py       | L19  | [email] Sending to=priya.sharma@gmail.com template=order_placed.
2026-04-20_09:17:57 | DEBUG   | email_sender.py       | L24  | [email] Delivered - email=priya.sharma@gmail.com.
2026-04-20_09:17:57 | INFO    | notification_service.py | L55  | [notif] Email sent - user_id=USR-A1B2C3D4 template=order_placed.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_10:04:11 | DEBUG   | main.py               | L55  | [e8c9d2a0] Incoming POST /api/v1/payments/webhook/stripe from 54.187.174.169
2026-04-20_10:04:11 | INFO    | payments.py           | L52  | [webhook] Stripe webhook received - sig_present=True.
2026-04-20_10:04:11 | INFO    | payment_service.py    | L51  | [payment] Stripe webhook - event_type=payment_intent.succeeded event_id=evt_3Ptest001.
2026-04-20_10:04:11 | INFO    | payment_service.py    | L61  | [payment] Payment completed via webhook - order_id=ORD-F4A1B2C3 amount=710.45.
2026-04-20_10:04:11 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=payment.completed key=ORD-F4A1B2C3.
2026-04-20_10:04:11 | INFO    | main.py               | L67  | [e8c9d2a0] POST /api/v1/payments/webhook/stripe -> HTTP 200 in 88.4ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_10:04:11 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=payment.completed partition=1 offset=882 key=ORD-F4A1B2C3.
2026-04-20_10:04:11 | INFO    | payment_consumer.py   | L26  | [order/payment-consumer] Received payment event - order_id=ORD-F4A1B2C3 status=completed.
2026-04-20_10:04:11 | INFO    | payment_consumer.py   | L31  | [order/payment-consumer] Order confirmed - order_id=ORD-F4A1B2C3.
2026-04-20_10:04:11 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=order.confirmed key=ORD-F4A1B2C3.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_10:04:12 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=order.confirmed partition=0 offset=1442 key=ORD-F4A1B2C3.
2026-04-20_10:04:12 | INFO    | order_consumer.py     | L28  | [notif/order-consumer] Processing event - order_id=ORD-F4A1B2C3 type=confirmed.
2026-04-20_10:04:12 | INFO    | notification_service.py | L41  | [notif] Sending notification - user_id=USR-A1B2C3D4 template=order_confirmed channels=sms_push.
2026-04-20_10:04:12 | INFO    | notification_service.py | L48  | [notif] SMS sent - user_id=USR-A1B2C3D4 template=order_confirmed.
2026-04-20_10:04:12 | INFO    | notification_service.py | L55  | [notif] Push sent - user_id=USR-A1B2C3D4 template=order_confirmed.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_11:30:00 | DEBUG   | main.py               | L55  | [f1a2b3c4] Incoming POST /api/v1/orders from 10.0.7.88
2026-04-20_11:30:00 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-d4e5f6a7 method=wallet.
2026-04-20_11:30:00 | DEBUG   | order_service.py      | L35  | [order] New placement attempt - order_id=ORD-C9D8E7F6 cart_id=cart-d4e5f6a7.
2026-04-20_11:30:00 | DEBUG   | order_service.py      | L38  | [order] Validating user token - order_id=ORD-C9D8E7F6.
2026-04-20_11:30:00 | INFO    | order_service.py      | L41  | [order] User validated - user_id=USR-B5C6D7E8 order_id=ORD-C9D8E7F6.
2026-04-20_11:30:00 | DEBUG   | order_service.py      | L44  | [order] Loading cart - cart_id=cart-d4e5f6a7.
2026-04-20_11:30:00 | DEBUG   | order_service.py      | L49  | [order] Cart loaded - 3 items.
2026-04-20_11:30:00 | DEBUG   | order_service.py      | L52  | [order] Checking inventory - order_id=ORD-C9D8E7F6.
2026-04-20_11:30:01 | WARNING | order_service.py      | L54  | [order] Inventory check returned unavailable items: ['ITEM-ALPHONSO-MANGO-1KG'].
2026-04-20_11:30:01 | WARNING | orders.py             | L62  | [n/a] Validation failed: Items out of stock: ['ITEM-ALPHONSO-MANGO-1KG'].
2026-04-20_11:30:01 | ERROR   | main.py               | L67  | [f1a2b3c4] POST /api/v1/orders -> HTTP 422 in 891.2ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_13:45:00 | DEBUG   | main.py               | L55  | [g7h8i9j0] Incoming POST /api/v1/orders from 10.0.2.33
2026-04-20_13:45:00 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-99ba11cc method=card.
2026-04-20_13:45:00 | DEBUG   | order_service.py      | L35  | [order] New placement attempt - order_id=ORD-A2B3C4D5 cart_id=cart-99ba11cc.
2026-04-20_13:45:00 | DEBUG   | order_service.py      | L38  | [order] Validating user token - order_id=ORD-A2B3C4D5.
2026-04-20_13:45:05 | ERROR   | order_service.py      | L41  | [order] User validated failed - order_id=ORD-A2B3C4D5: HTTPStatusError 401 from user-service /api/v1/auth/me.
2026-04-20_13:45:05 | WARNING | orders.py             | L60  | [n/a] Auth failed placing order: 401 from user-service.
2026-04-20_13:45:05 | ERROR   | main.py               | L67  | [g7h8i9j0] POST /api/v1/orders -> HTTP 403 in 5012.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-20_15:00:00 | DEBUG   | main.py               | L55  | [h1i2j3k4] Incoming POST /api/v1/refunds from 10.0.1.11
2026-04-20_15:00:00 | INFO    | refunds.py            | L22  | [refund] Refund request - order_id=ORD-F4A1B2C3 amount=710.45.
2026-04-20_15:00:00 | DEBUG   | refund_service.py     | L34  | [refund] Processing refund - order_id=ORD-F4A1B2C3 amount=710.45.
2026-04-20_15:00:00 | INFO    | refund_service.py     | L49  | [refund] Refund type=partial - order_id=ORD-F4A1B2C3 paid=710.45 requested=710.45.
2026-04-20_15:00:00 | DEBUG   | stripe_gateway.py     | L39  | [stripe] Refunding - pi=pi_3Ptest001 amount_paise=71044 full=False.
2026-04-20_15:00:01 | INFO    | stripe_gateway.py     | L48  | [stripe] Refund created - refund_id=re_3Ptest001.
2026-04-20_15:00:01 | INFO    | refund_service.py     | L57  | [refund] Refund recorded - order_id=ORD-F4A1B2C3 gateway_result={'refund_id': 're_3Ptest001', 'status': 'succeeded'}.
2026-04-20_15:00:01 | INFO    | main.py               | L67  | [h1i2j3k4] POST /api/v1/refunds -> HTTP 200 in 1102.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""

LOG_APR_21 = """\
2026-04-21_07:00:00 | INFO    | main.py               | L27  | OrderService starting up - initialising infrastructure.
2026-04-21_07:00:01 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-21_07:00:01 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-21_07:00:01 | INFO    | kafka_client.py       | L32  | [kafka] Producer ready.
2026-04-21_07:00:01 | INFO    | main.py               | L35  | OrderService ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_09:00:00 | DEBUG   | main.py               | L55  | [j5k6l7m8] Incoming POST /api/v1/orders from 10.0.4.55
2026-04-21_09:00:00 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-bb22dd44 method=card.
2026-04-21_09:00:00 | DEBUG   | order_service.py      | L35  | [order] New placement attempt - order_id=ORD-E3F4A5B6 cart_id=cart-bb22dd44.
2026-04-21_09:00:00 | INFO    | order_service.py      | L41  | [order] User validated - user_id=USR-F9G0H1I2 order_id=ORD-E3F4A5B6.
2026-04-21_09:00:00 | DEBUG   | order_service.py      | L49  | [order] Cart loaded - 1 items.
2026-04-21_09:00:00 | INFO    | order_service.py      | L54  | [order] Inventory confirmed available - order_id=ORD-E3F4A5B6.
2026-04-21_09:00:00 | DEBUG   | order_service.py      | L57  | [order] Validating delivery address - address_id=ADDR-044.
2026-04-21_09:00:00 | DEBUG   | order_service.py      | L59  | [order] Address validated - city=Bangalore.
2026-04-21_09:00:00 | INFO    | order_service.py      | L62  | [order] Promo applied - code=FIRST10 discount=39.90.
2026-04-21_09:00:00 | DEBUG   | order_service.py      | L66  | [order] Totals - subtotal=399.00 discount=39.90 gst=19.95 delivery=29.00 total=408.05.
2026-04-21_09:00:00 | INFO    | order_service.py      | L76  | [order] Order record created - order_id=ORD-E3F4A5B6 status=pending_payment.
2026-04-21_09:00:00 | INFO    | order_service.py      | L81  | [order] order.created published - order_id=ORD-E3F4A5B6.
2026-04-21_09:00:00 | DEBUG   | stripe_gateway.py     | L17  | [stripe] Creating PaymentIntent - order_id=ORD-E3F4A5B6 amount_paise=40805.
2026-04-21_09:00:01 | INFO    | stripe_gateway.py     | L29  | [stripe] PaymentIntent created - id=pi_3Ptest002 order_id=ORD-E3F4A5B6.
2026-04-21_09:00:01 | INFO    | order_service.py      | L91  | [order] Order status updated - order_id=ORD-E3F4A5B6 status=awaiting_payment.
2026-04-21_09:00:01 | INFO    | orders.py             | L55  | [n/a] Order placed - order_id=ORD-E3F4A5B6 total=408.05.
2026-04-21_09:00:01 | INFO    | main.py               | L67  | [j5k6l7m8] POST /api/v1/orders -> HTTP 200 in 1189.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_09:00:14 | DEBUG   | main.py               | L55  | [n3o4p5q6] Incoming POST /api/v1/payments/webhook/stripe from 54.187.174.169
2026-04-21_09:00:14 | INFO    | payments.py           | L52  | [webhook] Stripe webhook received - sig_present=True.
2026-04-21_09:00:14 | INFO    | payment_service.py    | L51  | [payment] Stripe webhook - event_type=payment_intent.succeeded event_id=evt_3Ptest002.
2026-04-21_09:00:14 | INFO    | payment_service.py    | L61  | [payment] Payment completed via webhook - order_id=ORD-E3F4A5B6 amount=408.05.
2026-04-21_09:00:14 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=payment.completed key=ORD-E3F4A5B6.
2026-04-21_09:00:14 | INFO    | main.py               | L67  | [n3o4p5q6] POST /api/v1/payments/webhook/stripe -> HTTP 200 in 91.2ms
2026-04-21_09:00:14 | DEBUG   | main.py               | L55  | [r7s8t9u0] Incoming POST /api/v1/payments/webhook/stripe from 54.187.174.169
2026-04-21_09:00:14 | INFO    | payments.py           | L52  | [webhook] Stripe webhook received - sig_present=True.
2026-04-21_09:00:14 | INFO    | payment_service.py    | L51  | [payment] Stripe webhook - event_type=payment_intent.succeeded event_id=evt_3Ptest002.
2026-04-21_09:00:14 | INFO    | payment_service.py    | L61  | [payment] Payment completed via webhook - order_id=ORD-E3F4A5B6 amount=408.05.
2026-04-21_09:00:14 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=payment.completed key=ORD-E3F4A5B6.
2026-04-21_09:00:14 | INFO    | main.py               | L67  | [r7s8t9u0] POST /api/v1/payments/webhook/stripe -> HTTP 200 in 87.6ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_09:00:15 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=payment.completed partition=1 offset=883 key=ORD-E3F4A5B6.
2026-04-21_09:00:15 | INFO    | payment_consumer.py   | L26  | [order/payment-consumer] Received payment event - order_id=ORD-E3F4A5B6 status=completed.
2026-04-21_09:00:15 | INFO    | payment_consumer.py   | L31  | [order/payment-consumer] Order confirmed - order_id=ORD-E3F4A5B6.
2026-04-21_09:00:15 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=payment.completed partition=1 offset=884 key=ORD-E3F4A5B6.
2026-04-21_09:00:15 | INFO    | payment_consumer.py   | L26  | [order/payment-consumer] Received payment event - order_id=ORD-E3F4A5B6 status=completed.
2026-04-21_09:00:15 | INFO    | payment_consumer.py   | L31  | [order/payment-consumer] Order confirmed - order_id=ORD-E3F4A5B6.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_10:30:00 | DEBUG   | main.py               | L55  | [v1w2x3y4] Incoming POST /api/v1/stock/reserve from 10.0.6.99
2026-04-21_10:30:00 | INFO    | stock.py              | L32  | [stock] Reserve request - order_id=ORD-G5H6I7J8 items=1.
2026-04-21_10:30:00 | INFO    | reservation_service.py | L24  | [reservation] Reserving stock - order_id=ORD-G5H6I7J8 items=1.
2026-04-21_10:30:00 | DEBUG   | reservation_service.py | L35  | [reservation] Advisory lock acquired - order_id=ORD-G5H6I7J8.
2026-04-21_10:30:00 | DEBUG   | reservation_service.py | L35  | [reservation] Advisory lock acquired - order_id=ORD-K9L0M1N2.
2026-04-21_10:30:00 | DEBUG   | reservation_service.py | L47  | [reservation] Decremented - item_id=ITEM-AMUL-BUTTER-500G qty=2 remaining=2.
2026-04-21_10:30:00 | DEBUG   | reservation_service.py | L47  | [reservation] Decremented - item_id=ITEM-AMUL-BUTTER-500G qty=2 remaining=0.
2026-04-21_10:30:00 | INFO    | reservation_service.py | L68  | [reservation] Stock reserved - order_id=ORD-G5H6I7J8 items=[{'item_id': 'ITEM-AMUL-BUTTER-500G', 'quantity': 2}].
2026-04-21_10:30:00 | INFO    | reservation_service.py | L68  | [reservation] Stock reserved - order_id=ORD-K9L0M1N2 items=[{'item_id': 'ITEM-AMUL-BUTTER-500G', 'quantity': 2}].
2026-04-21_10:30:01 | INFO    | main.py               | L67  | [v1w2x3y4] POST /api/v1/stock/reserve -> HTTP 200 in 312.4ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_11:15:00 | DEBUG   | main.py               | L55  | [z5a6b7c8] Incoming GET /api/v1/addresses/42 from 10.0.4.12
2026-04-21_11:15:00 | DEBUG   | addresses.py          | L38  | [address] Fetched address - address_id=42.
2026-04-21_11:15:00 | INFO    | main.py               | L67  | [z5a6b7c8] GET /api/v1/addresses/42 -> HTTP 200 in 14.7ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_13:00:00 | DEBUG   | main.py               | L55  | [d9e0f1g2] Incoming POST /api/v1/orders from 10.0.9.14
2026-04-21_13:00:00 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-ff11ee22 method=cod.
2026-04-21_13:00:00 | DEBUG   | order_service.py      | L35  | [order] New placement attempt - order_id=ORD-P7Q8R9S0 cart_id=cart-ff11ee22.
2026-04-21_13:00:00 | INFO    | order_service.py      | L41  | [order] User validated - user_id=USR-T1U2V3W4 order_id=ORD-P7Q8R9S0.
2026-04-21_13:00:00 | DEBUG   | order_service.py      | L49  | [order] Cart loaded - 4 items.
2026-04-21_13:00:00 | INFO    | order_service.py      | L54  | [order] Inventory confirmed available - order_id=ORD-P7Q8R9S0.
2026-04-21_13:00:00 | DEBUG   | order_service.py      | L59  | [order] Address validated - city=Pune.
2026-04-21_13:00:00 | DEBUG   | order_service.py      | L66  | [order] Totals - subtotal=1245.00 discount=0 gst=62.25 delivery=29.00 total=1336.25.
2026-04-21_13:00:00 | INFO    | order_service.py      | L76  | [order] Order record created - order_id=ORD-P7Q8R9S0 status=pending_payment.
2026-04-21_13:00:00 | INFO    | order_service.py      | L91  | [order] Order status updated - order_id=ORD-P7Q8R9S0 status=awaiting_payment.
2026-04-21_13:00:00 | INFO    | orders.py             | L55  | [n/a] Order placed - order_id=ORD-P7Q8R9S0 total=1336.25.
2026-04-21_13:00:00 | INFO    | main.py               | L67  | [d9e0f1g2] POST /api/v1/orders -> HTTP 200 in 988.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-21_14:30:00 | DEBUG   | main.py               | L55  | [h3i4j5k6] Incoming POST /api/v1/orders/ORD-P7Q8R9S0/cancel from 10.0.9.14
2026-04-21_14:30:00 | INFO    | orders.py             | L79  | [n/a] Cancel order request - order_id=ORD-P7Q8R9S0.
2026-04-21_14:30:00 | INFO    | order_service.py      | L100 | [order] Order cancelled - order_id=ORD-P7Q8R9S0.
2026-04-21_14:30:00 | DEBUG   | kafka_client.py       | L50  | [kafka] Published to topic=order.cancelled key=ORD-P7Q8R9S0.
2026-04-21_14:30:00 | INFO    | main.py               | L67  | [h3i4j5k6] POST /api/v1/orders/ORD-P7Q8R9S0/cancel -> HTTP 200 in 77.2ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""

LOG_APR_22 = """\
2026-04-22_07:00:00 | INFO    | main.py               | L27  | OrderService starting up - initialising infrastructure.
2026-04-22_07:00:00 | INFO    | db.py                 | L21  | [db] Pool ready.
2026-04-22_07:00:00 | INFO    | kafka_client.py       | L32  | [kafka] Producer ready.
2026-04-22_07:00:00 | INFO    | main.py               | L35  | OrderService ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_07:00:01 | INFO    | main.py               | L27  | NotificationService starting up.
2026-04-22_07:00:01 | INFO    | redis_client.py       | L36  | [redis] Connection established.
2026-04-22_07:00:01 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.notification-service.orders.
2026-04-22_07:00:01 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.notification-service.payments.
2026-04-22_07:00:01 | INFO    | main.py               | L35  | NotificationService ready.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_08:00:00 | WARNING | kafka_client.py       | L80  | [kafka] Broker kafka-1.biterush.svc:9092 unreachable. Reconnecting...
2026-04-22_08:00:03 | INFO    | kafka_client.py       | L72  | [kafka] Consumer ready - group=biterush.notification-service.orders.
2026-04-22_08:00:03 | WARNING | kafka_client.py       | L80  | [kafka] Duplicate consumer created - group=biterush.notification-service.orders (old task not cancelled).
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_08:02:00 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=order.created partition=0 offset=1501 key=ORD-X1Y2Z3A4.
2026-04-22_08:02:00 | INFO    | order_consumer.py     | L28  | [notif/order-consumer] Processing event - order_id=ORD-X1Y2Z3A4 type=created.
2026-04-22_08:02:00 | INFO    | notification_service.py | L41  | [notif] Sending notification - user_id=USR-B2C3D4E5 template=order_placed channels=sms_email.
2026-04-22_08:02:00 | INFO    | notification_service.py | L48  | [notif] SMS sent - user_id=USR-B2C3D4E5 template=order_placed.
2026-04-22_08:02:00 | INFO    | notification_service.py | L55  | [notif] Email sent - user_id=USR-B2C3D4E5 template=order_placed.
2026-04-22_08:02:01 | INFO    | kafka_client.py       | L80  | [kafka] Received topic=order.created partition=0 offset=1501 key=ORD-X1Y2Z3A4.
2026-04-22_08:02:01 | INFO    | order_consumer.py     | L28  | [notif/order-consumer] Processing event - order_id=ORD-X1Y2Z3A4 type=created.
2026-04-22_08:02:01 | INFO    | notification_service.py | L41  | [notif] Sending notification - user_id=USR-B2C3D4E5 template=order_placed channels=sms_email.
2026-04-22_08:02:01 | INFO    | notification_service.py | L48  | [notif] SMS sent - user_id=USR-B2C3D4E5 template=order_placed.
2026-04-22_08:02:01 | INFO    | notification_service.py | L55  | [notif] Email sent - user_id=USR-B2C3D4E5 template=order_placed.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_09:30:00 | DEBUG   | main.py               | L55  | [m7n8o9p0] Incoming POST /api/v1/orders from 10.0.5.77
2026-04-22_09:30:00 | INFO    | orders.py             | L36  | [n/a] Place order request - cart_id=cart-aa99bb88 method=card.
2026-04-22_09:30:00 | INFO    | order_service.py      | L41  | [order] User validated - user_id=USR-Q5R6S7T8 order_id=ORD-B4C5D6E7.
2026-04-22_09:30:00 | DEBUG   | order_service.py      | L49  | [order] Cart loaded - 2 items.
2026-04-22_09:30:00 | INFO    | order_service.py      | L54  | [order] Inventory confirmed available - order_id=ORD-B4C5D6E7.
2026-04-22_09:30:00 | DEBUG   | order_service.py      | L59  | [order] Address validated - city=Chennai.
2026-04-22_09:30:00 | DEBUG   | order_service.py      | L66  | [order] Totals - subtotal=889.00 discount=0 gst=44.45 delivery=29.00 total=962.45.
2026-04-22_09:30:00 | INFO    | order_service.py      | L76  | [order] Order record created - order_id=ORD-B4C5D6E7 status=pending_payment.
2026-04-22_09:30:01 | INFO    | order_service.py      | L81  | [order] order.created published - order_id=ORD-B4C5D6E7.
2026-04-22_09:30:01 | DEBUG   | stripe_gateway.py     | L17  | [stripe] Creating PaymentIntent - order_id=ORD-B4C5D6E7 amount_paise=96245.
2026-04-22_09:30:04 | ERROR   | order_service.py      | L88  | [order] Initiating payment failed - order_id=ORD-B4C5D6E7: ConnectTimeout connecting to payment-service.
2026-04-22_09:30:04 | ERROR   | orders.py             | L67  | [n/a] Order placement failed: ConnectTimeout connecting to payment-service.
2026-04-22_09:30:04 | ERROR   | main.py               | L67  | [m7n8o9p0] POST /api/v1/orders -> HTTP 503 in 4441.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_10:00:00 | DEBUG   | main.py               | L55  | [q1r2s3t4] Incoming POST /api/v1/auth/register from 10.0.3.21
2026-04-22_10:00:00 | INFO    | auth.py               | L28  | [auth] Register request - email=rohan.mehta@gmail.com.
2026-04-22_10:00:01 | INFO    | auth.py               | L32  | [auth] User registered - user_id=USR-C7D8E9F0.
2026-04-22_10:00:01 | INFO    | main.py               | L67  | [q1r2s3t4] POST /api/v1/auth/register -> HTTP 200 in 412.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_10:30:00 | WARNING | notification_service.py | L29  | [notif] resolve_user failed attempt=0 order_id=ORD-U5V6W7X8: ConnectError - order-service unreachable.
2026-04-22_10:30:00 | WARNING | notification_service.py | L29  | [notif] resolve_user failed attempt=1 order_id=ORD-U5V6W7X8: ConnectError - order-service unreachable.
2026-04-22_10:30:00 | WARNING | notification_service.py | L29  | [notif] resolve_user failed attempt=2 order_id=ORD-U5V6W7X8: ConnectError - order-service unreachable.
2026-04-22_10:30:00 | ERROR   | notification_service.py | L33  | [notif] Could not resolve user_id for order_id=ORD-U5V6W7X8.
2026-04-22_10:30:00 | WARNING | notification_service.py | L36  | [notif] Cannot send order_confirmed - user_id is None.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_11:00:00 | DEBUG   | main.py               | L55  | [u9v0w1x2] Incoming POST /api/v1/cart/cart-3c4d5e6f/items from 10.0.4.12
2026-04-22_11:00:00 | INFO    | cart.py               | L31  | [cart] Add item - cart_id=cart-3c4d5e6f item_id=ITEM-TOOR-DAL-1KG qty=3.
2026-04-22_11:00:00 | DEBUG   | cart_service.py       | L30  | [cart] add_item - cart_id=cart-3c4d5e6f item_id=ITEM-TOOR-DAL-1KG qty=3.
2026-04-22_11:00:00 | INFO    | cart_service.py       | L53  | [cart] Item added - cart_id=cart-3c4d5e6f item_id=ITEM-TOOR-DAL-1KG total_items=1.
2026-04-22_11:00:00 | INFO    | main.py               | L67  | [u9v0w1x2] POST /api/v1/cart/cart-3c4d5e6f/items -> HTTP 200 in 18.9ms
2026-04-22_11:00:00 | DEBUG   | main.py               | L55  | [y3z4a5b6] Incoming POST /api/v1/cart/cart-3c4d5e6f/items from 10.0.4.13
2026-04-22_11:00:00 | INFO    | cart.py               | L31  | [cart] Add item - cart_id=cart-3c4d5e6f item_id=ITEM-TOOR-DAL-1KG qty=2.
2026-04-22_11:00:00 | INFO    | cart_service.py       | L53  | [cart] Item added - cart_id=cart-3c4d5e6f item_id=ITEM-TOOR-DAL-1KG total_items=1.
2026-04-22_11:00:00 | INFO    | main.py               | L67  | [y3z4a5b6] POST /api/v1/cart/cart-3c4d5e6f/items -> HTTP 200 in 21.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_14:00:00 | DEBUG   | main.py               | L55  | [c7d8e9f0] Incoming POST /api/v1/auth/login from 10.0.8.44
2026-04-22_14:00:00 | INFO    | auth.py               | L44  | [auth] Login request - email=amit.patel@hotmail.com.
2026-04-22_14:00:00 | WARNING | auth_service.py       | L46  | [auth_svc] Login failed - email=amit.patel@hotmail.com: Invalid email or password.
2026-04-22_14:00:00 | ERROR   | main.py               | L67  | [c7d8e9f0] POST /api/v1/auth/login -> HTTP 401 in 61.2ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-22_16:00:00 | DEBUG   | main.py               | L55  | [g1h2i3j4] Incoming POST /api/v1/refunds from 10.0.1.11
2026-04-22_16:00:00 | INFO    | refunds.py            | L22  | [refund] Refund request - order_id=ORD-B4C5D6E7 amount=199.90.
2026-04-22_16:00:00 | DEBUG   | refund_service.py     | L34  | [refund] Processing refund - order_id=ORD-B4C5D6E7 amount=199.90.
2026-04-22_16:00:00 | INFO    | refund_service.py     | L49  | [refund] Refund type=partial - order_id=ORD-B4C5D6E7 paid=199.90 requested=199.90.
2026-04-22_16:00:00 | DEBUG   | stripe_gateway.py     | L39  | [stripe] Refunding - pi=pi_3Ptest003 amount_paise=19989 full=False.
2026-04-22_16:00:01 | INFO    | stripe_gateway.py     | L48  | [stripe] Refund created - refund_id=re_3Ptest003.
2026-04-22_16:00:01 | INFO    | refund_service.py     | L57  | [refund] Refund recorded - order_id=ORD-B4C5D6E7 gateway_result={'refund_id': 're_3Ptest003', 'status': 'succeeded'}.
2026-04-22_16:00:01 | INFO    | main.py               | L67  | [g1h2i3j4] POST /api/v1/refunds -> HTTP 200 in 1089.7ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""


# ============================================================================
# GENERATOR
# ============================================================================

def generate() -> tuple[Path, Path]:
    base = Path(__file__).resolve().parent
    project_root = base / "biterush"
    log_root = base / "logs"

    # Shared modules
    shared = project_root / "shared"
    shared_files = {
        shared / "logger.py": SHARED_LOGGER_PY,
        shared / "kafka_client.py": SHARED_KAFKA_CLIENT_PY,
        shared / "redis_client.py": SHARED_REDIS_CLIENT_PY,
        shared / "settings.py": SHARED_SETTINGS_PY,
        shared / "__init__.py": "",
    }

    # Order service
    order_root = project_root / "order-service" / "src"
    order_files = {
        order_root / "__init__.py": "",
        order_root / "main.py": ORDER_MAIN_PY,
        order_root / "api" / "__init__.py": "",
        order_root / "api" / "orders.py": ORDER_ORDERS_PY,
        order_root / "api" / "cart.py": ORDER_CART_PY,
        order_root / "services" / "__init__.py": "",
        order_root / "services" / "order_service.py": ORDER_SERVICE_PY,
        order_root / "services" / "cart_service.py": CART_SERVICE_PY,
        order_root / "consumers" / "__init__.py": "",
        order_root / "consumers" / "payment_consumer.py": ORDER_PAYMENT_CONSUMER_PY,
        order_root / "consumers" / "inventory_consumer.py": ORDER_INVENTORY_CONSUMER_PY,
        order_root / "repositories" / "__init__.py": "",
        order_root / "repositories" / "order_repo.py": ORDER_REPO_PY,
        order_root / "models" / "__init__.py": "",
        order_root / "models" / "order_models.py": ORDER_MODELS_PY,
        order_root / "infrastructure" / "__init__.py": "",
        order_root / "infrastructure" / "db.py": ORDER_DB_PY,
        order_root / "infrastructure" / "cache.py": ORDER_CACHE_PY,
    }

    # Inventory service
    inv_root = project_root / "inventory-service" / "src"
    inv_files = {
        inv_root / "__init__.py": "",
        inv_root / "main.py": INVENTORY_MAIN_PY,
        inv_root / "api" / "__init__.py": "",
        inv_root / "api" / "items.py": INVENTORY_ITEMS_PY,
        inv_root / "api" / "stock.py": INVENTORY_STOCK_PY,
        inv_root / "services" / "__init__.py": "",
        inv_root / "services" / "inventory_service.py": INVENTORY_SERVICE_PY,
        inv_root / "services" / "reservation_service.py": RESERVATION_SERVICE_PY,
        inv_root / "consumers" / "__init__.py": "",
        inv_root / "consumers" / "order_consumer.py": INVENTORY_ORDER_CONSUMER_PY,
        inv_root / "repositories" / "__init__.py": "",
        inv_root / "repositories" / "item_repo.py": ITEM_REPO_PY,
        inv_root / "repositories" / "stock_repo.py": STOCK_REPO_PY,
        inv_root / "infrastructure" / "__init__.py": "",
        inv_root / "infrastructure" / "db.py": INVENTORY_DB_PY,
        inv_root / "infrastructure" / "cache.py": INVENTORY_CACHE_PY,
    }

    # Payment service
    pay_root = project_root / "payment-service" / "src"
    pay_files = {
        pay_root / "__init__.py": "",
        pay_root / "main.py": PAYMENT_MAIN_PY,
        pay_root / "api" / "__init__.py": "",
        pay_root / "api" / "payments.py": PAYMENT_PAYMENTS_PY,
        pay_root / "api" / "refunds.py": PAYMENT_REFUNDS_PY,
        pay_root / "services" / "__init__.py": "",
        pay_root / "services" / "payment_service.py": PAYMENT_SERVICE_PY,
        pay_root / "services" / "refund_service.py": REFUND_SERVICE_PY,
        pay_root / "consumers" / "__init__.py": "",
        pay_root / "consumers" / "order_consumer.py": PAYMENT_ORDER_CONSUMER_PY,
        pay_root / "repositories" / "__init__.py": "",
        pay_root / "repositories" / "payment_repo.py": PAYMENT_REPO_PY,
        pay_root / "infrastructure" / "__init__.py": "",
        pay_root / "infrastructure" / "db.py": PAYMENT_DB_PY,
        pay_root / "infrastructure" / "cache.py": PAYMENT_CACHE_PY,
        pay_root / "gateway" / "__init__.py": "",
        pay_root / "gateway" / "stripe_gateway.py": STRIPE_GATEWAY_PY,
        pay_root / "gateway" / "razorpay_gateway.py": RAZORPAY_GATEWAY_PY,
    }

    # User service
    user_root = project_root / "user-service" / "src"
    user_files = {
        user_root / "__init__.py": "",
        user_root / "main.py": USER_MAIN_PY,
        user_root / "api" / "__init__.py": "",
        user_root / "api" / "users.py": USER_USERS_PY,
        user_root / "api" / "addresses.py": USER_ADDRESSES_PY,
        user_root / "api" / "auth.py": USER_AUTH_PY,
        user_root / "services" / "__init__.py": "",
        user_root / "services" / "user_service.py": USER_SERVICE_PY,
        user_root / "services" / "auth_service.py": USER_AUTH_SERVICE_PY,
        user_root / "services" / "address_service.py": ADDRESS_SERVICE_PY,
        user_root / "repositories" / "__init__.py": "",
        user_root / "repositories" / "user_repo.py": USER_REPO_PY,
        user_root / "repositories" / "address_repo.py": ADDRESS_REPO_PY,
        user_root / "infrastructure" / "__init__.py": "",
        user_root / "infrastructure" / "db.py": USER_DB_PY,
        user_root / "infrastructure" / "cache.py": USER_CACHE_PY,
    }

    # Notification service
    notif_root = project_root / "notification-service" / "src"
    notif_files = {
        notif_root / "__init__.py": "",
        notif_root / "main.py": NOTIFICATION_MAIN_PY,
        notif_root / "consumers" / "__init__.py": "",
        notif_root / "consumers" / "order_consumer.py": NOTIFICATION_ORDER_CONSUMER_PY,
        notif_root / "consumers" / "payment_consumer.py": NOTIFICATION_PAYMENT_CONSUMER_PY,
        notif_root / "services" / "__init__.py": "",
        notif_root / "services" / "notification_service.py": NOTIFICATION_SERVICE_PY,
        notif_root / "services" / "template_service.py": TEMPLATE_SERVICE_PY,
        notif_root / "infrastructure" / "__init__.py": "",
        notif_root / "infrastructure" / "cache.py": NOTIFICATION_CACHE_PY,
        notif_root / "senders" / "__init__.py": "",
        notif_root / "senders" / "sms_sender.py": SMS_SENDER_PY,
        notif_root / "senders" / "email_sender.py": EMAIL_SENDER_PY,
        notif_root / "senders" / "push_sender.py": PUSH_SENDER_PY,
    }

    log_files = {
        log_root / "2026-04-20.text": LOG_APR_20,
        log_root / "2026-04-21.text": LOG_APR_21,
        log_root / "2026-04-22.text": LOG_APR_22,
    }

    all_files = {
        **shared_files,
        **order_files,
        **inv_files,
        **pay_files,
        **user_files,
        **notif_files,
        **log_files,
    }

    for path, content in all_files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    return project_root, log_root


def print_tree(root: Path, label: str) -> None:
    print(f"\n{'-' * 64}")
    print(f"  {label}")
    print(f"{'-' * 64}")
    def _walk(path: Path, prefix: str = "") -> None:
        if path.is_dir():
            print(f"{prefix}{path.name}/")
            children = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name))
            for child in children:
                _walk(child, prefix + "  ")
        else:
            print(f"{prefix}{path.name}")
    _walk(root)


def main() -> None:
    project_root, log_root = generate()

    import ast
    errors = []
    for fp in project_root.rglob("*.py"):
        src = fp.read_text(encoding="utf-8")
        if not src.strip():
            continue
        try:
            ast.parse(src)
        except SyntaxError as exc:
            errors.append(f"{fp.relative_to(project_root)}: {exc}")

    print(f"\n{'=' * 64}")
    print("  BiteRush — food & grocery delivery platform generated")
    print(f"{'=' * 64}\n")

    if errors:
        print("  Syntax errors detected:")
        for e in errors:
            print(f"    {e}")
        print()
    else:
        print("  All Python files pass syntax check.\n")

    print("  Services")
    print("  - order-service        :8000  Postgres: pg-orders")
    print("  - inventory-service    :8001  Postgres: pg-inventory")
    print("  - payment-service      :8002  Postgres: pg-payments  Stripe + Razorpay")
    print("  - user-service         :8003  Postgres: pg-users")
    print("  - notification-service :8004  Exotel/SendGrid/FCM")
    print()
    print("  Shared infrastructure")
    print("  - Kafka      kafka-1.biterush.svc:9092, kafka-2.biterush.svc:9092")
    print("  - Redis      redis-primary.biterush.svc.cluster.local:6379")
    print()
    print("  Bugs embedded (10 total — no obvious signs in code):")
    print("  #1  [EASY]      order_service.py       GST calculated before discount")
    print("  #2  [MEDIUM]    cart_service.py        Race condition on concurrent cart writes")
    print("  #3  [MEDIUM]    payment_consumer.py    Exception swallowing + unconditional commit")
    print("  #4  [HARD]      reservation_service.py Advisory lock keyed on order, not item")
    print("  #5  [HARD]      payment_service.py     Stripe webhook duplicate processing")
    print("  #6  [HARD]      refund_service.py      Decimal/float comparison + paise precision loss")
    print("  #7  [VERY DEEP] auth_service.py        Token leak via cache key + log exposure")
    print("  #8  [HARD]      address_service.py     IDOR — no ownership check on get_address")
    print("  #9  [VERY DEEP] order_consumer.py      Task leak → duplicate Kafka consumers on reconnect")
    print("  #10 [VERY DEEP] notification_service.py Thundering herd + broken retry backoff (sleep(0))")

    print_tree(project_root, "biterush/")
    print_tree(log_root, "logs/")
    print()


if __name__ == "__main__":
    main()
GENERATOR_EOF
echo "Generator script written. Size: $(wc -l < ecommerce_generator.py) lines"