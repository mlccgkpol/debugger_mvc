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
