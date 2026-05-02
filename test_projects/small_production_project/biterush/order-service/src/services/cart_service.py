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
