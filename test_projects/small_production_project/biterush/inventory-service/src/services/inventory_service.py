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
