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
