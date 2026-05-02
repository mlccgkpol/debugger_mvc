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
