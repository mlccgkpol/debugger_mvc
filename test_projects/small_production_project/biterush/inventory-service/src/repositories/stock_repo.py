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
