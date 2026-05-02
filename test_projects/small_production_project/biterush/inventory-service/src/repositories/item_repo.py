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
