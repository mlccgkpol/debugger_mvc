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
