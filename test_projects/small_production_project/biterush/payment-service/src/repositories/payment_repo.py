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
