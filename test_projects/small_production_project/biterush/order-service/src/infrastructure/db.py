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
