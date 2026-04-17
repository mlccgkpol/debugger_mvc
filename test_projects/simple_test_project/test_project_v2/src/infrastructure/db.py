"""
src/infrastructure/db.py

Database connection pool wrapper (simulated asyncpg interface).

BUG #1 lives here: the pool size is hard-capped at 5 connections.
Under load, acquire() raises OperationalError when all connections
are checked out and the wait exceeds the timeout.
"""

import asyncio
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_POOL_SIZE    = 5
_ACQUIRE_TIMEOUT = 2.0   # seconds


class OperationalError(Exception):
    """Raised when a database operation cannot be completed."""


class _FakeConn:
    """Simulated asyncpg connection."""
    async def execute(self, sql, *args):
        await asyncio.sleep(0.01)
        return "INSERT 0 1"

    async def fetch(self, sql, *args):
        await asyncio.sleep(0.01)
        return []

    async def fetchrow(self, sql, *args):
        await asyncio.sleep(0.01)
        return {"source_id": args[0], "tenant": args[1],
                "retention_days": 30, "sampling_rate": 1.0, "active": True}


class Database:
    """Singleton-style connection pool manager."""

    _pool:     list   = []
    _checked:  int    = 0
    _max_conns: int   = _POOL_SIZE

    async def connect(self):
        logger.info(f"[db] Connecting — pool_size={self._max_conns}.")
        self._pool = [_FakeConn() for _ in range(self._max_conns)]
        self._checked = 0
        logger.info("[db] Connection pool ready.")

    async def disconnect(self):
        logger.info("[db] Closing connection pool.")
        self._pool.clear()

    async def acquire(self):
        """
        Check out a connection from the pool.

        Raises:
            OperationalError: When pool is exhausted and timeout is exceeded.  ← BUG #1
        """
        logger.debug(
            f"[db] acquire() called — "
            f"available={len(self._pool) - self._checked}/{len(self._pool)}."
        )

        if self._checked >= len(self._pool):          # line 68
            # Pool exhausted — simulate timeout wait
            logger.warning(
                f"[db] Pool exhausted ({self._checked}/{len(self._pool)} in use). "
                f"Waiting up to {_ACQUIRE_TIMEOUT}s for a free connection."
            )
            await asyncio.sleep(_ACQUIRE_TIMEOUT)
            raise OperationalError(                   # line 75 — BUG #1 raise site
                f"Connection pool exhausted: all {len(self._pool)} connections "
                f"are checked out. Increase pool_size or reduce query concurrency."
            )

        self._checked += 1
        conn = self._pool[self._checked - 1]
        logger.debug(f"[db] Connection acquired — checked_out={self._checked}.")
        return conn

    async def release(self, conn):
        if self._checked > 0:
            self._checked -= 1
        logger.debug(f"[db] Connection released — checked_out={self._checked}.")

    async def ping(self) -> str:
        return "ok" if self._pool else "disconnected"
