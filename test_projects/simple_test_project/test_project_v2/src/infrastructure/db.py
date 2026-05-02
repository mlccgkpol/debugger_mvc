"""
src/infrastructure/db.py

asyncpg connection pool to TimescaleDB.

DSN             : postgresql://pulse_rw:changeme@ts-db-prod-1.pulse.internal:5432/pulsemetrics
Pool size       : 5 (hard cap; raise via DB_POOL_SIZE env var)
Acquire timeout : 2.0 s — after which OperationalError is raised  ← BUG #1 source
"""

import asyncio
import asyncpg                                         # noqa: F401  (real asyncpg)

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_DSN             = "postgresql://pulse_rw:changeme@ts-db-prod-1.pulse.internal:5432/pulsemetrics"
_POOL_SIZE       = 5
_ACQUIRE_TIMEOUT = 2.0   # seconds


class OperationalError(Exception):
    """Raised when a database operation cannot be completed."""


class Database:
    """asyncpg connection pool manager (singleton pattern via module-level instance)."""

    _pool:      asyncpg.Pool | None = None
    _checked:   int                 = 0
    _max_conns: int                 = _POOL_SIZE

    async def connect(self):
        logger.info(
            f"[db] Opening asyncpg pool — dsn={_DSN!r} pool_size={self._max_conns}."
        )
        self._pool = await asyncpg.create_pool(
            dsn=_DSN,
            min_size=2,
            max_size=self._max_conns,
            command_timeout=30,
        )
        self._checked = 0
        logger.info("[db] asyncpg pool ready.")

    async def disconnect(self):
        logger.info("[db] Closing asyncpg pool.")
        if self._pool:
            await self._pool.close()

    async def acquire(self) -> asyncpg.Connection:
        """
        Check out a connection from the pool.

        Raises:
            OperationalError: When the pool is exhausted and the acquire
                              timeout (2.0 s) elapses.                     ← BUG #1
        """
        logger.debug(
            f"[db] acquire() called — "
            f"available={self._max_conns - self._checked}/{self._max_conns}."
        )

        if self._checked >= self._max_conns:           # line 68
            logger.warning(
                f"[db] Pool exhausted ({self._checked}/{self._max_conns} in use). "
                f"Waiting up to {_ACQUIRE_TIMEOUT}s for a free connection."
            )
            await asyncio.sleep(_ACQUIRE_TIMEOUT)
            raise OperationalError(                    # line 75 — BUG #1 raise site
                f"Connection pool exhausted: all {self._max_conns} connections "
                "are checked out. Increase pool_size or reduce query concurrency."
            )

        self._checked += 1
        conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT)
        logger.debug(f"[db] Connection acquired — checked_out={self._checked}.")
        return conn

    async def release(self, conn: asyncpg.Connection):
        if self._pool and conn:
            await self._pool.release(conn)
        if self._checked > 0:
            self._checked -= 1
        logger.debug(f"[db] Connection released — checked_out={self._checked}.")

    async def ping(self) -> str:
        if not self._pool:
            return "disconnected"
        try:
            await self._pool.fetchval("SELECT 1")
            return "ok"
        except Exception:
            return "error"
