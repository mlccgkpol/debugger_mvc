"""
src/infrastructure/db.py

asyncpg connection pool for the primary TimescaleDB cluster.
Configured from the runtime environment for production deployment.
"""

import asyncio
import os

import asyncpg

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_DSN = os.getenv(
    "PULSEMETRICS_DB_DSN",
    "postgresql://pulse_rw@ts-db-primary.pulsemetrics.svc.cluster.local:5432/pulsemetrics",
)
_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
_ACQUIRE_TIMEOUT = float(os.getenv("DB_ACQUIRE_TIMEOUT", "2.0"))


class OperationalError(Exception):
    """Raised when a database operation cannot be completed."""


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._pool = None
            instance._checked = 0
            instance._max_conns = _POOL_SIZE
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._pool is not None:
            return

        logger.info(
            f"[db] Opening asyncpg pool - dsn={_DSN!r} pool_size={self._max_conns}."
        )
        self._pool = await asyncpg.create_pool(
            dsn=_DSN,
            min_size=min(2, self._max_conns),
            max_size=self._max_conns,
            command_timeout=30,
        )
        self._checked = 0
        logger.info("[db] asyncpg pool ready.")

    async def disconnect(self):
        logger.info("[db] Closing asyncpg pool.")
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        self._checked = 0

    async def acquire(self) -> asyncpg.Connection:
        if self._pool is None:
            raise OperationalError("Database pool has not been initialised.")

        logger.debug(
            f"[db] acquire() called - checked_out={self._checked}/{self._max_conns}."
        )

        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT)
        except asyncio.TimeoutError as exc:
            logger.warning(
                f"[db] Pool acquire timed out after {_ACQUIRE_TIMEOUT}s "
                f"(checked_out={self._checked}/{self._max_conns})."
            )
            raise OperationalError(
                f"Timed out after {_ACQUIRE_TIMEOUT}s waiting for a database "
                f"connection from a pool of {self._max_conns}."
            ) from exc

        self._checked += 1
        logger.debug(f"[db] Connection acquired - checked_out={self._checked}.")
        return conn

    async def release(self, conn: asyncpg.Connection):
        if self._pool is not None and conn is not None:
            await self._pool.release(conn)
        if self._checked > 0:
            self._checked -= 1
        logger.debug(f"[db] Connection released - checked_out={self._checked}.")

    async def ping(self) -> str:
        if self._pool is None:
            return "disconnected"
        try:
            await self._pool.fetchval("SELECT 1")
            return "ok"
        except Exception:
            return "error"
