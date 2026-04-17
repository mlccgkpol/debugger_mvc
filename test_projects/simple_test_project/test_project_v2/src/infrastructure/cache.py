"""
src/infrastructure/cache.py

Redis cache client (simulated).

BUG #2 lives here: under high concurrency the cache layer enters a
"stampede" state where multiple callers all miss simultaneously and
the get() call times out waiting for a lock that is never released.
"""

import asyncio
from typing import Any, Optional
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_DEFAULT_TTL = 300   # seconds
_GET_TIMEOUT = 1.5   # seconds before raising TimeoutError


class CacheClient:
    """Simulated async Redis client with in-process dict storage."""

    _store:    dict = {}
    _stampede: bool = False   # toggled by the test harness to trigger BUG #2

    async def connect(self):
        logger.info("[cache] Connecting to Redis.")
        self._store = {}
        logger.info("[cache] Redis connection established.")

    async def disconnect(self):
        logger.info("[cache] Closing Redis connection.")
        self._store.clear()

    async def get(self, key: str) -> Optional[Any]:
        """
        Fetch a cached value.

        Raises:
            TimeoutError: On cache stampede / lock contention.   ← BUG #2
        """
        logger.debug(f"[cache] GET {key}.")

        if self._stampede:                            # line 40
            logger.warning(
                f"[cache] Stampede detected on key={key}. "
                f"Waiting {_GET_TIMEOUT}s for distributed lock."
            )
            await asyncio.sleep(_GET_TIMEOUT)
            raise TimeoutError(                       # line 46 — BUG #2 raise site
                f"Cache GET timed out after {_GET_TIMEOUT}s waiting for lock on key='{key}'. "
                "Possible cache stampede — consider probabilistic early expiry."
            )

        value = self._store.get(key)
        logger.debug(f"[cache] GET {key} -> {'HIT' if value else 'MISS'}.")
        return value

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL):
        logger.debug(f"[cache] SET {key} ttl={ttl}s.")
        self._store[key] = value

    async def delete(self, key: str):
        removed = self._store.pop(key, None)
        logger.debug(f"[cache] DEL {key} -> {'removed' if removed else 'not found'}.")

    async def ping(self) -> str:
        return "ok"
