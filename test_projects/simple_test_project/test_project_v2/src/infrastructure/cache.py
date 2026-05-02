"""
src/infrastructure/cache.py

Redis 7 client via redis-py asyncio interface.

Host    : redis-prod-1.pulse.internal:6379
DB      : 0
GET timeout : 1.5 s — after which TimeoutError is raised  ← BUG #2 source

Under high concurrency the cache layer can enter a stampede state where
multiple callers miss simultaneously and compete for a distributed lock
that is never released, causing every GET to time out.
"""

import asyncio
import json
from typing import Any, Optional

import redis.asyncio as aioredis              # redis-py ≥ 4.2

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_HOST        = "redis-prod-1.pulse.internal"
_PORT        = 6379
_DB          = 0
_DEFAULT_TTL = 300    # seconds
_GET_TIMEOUT = 1.5    # seconds before raising TimeoutError


class CacheClient:
    """Async Redis client wrapping redis-py's asyncio interface."""

    _client:    aioredis.Redis | None = None
    _stampede:  bool                  = False   # toggled by the chaos harness

    async def connect(self):
        logger.info(f"[cache] Connecting to Redis — {_HOST}:{_PORT}/{_DB}.")
        self._client = aioredis.Redis(
            host=_HOST, port=_PORT, db=_DB,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=2,
        )
        await self._client.ping()
        logger.info("[cache] Redis connection established.")

    async def disconnect(self):
        logger.info("[cache] Closing Redis connection.")
        if self._client:
            await self._client.aclose()

    async def get(self, key: str) -> Optional[Any]:
        """
        Fetch a cached value (JSON-decoded).

        Raises:
            TimeoutError: On cache stampede / distributed lock contention.  ← BUG #2
        """
        logger.debug(f"[cache] GET {key}.")

        if self._stampede:                             # line 40
            logger.warning(
                f"[cache] Stampede detected on key={key}. "
                f"Waiting {_GET_TIMEOUT}s for distributed lock."
            )
            await asyncio.sleep(_GET_TIMEOUT)
            raise TimeoutError(                        # line 46 — BUG #2 raise site
                f"Cache GET timed out after {_GET_TIMEOUT}s waiting for lock "
                f"on key='{key}'. "
                "Possible cache stampede — consider probabilistic early expiry."
            )

        raw = await self._client.get(key)
        if raw is None:
            logger.debug(f"[cache] GET {key} -> MISS.")
            return None
        logger.debug(f"[cache] GET {key} -> HIT.")
        return json.loads(raw)

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL):
        logger.debug(f"[cache] SET {key} ttl={ttl}s.")
        await self._client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str):
        removed = await self._client.delete(key)
        logger.debug(
            f"[cache] DEL {key} -> {'removed' if removed else 'not found'}."
        )

    async def ping(self) -> str:
        if not self._client:
            return "disconnected"
        try:
            await self._client.ping()
            return "ok"
        except Exception:
            return "error"
