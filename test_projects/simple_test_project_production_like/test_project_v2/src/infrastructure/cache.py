"""
src/infrastructure/cache.py

Redis 7 client via redis-py asyncio interface.
Summary requests use Redis as a read-through cache.
"""

import asyncio
import json
import os
from typing import Any, Optional

import redis.asyncio as aioredis

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_HOST = os.getenv("REDIS_HOST", "redis-primary.pulsemetrics.svc.cluster.local")
_PORT = int(os.getenv("REDIS_PORT", "6379"))
_DB = int(os.getenv("REDIS_DB", "0"))
_DEFAULT_TTL = int(os.getenv("REDIS_DEFAULT_TTL", "300"))
_GET_TIMEOUT = float(os.getenv("REDIS_GET_TIMEOUT", "1.5"))
_LOCK_POLL_INTERVAL = 0.05


class CacheClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._client = None
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._client is not None:
            return

        logger.info(f"[cache] Connecting to Redis - {_HOST}:{_PORT}/{_DB}.")
        self._client = aioredis.Redis(
            host=_HOST,
            port=_PORT,
            db=_DB,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=2,
        )
        await self._client.ping()
        logger.info("[cache] Redis connection established.")

    async def disconnect(self):
        logger.info("[cache] Closing Redis connection.")
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, key: str) -> Optional[Any]:
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")

        logger.debug(f"[cache] GET {key}.")

        raw = await self._client.get(key)
        if raw is not None:
            logger.debug(f"[cache] GET {key} -> HIT.")
            return json.loads(raw)

        lock_key = f"{key}:refresh-lock"
        if not await self._client.exists(lock_key):
            logger.debug(f"[cache] GET {key} -> MISS.")
            return None

        logger.warning(
            f"[cache] Refresh lock present for key={key}. "
            f"Waiting up to {_GET_TIMEOUT}s for a hydrated value."
        )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + _GET_TIMEOUT
        while loop.time() < deadline:
            await asyncio.sleep(_LOCK_POLL_INTERVAL)
            raw = await self._client.get(key)
            if raw is not None:
                logger.debug(f"[cache] GET {key} -> HIT after refresh wait.")
                return json.loads(raw)
            if not await self._client.exists(lock_key):
                logger.debug(f"[cache] GET {key} -> MISS after refresh wait.")
                return None

        raise TimeoutError(
            f"Cache GET timed out after {_GET_TIMEOUT}s waiting for refresh lock "
            f"on key='{key}'."
        )

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL):
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")
        logger.debug(f"[cache] SET {key} ttl={ttl}s.")
        await self._client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str):
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")
        removed = await self._client.delete(key)
        logger.debug(
            f"[cache] DEL {key} -> {'removed' if removed else 'not found'}."
        )

    async def ping(self) -> str:
        if self._client is None:
            return "disconnected"
        try:
            await self._client.ping()
            return "ok"
        except Exception:
            return "error"
