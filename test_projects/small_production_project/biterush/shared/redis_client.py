"""
shared/redis_client.py

Shared async Redis client (redis-py) for BiteRush services.
Each service uses this as a singleton scoped to the process.
"""

import json
import os
from typing import Any, Optional

import redis.asyncio as aioredis

from shared.logger import AppLogger

logger = AppLogger(__name__)

_HOST = os.getenv("REDIS_HOST", "redis-primary.biterush.svc.cluster.local")
_PORT = int(os.getenv("REDIS_PORT", "6379"))
_DB = int(os.getenv("REDIS_DB", "0"))
_DEFAULT_TTL = int(os.getenv("REDIS_DEFAULT_TTL", "300"))


class RedisClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._client = None
            cls._instance = inst
        return cls._instance

    async def connect(self):
        if self._client is not None:
            return
        logger.info(f"[redis] Connecting - {_HOST}:{_PORT}/{_DB}.")
        self._client = aioredis.Redis(
            host=_HOST, port=_PORT, db=_DB,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=2,
        )
        await self._client.ping()
        logger.info("[redis] Connection established.")

    async def disconnect(self):
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("[redis] Connection closed.")

    async def get(self, key: str) -> Optional[Any]:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        raw = await self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL) -> None:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        await self._client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str) -> None:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        await self._client.delete(key)

    async def incr(self, key: str, ttl: int = _DEFAULT_TTL) -> int:
        if self._client is None:
            raise RuntimeError("Redis not connected.")
        val = await self._client.incr(key)
        if val == 1:
            await self._client.expire(key, ttl)
        return val

    async def ping(self) -> str:
        if self._client is None:
            return "disconnected"
        try:
            await self._client.ping()
            return "ok"
        except Exception:
            return "error"
