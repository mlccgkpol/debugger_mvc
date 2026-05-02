"""
src/services/query_service.py

Orchestrates metric queries: Redis cache-first read with TimescaleDB fallback.
"""

from typing import Any, Optional

from src.infrastructure.cache import CacheClient
from src.repositories.metric_repo import MetricRepository
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
metric_repo = MetricRepository()
cache = CacheClient()


class QueryService:
    async def get_summary(
        self,
        source_id: str,
        metric: str,
        window: int,
        tenant_id: Optional[str],
    ) -> dict[str, Any]:
        tenant = tenant_id or "default"
        cache_key = f"summary:{tenant}:{source_id}:{metric}:{window}"

        logger.debug(
            f"[query] Summary requested - source={source_id} "
            f"metric={metric} window={window}s tenant={tenant}."
        )

        logger.debug(f"[query] Checking Redis - key={cache_key}.")
        try:
            cached = await cache.get(cache_key)
        except TimeoutError as exc:
            logger.error(f"[query] Cache read failed - key={cache_key}: {exc}")
            raise

        if cached is not None:
            logger.info(f"[query] Cache hit - key={cache_key}.")
            return {**cached, "from_cache": True}

        logger.info(
            f"[query] Cache miss - falling back to TimescaleDB "
            f"source={source_id} metric={metric}."
        )

        logger.debug(
            f"[query] Querying metric_repo - source={source_id} window={window}s."
        )
        rows = await metric_repo.read_window(source_id, metric, window, tenant)
        logger.debug(
            f"[query] DB returned {len(rows) if rows is not None else 'None'} rows."
        )

        logger.debug(f"[query] Beginning aggregation - source={source_id}.")
        try:
            summary = self._aggregate(rows)
        except AttributeError as exc:
            logger.error(f"[query] Aggregation failed - source={source_id}: {exc}")
            raise

        logger.info(
            f"[query] Aggregation complete - "
            f"count={summary['count']} avg={summary['avg']:.4f}."
        )

        await cache.set(cache_key, summary, ttl=window)
        logger.debug(f"[query] Redis populated - key={cache_key} ttl={window}s.")

        return {**summary, "from_cache": False}

    async def get_timeseries(
        self,
        source_id: str,
        metric: str,
        start: int,
        end: int,
    ) -> dict[str, Any]:
        tenant = "default"
        logger.debug(
            f"[query] Timeseries fetch - source={source_id} "
            f"metric={metric} range=[{start},{end}]."
        )
        rows = await metric_repo.read_range(source_id, metric, start, end, tenant)
        return {"source_id": source_id, "metric": metric, "rows": rows}

    @staticmethod
    def _aggregate(rows) -> dict[str, Any]:
        iterator = rows.__iter__()
        values = [row["value"] for row in iterator]
        if not values:
            return {"count": 0, "min": None, "max": None, "avg": 0.0, "p95": None}

        sorted_vals = sorted(values)
        p95_idx = int(len(sorted_vals) * 0.95)
        return {
            "count": len(values),
            "min": sorted_vals[0],
            "max": sorted_vals[-1],
            "avg": sum(values) / len(values),
            "p95": sorted_vals[p95_idx],
        }
