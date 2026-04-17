"""
src/services/query_service.py

Orchestrates metric queries: cache-first read with DB fallback and aggregation.
"""

from typing import Any, Optional

from src.repositories.metric_repo import MetricRepository
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger      = AppLogger(__name__)
metric_repo = MetricRepository()
cache       = CacheClient()


class QueryService:
    """Serves metric summaries and time-series data."""

    async def get_summary(
        self,
        source_id: str,
        metric:    str,
        window:    int,
        tenant_id: Optional[str],
    ) -> dict[str, Any]:
        """
        Return an aggregated summary (min/max/avg/count) over a rolling window.

        Cache-first strategy:
          1. Check cache for pre-computed result.
          2. On cache miss: query DB, aggregate, write back to cache.

        Raises:
            TimeoutError:   If the cache read exceeds its deadline.   ← BUG #2 path
            AttributeError: If the DB result set is unexpectedly None. ← BUG #3 path
        """
        tenant = tenant_id or "default"
        cache_key = f"summary:{tenant}:{source_id}:{metric}:{window}"

        logger.debug(
            f"[query] Summary requested — source={source_id} "
            f"metric={metric} window={window}s tenant={tenant}."
        )

        # ── Cache read ────────────────────────────────────────────────────────
        logger.debug(f"[query] Checking cache — key={cache_key}.")
        cached = await cache.get(cache_key)           # raises TimeoutError on stampede

        if cached is not None:
            logger.info(f"[query] Cache hit — key={cache_key}.")
            return {**cached, "from_cache": True}

        logger.info(
            f"[query] Cache miss — falling back to DB "
            f"source={source_id} metric={metric}."
        )

        # ── DB read ───────────────────────────────────────────────────────────
        logger.debug(
            f"[query] Querying metric_repo — source={source_id} window={window}s."
        )
        rows = await metric_repo.read_window(source_id, metric, window, tenant)
        logger.debug(f"[query] DB returned {len(rows) if rows is not None else 'None'} rows.")

        # ── Aggregation ───────────────────────────────────────────────────────
        logger.debug(f"[query] Beginning aggregation — source={source_id}.")
        summary = self._aggregate(rows)              # raises AttributeError when rows is None
        logger.info(
            f"[query] Aggregation complete — "
            f"count={summary['count']} avg={summary['avg']:.4f}."
        )

        # ── Write back to cache ───────────────────────────────────────────────
        await cache.set(cache_key, summary, ttl=window)
        logger.debug(f"[query] Cache populated — key={cache_key} ttl={window}s.")

        return {**summary, "from_cache": False}

    async def get_timeseries(
        self,
        source_id: str,
        metric:    str,
        start:     int,
        end:       int,
    ) -> dict[str, Any]:
        """Return raw time-series rows for a given range."""
        tenant = "default"
        logger.debug(
            f"[query] Timeseries fetch — source={source_id} "
            f"metric={metric} range=[{start},{end}]."
        )
        rows = await metric_repo.read_range(source_id, metric, start, end, tenant)
        return {"source_id": source_id, "metric": metric, "rows": rows}

    @staticmethod
    def _aggregate(rows) -> dict[str, Any]:         # line 74
        """
        Compute min/max/avg/p95 over a list of metric row dicts.

        Raises:
            AttributeError: When rows is None (DB returned nothing, not []).   ← BUG #3
        """
        values = [r["value"] for r in rows]          # line 79 — AttributeError if rows is None
        if not values:
            return {"count": 0, "min": None, "max": None, "avg": 0.0, "p95": None}

        sorted_vals = sorted(values)
        p95_idx     = int(len(sorted_vals) * 0.95)
        return {
            "count": len(values),
            "min":   sorted_vals[0],
            "max":   sorted_vals[-1],
            "avg":   sum(values) / len(values),
            "p95":   sorted_vals[p95_idx],
        }
