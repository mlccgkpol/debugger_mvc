"""
src/repositories/metric_repo.py

Data-access layer for time-series metric storage.
Wraps the DB connection pool; all queries go through here.
"""

from typing import Any, Optional

from src.infrastructure.db import Database
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db    = Database()


class MetricRepository:
    """CRUD operations for metric events in the time-series store."""

    async def write(
        self,
        tenant:     str,
        event:      dict[str, Any],
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Persist a metric event to the time-series table.

        Raises:
            OperationalError: When the DB connection pool is exhausted.  ← BUG #1
        """
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] Acquiring DB connection — table={table} "
            f"metric={event.get('metric')}."
        )

        conn = await _db.acquire()                   # line 33 — raises OperationalError
        logger.debug(f"[metric_repo] Connection acquired from pool.")

        try:
            sql = (
                f"INSERT INTO {table} "
                "(source_id, metric, value, timestamp, tags) "
                "VALUES ($1, $2, $3, $4, $5)"
            )
            logger.debug(f"[metric_repo] Executing INSERT — metric={event.get('metric')}.")
            result = await conn.execute(
                sql,
                event["source_id"],
                event["metric"],
                event["value"],
                event.get("timestamp"),
                event.get("tags", {}),
            )
            rows = int(result.split()[-1])
            logger.debug(f"[metric_repo] INSERT complete — rows={rows}.")
            return {"rows": rows, "table": table}
        finally:
            await _db.release(conn)
            logger.debug("[metric_repo] Connection returned to pool.")

    async def read_window(
        self,
        source_id: str,
        metric:    str,
        window:    int,
        tenant:    str,
    ) -> Optional[list[dict[str, Any]]]:
        """
        Read metric rows within a rolling time window.

        Returns None when the DB query times out (simulated).   ← BUG #3 root cause
        """
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] read_window — source={source_id} "
            f"metric={metric} window={window}s table={table}."
        )

        conn = await _db.acquire()
        try:
            # Simulated: for specific combination, DB query returns None
            # (e.g. statement timeout reached, driver returns None instead of [])
            if source_id == "sensor-404" and metric == "temperature":
                logger.warning(
                    f"[metric_repo] Query returned None for source={source_id} "
                    f"metric={metric} — possible statement timeout."
                )
                return None                          # ← triggers BUG #3 in query_service

            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} "
                f"WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp > extract(epoch from now()) - $3"
            )
            rows = await conn.fetch(sql, source_id, metric, window)
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)

    async def read_range(
        self,
        source_id: str,
        metric:    str,
        start:     int,
        end:       int,
        tenant:    str,
    ) -> list[dict[str, Any]]:
        """Read raw rows between two Unix timestamps."""
        table = f"metrics_{tenant}"
        conn  = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp BETWEEN $3 AND $4"
            )
            rows = await conn.fetch(sql, source_id, metric, start, end)
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)
