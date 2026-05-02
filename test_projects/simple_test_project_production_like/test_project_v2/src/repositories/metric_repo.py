"""
src/repositories/metric_repo.py

Data-access layer for time-series metric storage backed by TimescaleDB.
All queries are routed through the shared asyncpg connection pool in db.py.
"""

from typing import Any, Optional

import asyncpg

from src.infrastructure.db import Database, OperationalError
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class MetricRepository:
    async def write(
        self,
        tenant: str,
        event: dict[str, Any],
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] Acquiring DB connection - table={table} "
            f"metric={event.get('metric')}."
        )

        try:
            conn = await _db.acquire()
        except OperationalError as exc:
            logger.error(
                f"[metric_repo] Failed to acquire DB connection - table={table}: {exc}"
            )
            raise

        logger.debug("[metric_repo] Connection acquired from pool.")

        try:
            sql = (
                f"INSERT INTO {table} "
                "(source_id, metric, value, timestamp, tags) "
                "VALUES ($1, $2, $3, $4, $5)"
            )
            logger.debug(
                f"[metric_repo] Executing INSERT - metric={event.get('metric')}."
            )
            result = await conn.execute(
                sql,
                event["source_id"],
                event["metric"],
                event["value"],
                event.get("timestamp"),
                event.get("tags", {}),
            )
            rows = int(result.split()[-1])
            logger.debug(f"[metric_repo] INSERT complete - rows={rows}.")
            return {"rows": rows, "table": table}
        finally:
            await _db.release(conn)
            logger.debug("[metric_repo] Connection returned to pool.")

    async def read_window(
        self,
        source_id: str,
        metric: str,
        window: int,
        tenant: str,
    ) -> Optional[list[dict[str, Any]]]:
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] read_window - source={source_id} "
            f"metric={metric} window={window}s table={table}."
        )

        conn = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} "
                f"WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp > extract(epoch from now()) - $3"
            )
            try:
                rows = await conn.fetch(sql, source_id, metric, window)
            except asyncpg.exceptions.QueryCanceledError:
                logger.warning(
                    f"[metric_repo] Statement timeout reading source={source_id} "
                    f"metric={metric} window={window}s."
                )
                return None
            return [dict(row) for row in rows]
        finally:
            await _db.release(conn)

    async def read_range(
        self,
        source_id: str,
        metric: str,
        start: int,
        end: int,
        tenant: str,
    ) -> list[dict[str, Any]]:
        table = f"metrics_{tenant}"
        conn = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp BETWEEN $3 AND $4"
            )
            rows = await conn.fetch(sql, source_id, metric, start, end)
            return [dict(row) for row in rows]
        finally:
            await _db.release(conn)
