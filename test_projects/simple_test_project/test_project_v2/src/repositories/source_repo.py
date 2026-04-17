"""
src/repositories/source_repo.py

Data-access layer for source/sensor configuration records.
"""

from typing import Any, Optional

from src.infrastructure.db import Database
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db    = Database()


class SourceRepository:
    """Reads source configuration from the relational store."""

    async def get(self, source_id: str, tenant: str) -> Optional[dict[str, Any]]:
        """
        Fetch the configuration record for a source/sensor.

        Returns None if the source does not exist.
        """
        logger.debug(
            f"[source_repo] Fetching config — source_id={source_id} tenant={tenant}."
        )
        conn = await _db.acquire()
        try:
            sql = (
                "SELECT source_id, tenant, retention_days, sampling_rate, active "
                "FROM sources WHERE source_id=$1 AND tenant=$2"
            )
            row = await conn.fetchrow(sql, source_id, tenant)
            if row is None:
                logger.warning(
                    f"[source_repo] Source not found — "
                    f"source_id={source_id} tenant={tenant}."
                )
                return None
            return dict(row)
        finally:
            await _db.release(conn)
