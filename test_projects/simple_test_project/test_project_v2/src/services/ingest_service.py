"""
src/services/ingest_service.py

Orchestrates the metric ingestion pipeline:
  validate → enrich → write to DB → invalidate cache
"""

import uuid
from typing import Any

from src.repositories.metric_repo import MetricRepository
from src.repositories.source_repo import SourceRepository
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger      = AppLogger(__name__)
metric_repo = MetricRepository()
source_repo = SourceRepository()
cache       = CacheClient()


class IngestService:
    """Coordinates validation, enrichment, and persistence of metric events."""

    async def ingest(self, tenant: str, event: dict[str, Any]) -> dict[str, Any]:
        """
        Run the full ingest pipeline for one event.

        Steps:
          1. Validate the event schema.
          2. Resolve the source to its configuration.
          3. Write the metric to the time-series store.
          4. Invalidate any cached summaries for this source.

        Returns:
            dict containing event_id and storage metadata.

        Raises:
            ValueError:        On schema validation failure.
            RuntimeError:      If the source cannot be resolved.
            OperationalError:  If the DB write fails.            ← BUG #1 path
        """
        event_id = str(uuid.uuid4())[:12]
        logger.debug(
            f"[ingest] Entering pipeline — event_id={event_id} "
            f"tenant={tenant} metric={event.get('metric')}."
        )

        # Step 1 — validate
        self._validate(event)
        logger.debug(f"[ingest] Schema validation passed — event_id={event_id}.")

        # Step 2 — resolve source
        logger.debug(f"[ingest] Resolving source_id={event.get('source_id')}.")
        source_cfg = await source_repo.get(event["source_id"], tenant)
        if source_cfg is None:
            raise RuntimeError(
                f"Source '{event['source_id']}' not found for tenant '{tenant}'."
            )
        logger.debug(
            f"[ingest] Source resolved — retention={source_cfg.get('retention_days')}d."
        )

        # Step 3 — persist
        logger.info(
            f"[ingest] Writing metric to DB — event_id={event_id} "
            f"source={event['source_id']}."
        )
        meta = await metric_repo.write(tenant, event, source_cfg)   # raises on pool exhaustion
        logger.info(f"[ingest] DB write OK — event_id={event_id} rows={meta.get('rows')}.")

        # Step 4 — cache invalidation
        cache_key = f"summary:{tenant}:{event['source_id']}:{event['metric']}"
        await cache.delete(cache_key)
        logger.debug(f"[ingest] Cache key invalidated — key={cache_key}.")

        return {"event_id": event_id, "tenant": tenant, "meta": meta}

    @staticmethod
    def _validate(event: dict[str, Any]) -> None:
        required = {"source_id", "metric", "value"}
        missing  = required - set(event.keys())
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if not isinstance(event["value"], (int, float)):
            raise ValueError(f"Field 'value' must be numeric, got {type(event['value']).__name__}.")
