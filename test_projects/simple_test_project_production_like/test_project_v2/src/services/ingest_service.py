"""
src/services/ingest_service.py

Orchestrates the metric ingestion pipeline:
  validate -> enrich -> write to DB -> forward to telemetry -> invalidate cache
"""

import uuid
from typing import Any

from src.infrastructure.cache import CacheClient
from src.infrastructure.telemetry import TelemetryClient
from src.repositories.metric_repo import MetricRepository
from src.repositories.source_repo import SourceRepository
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
metric_repo = MetricRepository()
source_repo = SourceRepository()
cache = CacheClient()
telemetry = TelemetryClient()


class IngestService:
    async def ingest(self, tenant: str, event: dict[str, Any]) -> dict[str, Any]:
        event_id = str(uuid.uuid4())[:12]
        logger.debug(
            f"[ingest] Entering pipeline - event_id={event_id} "
            f"tenant={tenant} metric={event.get('metric')}."
        )

        self._validate(event)
        logger.debug(f"[ingest] Schema validation passed - event_id={event_id}.")

        logger.debug(f"[ingest] Resolving source_id={event.get('source_id')}.")
        source_cfg = await source_repo.get(event["source_id"], tenant)
        if source_cfg is None:
            raise RuntimeError(
                f"Source '{event['source_id']}' not found for tenant '{tenant}'."
            )
        logger.debug(
            f"[ingest] Source resolved - retention={source_cfg.get('retention_days')}d."
        )

        logger.info(
            f"[ingest] Writing metric to TimescaleDB - event_id={event_id} "
            f"source={event['source_id']}."
        )
        try:
            meta = await metric_repo.write(tenant, event, source_cfg)
        except Exception as exc:
            logger.error(f"[ingest] DB write failed for event_id={event_id}: {exc}")
            raise

        logger.info(f"[ingest] DB write OK - event_id={event_id} rows={meta.get('rows')}.")

        await telemetry.emit(tenant, event_id, event)
        logger.debug(f"[ingest] Relay emit acknowledged - event_id={event_id}.")

        cache_key = f"summary:{tenant}:{event['source_id']}:{event['metric']}"
        await cache.delete(cache_key)
        logger.debug(f"[ingest] Cache key invalidated - key={cache_key}.")

        return {"event_id": event_id, "tenant": tenant, "meta": meta}

    @staticmethod
    def _validate(event: dict[str, Any]) -> None:
        required = {"source_id", "metric", "value"}
        missing = required - set(event.keys())
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if not isinstance(event["value"], (int, float)):
            raise ValueError(
                f"Field 'value' must be numeric, got {type(event['value']).__name__}."
            )
