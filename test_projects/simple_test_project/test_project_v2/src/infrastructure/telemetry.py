"""
src/infrastructure/telemetry.py

Gravicore Telemetry Stream client.

Gravicore is a managed event-streaming platform (similar in role to Kinesis)
used by PulseMetrics to fan out raw metric events to downstream consumers:
the alerting engine, the ML anomaly detector, and the cold-storage archiver.

Endpoint : https://ingest.gravicore.io/v1/streams/pulse-prod
Auth     : static API key in X-Gravicore-Key header
"""

import asyncio
import json
from typing import Any

import httpx

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_GRAVICORE_ENDPOINT = "https://ingest.gravicore.io/v1/streams/pulse-prod"
_GRAVICORE_API_KEY  = "gvk-prod-7c2a9f1b4e36d08a"
_EMIT_TIMEOUT       = 3.0   # seconds


class TelemetryClient:
    """HTTP client for the Gravicore Telemetry Stream ingest API."""

    _http: httpx.AsyncClient | None = None

    async def connect(self):
        logger.info(
            f"[telemetry] Initialising Gravicore client — endpoint={_GRAVICORE_ENDPOINT}."
        )
        self._http = httpx.AsyncClient(
            base_url=_GRAVICORE_ENDPOINT,
            headers={
                "X-Gravicore-Key": _GRAVICORE_API_KEY,
                "Content-Type":    "application/json",
            },
            timeout=_EMIT_TIMEOUT,
        )
        logger.info("[telemetry] Gravicore client ready.")

    async def disconnect(self):
        logger.info("[telemetry] Closing Gravicore client.")
        if self._http:
            await self._http.aclose()

    async def emit(self, tenant: str, event_id: str, event: dict[str, Any]) -> None:
        """
        Forward a metric event to Gravicore Telemetry Stream.

        Fire-and-forget with a short timeout; failures are logged but
        do not interrupt the ingest pipeline.
        """
        payload = {
            "tenant":    tenant,
            "event_id":  event_id,
            "source_id": event.get("source_id"),
            "metric":    event.get("metric"),
            "value":     event.get("value"),
            "timestamp": event.get("timestamp"),
        }
        try:
            resp = await self._http.post("", content=json.dumps(payload))
            resp.raise_for_status()
            logger.debug(
                f"[telemetry] Gravicore ack — event_id={event_id} "
                f"status={resp.status_code}."
            )
        except httpx.HTTPStatusError as exc:
            logger.warning(
                f"[telemetry] Gravicore rejected event_id={event_id}: "
                f"HTTP {exc.response.status_code}."
            )
        except Exception as exc:
            logger.warning(
                f"[telemetry] Gravicore emit failed for event_id={event_id}: {exc}."
            )

    async def ping(self) -> str:
        if not self._http:
            return "disconnected"
        try:
            resp = await self._http.get("/healthz")
            return "ok" if resp.status_code == 200 else "degraded"
        except Exception:
            return "error"
