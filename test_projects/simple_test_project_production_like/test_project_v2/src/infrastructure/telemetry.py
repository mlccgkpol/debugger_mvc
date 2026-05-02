"""
src/infrastructure/telemetry.py

PulseMetrics telemetry relay client.
Forwards raw metric events to downstream consumers.
"""

import json
import os
from typing import Any

import httpx

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_TELEMETRY_ENDPOINT = os.getenv(
    "PULSEMETRICS_TELEMETRY_URL",
    "https://telemetry-gateway.pulsemetrics.internal/v1/streams/pulse-prod",
)
_TELEMETRY_API_KEY = os.getenv(
    "PULSEMETRICS_TELEMETRY_API_KEY",
    "pulsemetrics-relay-local",
)
_EMIT_TIMEOUT = float(os.getenv("PULSEMETRICS_TELEMETRY_TIMEOUT", "3.0"))


class TelemetryClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._http = None
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._http is not None:
            return

        logger.info(
            f"[telemetry] Initialising relay client - endpoint={_TELEMETRY_ENDPOINT}."
        )
        self._http = httpx.AsyncClient(
            base_url=_TELEMETRY_ENDPOINT,
            headers={
                "X-PulseMetrics-Key": _TELEMETRY_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=_EMIT_TIMEOUT,
        )
        logger.info("[telemetry] Relay client ready.")

    async def disconnect(self):
        logger.info("[telemetry] Closing relay client.")
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def emit(self, tenant: str, event_id: str, event: dict[str, Any]) -> None:
        if self._http is None:
            raise RuntimeError("Telemetry client has not been initialised.")

        payload = {
            "tenant": tenant,
            "event_id": event_id,
            "source_id": event.get("source_id"),
            "metric": event.get("metric"),
            "value": event.get("value"),
            "timestamp": event.get("timestamp"),
        }
        try:
            response = await self._http.post("", content=json.dumps(payload))
            response.raise_for_status()
            logger.debug(
                f"[telemetry] Relay ack - event_id={event_id} status={response.status_code}."
            )
        except httpx.HTTPStatusError as exc:
            logger.warning(
                f"[telemetry] Relay rejected event_id={event_id}: "
                f"HTTP {exc.response.status_code}."
            )
        except Exception as exc:
            logger.warning(
                f"[telemetry] Relay emit failed for event_id={event_id}: {exc}."
            )

    async def ping(self) -> str:
        if self._http is None:
            return "disconnected"
        try:
            response = await self._http.get("/healthz")
            return "ok" if response.status_code == 200 else "degraded"
        except Exception:
            return "error"
