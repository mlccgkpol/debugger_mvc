"""
src/api/ingest.py

Ingest endpoints: accept metric events, validate, enrich, and persist.
"""

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional
import time

from src.services.ingest_service import IngestService
from src.services.auth_service import AuthService
from src.utils.logger import AppLogger

logger  = AppLogger(__name__)
router  = APIRouter()
svc     = IngestService()
auth    = AuthService()


class MetricEvent(BaseModel):
    source_id:  str
    metric:     str
    value:      float
    timestamp:  Optional[int] = None
    tags:       dict          = Field(default_factory=dict)


@router.post("/event")
async def ingest_event(
    event: MetricEvent,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    """
    Accept a single metric event.

    Auth   → validate JWT bearer token.
    Enrich → resolve source_id to tenant context.
    Persist → write to time-series DB.
    """
    rid = "n/a"

    logger.info(f"[{rid}] Ingest request for metric='{event.metric}' source='{event.source_id}'.")

    # Auth check
    try:
        token = (authorization or "").removeprefix("Bearer ").strip()
        tenant = auth.validate_token(token)          # BUG #4 lurks here
        logger.info(f"[{rid}] Auth OK — tenant='{tenant}'.")
    except ValueError as exc:
        logger.warning(f"[{rid}] Auth rejected for source='{event.source_id}': {exc}")
        raise HTTPException(status_code=401, detail=str(exc))

    # Ingest
    try:
        result = await svc.ingest(tenant=tenant, event=event.model_dump())
        logger.info(f"[{rid}] Event accepted — event_id='{result['event_id']}'.")
        return result
    except Exception as exc:
        logger.error(f"[{rid}] Ingest pipeline failed for metric='{event.metric}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/batch")
async def ingest_batch(
    events: list[MetricEvent],
    authorization: Optional[str] = Header(default=None),
) -> dict:
    """Accept a batch of metric events (max 500)."""
    rid = "n/a"
    logger.info(f"[{rid}] Batch ingest — {len(events)} events.")

    if len(events) > 500:
        raise HTTPException(status_code=422, detail="Batch size exceeds 500 events.")

    token  = (authorization or "").removeprefix("Bearer ").strip()
    tenant = auth.validate_token(token)

    results = []
    for ev in events:
        try:
            r = await svc.ingest(tenant=tenant, event=ev.model_dump())
            results.append(r)
        except Exception as exc:
            logger.error(f"[{rid}] Batch item failed — metric='{ev.metric}': {exc}")
            results.append({"error": str(exc)})

    ok    = sum(1 for r in results if "event_id" in r)
    failed = len(results) - ok
    logger.info(f"[{rid}] Batch complete — ok={ok} failed={failed}.")
    return {"ok": ok, "failed": failed, "results": results}
