"""
src/api/endpoints.py

Route definitions for the DataBridge API.
Each endpoint delegates business logic to the service layer.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.services.external_client import ExternalClient
from src.services.data_processor import DataProcessor
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
client = ExternalClient()
processor = DataProcessor()


class ProcessRequest(BaseModel):
    feed_id: str
    record_id: int
    divisor: int = 10


# ── Route 1: Triggers ConnectionError (Bug #1) ──────────────────────────────

@router.get("/feeds/{feed_id}", tags=["feeds"])
async def get_feed(feed_id: str) -> dict:
    """
    Fetch a raw data feed from the upstream provider.

    Raises:
        HTTPException 503: When the upstream provider is unreachable.
    """
    logger.info(f"Fetching feed '{feed_id}' from upstream provider.")
    try:
        data = client.fetch_feed(feed_id)           # line 36 — raises ConnectionError
        return {"feed_id": feed_id, "data": data}
    except ConnectionError as exc:
        logger.error(f"Upstream provider unavailable for feed '{feed_id}': {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


# ── Route 2: Triggers ValueError / Schema Mismatch (Bug #2) ─────────────────

@router.get("/records/{record_id}", tags=["records"])
async def get_record(record_id: int) -> dict:
    """
    Retrieve a processed record by its numeric ID.

    Raises:
        HTTPException 500: When the upstream payload fails schema validation.
    """
    logger.info(f"Retrieving record id={record_id}.")
    try:
        raw = client.fetch_record(record_id)        # returns malformed payload
        result = processor.normalise_record(raw)    # line 54 — raises ValueError
        return {"record_id": record_id, "result": result}
    except ValueError as exc:
        logger.error(f"Schema validation failed for record id={record_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


# ── Route 3: Triggers ZeroDivisionError on divisor=0 (Bug #3) ───────────────

@router.post("/process", tags=["process"])
async def process_record(body: ProcessRequest) -> dict:
    """
    Submit a processing job for a feed record.

    Raises:
        HTTPException 500: For unexpected computation errors.
    """
    logger.info(
        f"Processing feed='{body.feed_id}' record={body.record_id} "
        f"divisor={body.divisor}."
    )
    try:
        output = processor.compute_score(           # line 74 — raises ZeroDivisionError
            record_id=body.record_id,
            divisor=body.divisor,
        )
        return {"feed_id": body.feed_id, "score": output}
    except ZeroDivisionError as exc:
        logger.error(
            f"Computation error for feed='{body.feed_id}' "
            f"record={body.record_id}: {exc}"
        )
        raise HTTPException(status_code=500, detail="Division by zero in score computation.")


# ── Route 4: Healthy baseline ────────────────────────────────────────────────

@router.get("/summary", tags=["feeds"])
async def summary(limit: int = Query(default=10, ge=1, le=100)) -> dict:
    """Return a summary of the most recent feed records (no external calls)."""
    logger.info(f"Summary requested, limit={limit}.")
    records = [{"id": i, "status": "ok"} for i in range(1, limit + 1)]
    return {"count": len(records), "records": records}
