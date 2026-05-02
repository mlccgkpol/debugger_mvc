"""
src/api/query.py

Query endpoints: aggregate metrics, return summaries and time-series data.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query as QParam

from src.services.query_service import QueryService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = QueryService()


@router.get("/summary/{source_id}")
async def get_summary(
    source_id: str,
    metric: str,
    window: int = QParam(default=3600, ge=60, le=86400),
    tenant_id: Optional[str] = None,
) -> dict:
    logger.info(
        f"Summary query - source='{source_id}' metric='{metric}' window={window}s."
    )

    try:
        result = await svc.get_summary(
            source_id=source_id,
            metric=metric,
            window=window,
            tenant_id=tenant_id,
        )
        logger.info(
            f"Summary OK - source='{source_id}' metric='{metric}' "
            f"cached={result.get('from_cache', False)}."
        )
        return result
    except TimeoutError as exc:
        logger.error(f"Summary timed out - source='{source_id}': {exc}")
        raise HTTPException(status_code=503, detail=str(exc))
    except AttributeError as exc:
        logger.error(f"Aggregation error - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected error in summary - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/timeseries/{source_id}")
async def get_timeseries(
    source_id: str,
    metric: str,
    start: int,
    end: int,
) -> dict:
    logger.info(
        f"Timeseries query - source='{source_id}' metric='{metric}' range=[{start},{end}]."
    )
    try:
        return await svc.get_timeseries(source_id, metric, start, end)
    except Exception as exc:
        logger.error(f"Timeseries failed - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
