"""
inventory-service/src/api/stock.py

Stock check and reservation endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.reservation_service import ReservationService
from src.services.inventory_service import InventoryService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
inv_svc = InventoryService()
rsv_svc = ReservationService()


class StockCheckRequest(BaseModel):
    items: list[dict]


class ReserveRequest(BaseModel):
    order_id: str
    items: list[dict]


@router.post("/check")
async def check_stock(body: StockCheckRequest) -> dict:
    logger.info(f"[stock] Stock check for {len(body.items)} items.")
    results = await inv_svc.check_stock(body.items)
    return {"items": results}


@router.post("/reserve")
async def reserve_stock(body: ReserveRequest) -> dict:
    logger.info(f"[stock] Reserve request - order_id={body.order_id} items={len(body.items)}.")
    try:
        result = await rsv_svc.reserve(order_id=body.order_id, items=body.items)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/release")
async def release_stock(body: ReserveRequest) -> dict:
    logger.info(f"[stock] Release request - order_id={body.order_id}.")
    result = await rsv_svc.release(order_id=body.order_id, items=body.items)
    return result
