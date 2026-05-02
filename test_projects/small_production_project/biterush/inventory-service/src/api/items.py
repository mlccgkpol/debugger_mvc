"""
inventory-service/src/api/items.py

Item catalogue endpoints.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from src.services.inventory_service import InventoryService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = InventoryService()


@router.get("/{item_id}")
async def get_item(item_id: str) -> dict:
    item = await svc.get_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
    return item


@router.get("/{item_id}/price")
async def get_price(item_id: str, variant_id: Optional[str] = None) -> dict:
    price = await svc.get_item_price(item_id, variant_id)
    if price is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
    return price


@router.get("")
async def list_items(
    category: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    return await svc.list_items(category=category, page=page, limit=limit)
