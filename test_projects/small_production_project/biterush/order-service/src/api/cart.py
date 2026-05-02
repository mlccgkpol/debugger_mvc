"""
order-service/src/api/cart.py

Shopping cart endpoints: add items, remove items, get cart summary.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from src.services.cart_service import CartService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = CartService()


class CartItemRequest(BaseModel):
    item_id: str
    quantity: int = Field(..., ge=1, le=50)
    variant_id: Optional[str] = None


@router.post("/{cart_id}/items")
async def add_item(
    cart_id: str,
    body: CartItemRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(
        f"[cart] Add item - cart_id={cart_id} item_id={body.item_id} qty={body.quantity}."
    )
    token = (authorization or "").removeprefix("Bearer ").strip()
    try:
        return await svc.add_item(
            cart_id=cart_id,
            item_id=body.item_id,
            quantity=body.quantity,
            variant_id=body.variant_id,
            token=token,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.delete("/{cart_id}/items/{item_id}")
async def remove_item(
    cart_id: str,
    item_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(f"[cart] Remove item - cart_id={cart_id} item_id={item_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    return await svc.remove_item(cart_id=cart_id, item_id=item_id, token=token)


@router.get("/{cart_id}")
async def get_cart(
    cart_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    logger.info(f"[cart] Get cart - cart_id={cart_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    result = await svc.get_cart(cart_id=cart_id, token=token)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Cart {cart_id} not found.")
    return result
