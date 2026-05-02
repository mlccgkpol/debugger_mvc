"""
order-service/src/api/orders.py

Order placement, status lookup, and cancellation endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Path
from pydantic import BaseModel, Field

from src.services.order_service import OrderService
from src.services.cart_service import CartService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = OrderService()
cart_svc = CartService()


class PlaceOrderRequest(BaseModel):
    cart_id: str
    delivery_address_id: str
    payment_method: str = Field(..., pattern="^(card|wallet|cod)$")
    promo_code: Optional[str] = None
    delivery_instructions: Optional[str] = None


@router.post("")
async def place_order(
    body: PlaceOrderRequest,
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(
        f"[{rid}] Place order request - cart_id={body.cart_id} "
        f"method={body.payment_method}."
    )

    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")

    try:
        order = await svc.place_order(
            token=token,
            cart_id=body.cart_id,
            delivery_address_id=body.delivery_address_id,
            payment_method=body.payment_method,
            promo_code=body.promo_code,
            delivery_instructions=body.delivery_instructions,
        )
        logger.info(
            f"[{rid}] Order placed - order_id={order['order_id']} "
            f"total={order['total_amount']}."
        )
        return order
    except PermissionError as exc:
        logger.warning(f"[{rid}] Auth failed placing order: {exc}")
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        logger.warning(f"[{rid}] Validation failed: {exc}")
        raise HTTPException(status_code=422, detail=str(exc))
    except RuntimeError as exc:
        logger.error(f"[{rid}] Order placement failed: {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


@router.get("/{order_id}")
async def get_order(
    order_id: str = Path(...),
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(f"[{rid}] Get order request - order_id={order_id}.")

    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")

    try:
        order = await svc.get_order(order_id=order_id, token=token)
        if order is None:
            raise HTTPException(status_code=404, detail=f"Order {order_id} not found.")
        return order
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[{rid}] Failed to fetch order {order_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{order_id}/cancel")
async def cancel_order(
    order_id: str = Path(...),
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(f"[{rid}] Cancel order request - order_id={order_id}.")
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing auth token.")
    try:
        result = await svc.cancel_order(order_id=order_id, token=token)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.error(f"[{rid}] Cancel failed for order_id={order_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
