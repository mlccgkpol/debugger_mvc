"""
payment-service/src/api/refunds.py

Refund endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.refund_service import RefundService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = RefundService()


class RefundRequest(BaseModel):
    order_id: str
    amount: float
    reason: str


@router.post("")
async def initiate_refund(body: RefundRequest) -> dict:
    logger.info(
        f"[refund] Refund request - order_id={body.order_id} amount={body.amount}."
    )
    try:
        result = await svc.refund(
            order_id=body.order_id, amount=body.amount, reason=body.reason
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
