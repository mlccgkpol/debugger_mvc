"""
payment-service/src/api/payments.py

Payment initiation and webhook endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from src.services.payment_service import PaymentService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = PaymentService()


class InitiateRequest(BaseModel):
    order_id: str
    amount: float
    method: str


@router.post("/initiate")
async def initiate_payment(
    body: InitiateRequest,
    authorization: Optional[str] = Header(default=None),
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    rid = x_request_id or "n/a"
    logger.info(
        f"[{rid}] Payment initiation - order_id={body.order_id} "
        f"amount={body.amount} method={body.method}."
    )
    try:
        result = await svc.initiate(
            order_id=body.order_id, amount=body.amount, method=body.method
        )
        logger.info(
            f"[{rid}] Payment initiated - payment_id={result['payment_id']} "
            f"order_id={body.order_id}."
        )
        return result
    except RuntimeError as exc:
        logger.error(f"[{rid}] Payment initiation failed - order_id={body.order_id}: {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


@router.post("/webhook/stripe")
async def stripe_webhook(request: Request) -> dict:
    body = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    logger.info(f"[webhook] Stripe webhook received - sig_present={bool(sig)}.")
    try:
        result = await svc.handle_stripe_webhook(raw_body=body, signature=sig)
        return result
    except ValueError as exc:
        logger.warning(f"[webhook] Stripe webhook rejected: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{payment_id}")
async def get_payment(payment_id: str) -> dict:
    pmt = await svc.get_payment(payment_id)
    if pmt is None:
        raise HTTPException(status_code=404, detail=f"Payment {payment_id} not found.")
    return pmt
