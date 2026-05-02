"""
payment-service/src/gateway/stripe_gateway.py

Stripe payment gateway integration via stripe-python SDK.
"""

import os
from typing import Any
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "sk_test_biterush")
STRIPE_BASE = "https://api.stripe.com/v1"


class StripeGateway:
    async def create_payment_intent(self, amount: float, order_id: str) -> dict[str, Any]:
        amount_paise = int(round(amount * 100))
        logger.debug(f"[stripe] Creating PaymentIntent - order_id={order_id} amount_paise={amount_paise}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{STRIPE_BASE}/payment_intents",
                auth=(STRIPE_SECRET_KEY, ""),
                data={
                    "amount": amount_paise,
                    "currency": "inr",
                    "metadata[order_id]": order_id,
                    "confirm": "false",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"[stripe] PaymentIntent created - id={data['id']} order_id={order_id}.")
            return {"ref_id": data["id"], "client_secret": data["client_secret"]}

    async def refund(self, payment_intent_id: str, amount_paise: int, full: bool) -> dict:
        logger.debug(f"[stripe] Refunding - pi={payment_intent_id} amount_paise={amount_paise} full={full}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            data = {"payment_intent": payment_intent_id}
            if not full:
                data["amount"] = str(amount_paise)
            resp = await client.post(
                f"{STRIPE_BASE}/refunds",
                auth=(STRIPE_SECRET_KEY, ""),
                data=data,
            )
            resp.raise_for_status()
            result = resp.json()
            logger.info(f"[stripe] Refund created - refund_id={result['id']}.")
            return {"refund_id": result["id"], "status": result["status"]}
