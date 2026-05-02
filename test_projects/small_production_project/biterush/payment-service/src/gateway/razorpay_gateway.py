"""
payment-service/src/gateway/razorpay_gateway.py

Razorpay payment gateway for wallet/UPI payments.
"""

import os
from typing import Any
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)
RAZORPAY_KEY = os.getenv("RAZORPAY_KEY_ID", "rzp_test_biterush")
RAZORPAY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "rzp_secret_test")
RAZORPAY_BASE = "https://api.razorpay.com/v1"


class RazorpayGateway:
    async def create_order(self, amount: float, order_id: str) -> dict[str, Any]:
        amount_paise = int(round(amount * 100))
        logger.debug(f"[razorpay] Creating order - order_id={order_id} amount_paise={amount_paise}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{RAZORPAY_BASE}/orders",
                auth=(RAZORPAY_KEY, RAZORPAY_SECRET),
                json={"amount": amount_paise, "currency": "INR", "receipt": order_id},
            )
            resp.raise_for_status()
            data = resp.json()
            return {"ref_id": data["id"], "status": data["status"]}

    async def refund(self, order_id: str, amount_paise: int, full: bool) -> dict:
        logger.debug(f"[razorpay] Refund - order_id={order_id} amount_paise={amount_paise} full={full}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{RAZORPAY_BASE}/payments/{order_id}/refund",
                auth=(RAZORPAY_KEY, RAZORPAY_SECRET),
                json={"amount": amount_paise, "speed": "normal"},
            )
            resp.raise_for_status()
            return resp.json()
