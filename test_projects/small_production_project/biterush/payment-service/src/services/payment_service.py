"""
payment-service/src/services/payment_service.py

Routes payments to Stripe (card) or Razorpay (wallet/upi).
Handles Stripe webhook signature verification and event processing.

BUG #5 [HARD]: Stripe webhook replay attack / double-processing.
handle_stripe_webhook() verifies the signature and extracts the event,
then checks whether the payment_id is already in COMPLETED state via the DB.
However it does NOT use an idempotency key or advisory lock before processing.
If Stripe delivers the same webhook twice in rapid succession (which it does on
network hiccups), two concurrent calls both pass the already-processed check
(because neither has committed yet), and both publish payment.completed —
resulting in double-credit of the customer's wallet or double-confirmation
of the order.
"""

import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Optional

from src.gateway.stripe_gateway import StripeGateway
from src.gateway.razorpay_gateway import RazorpayGateway
from src.repositories.payment_repo import PaymentRepository
from src.infrastructure.cache import CacheClient
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_test_biterush")
PAYMENT_REPO = PaymentRepository()
CACHE = CacheClient()
PRODUCER = KafkaProducer()
STRIPE = StripeGateway()
RAZORPAY = RazorpayGateway()


class PaymentService:
    async def initiate(self, order_id: str, amount: float, method: str) -> dict[str, Any]:
        payment_id = f"PAY-{str(uuid.uuid4())[:8].upper()}"
        logger.debug(f"[payment] Initiating - payment_id={payment_id} order_id={order_id} method={method}.")

        if method == "card":
            gateway_ref = await STRIPE.create_payment_intent(amount=amount, order_id=order_id)
        elif method in ("wallet", "upi"):
            gateway_ref = await RAZORPAY.create_order(amount=amount, order_id=order_id)
        elif method == "cod":
            gateway_ref = {"ref_id": f"COD-{order_id}", "status": "pending"}
        else:
            raise RuntimeError(f"Unknown payment method: {method}")

        await PAYMENT_REPO.create(
            payment_id=payment_id,
            order_id=order_id,
            amount=amount,
            method=method,
            gateway_ref=gateway_ref.get("ref_id"),
            status="pending",
        )
        logger.info(f"[payment] Payment record created - payment_id={payment_id} gateway_ref={gateway_ref.get('ref_id')}.")
        return {"payment_id": payment_id, "status": "pending", "gateway": gateway_ref}

    async def handle_stripe_webhook(self, raw_body: bytes, signature: str) -> dict:
        self._verify_stripe_signature(raw_body, signature)
        event = json.loads(raw_body)
        event_type = event.get("type")
        event_id = event.get("id")
        logger.info(f"[payment] Stripe webhook - event_type={event_type} event_id={event_id}.")

        if event_type == "payment_intent.succeeded":
            data = event["data"]["object"]
            order_id = data.get("metadata", {}).get("order_id")
            amount = data.get("amount_received", 0) / 100

            # BUG #5 [HARD]: No idempotency guard here — concurrent duplicate
            # webhooks both see status='pending' and both publish payment.completed.
            existing = await PAYMENT_REPO.get_by_order(order_id)
            if existing and existing["status"] == "completed":
                logger.info(f"[payment] Webhook already processed - order_id={order_id}.")
                return {"status": "already_processed"}

            await PAYMENT_REPO.update_by_order(order_id, status="completed", gateway_event_id=event_id)
            logger.info(f"[payment] Payment completed via webhook - order_id={order_id} amount={amount}.")

            await PRODUCER.publish(
                topic="payment.completed",
                key=order_id,
                payload={
                    "order_id": order_id,
                    "status": "completed",
                    "amount": amount,
                    "event_id": event_id,
                },
            )

        elif event_type == "payment_intent.payment_failed":
            data = event["data"]["object"]
            order_id = data.get("metadata", {}).get("order_id")
            reason = data.get("last_payment_error", {}).get("message", "unknown")
            await PAYMENT_REPO.update_by_order(order_id, status="failed", gateway_event_id=event_id)
            logger.warning(f"[payment] Payment failed via webhook - order_id={order_id} reason={reason}.")
            await PRODUCER.publish(
                topic="payment.failed",
                key=order_id,
                payload={"order_id": order_id, "status": "failed", "reason": reason},
            )

        return {"received": True}

    async def get_payment(self, payment_id: str) -> Optional[dict]:
        cache_key = f"payment:{payment_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            return cached
        pmt = await PAYMENT_REPO.get(payment_id)
        if pmt:
            await CACHE.set(cache_key, pmt, ttl=120)
        return pmt

    @staticmethod
    def _verify_stripe_signature(raw_body: bytes, signature: str) -> None:
        try:
            parts = dict(item.split("=", 1) for item in signature.split(","))
            timestamp = parts.get("t")
            sig_v1 = parts.get("v1")
        except Exception:
            raise ValueError("Malformed Stripe-Signature header.")

        if not timestamp or not sig_v1:
            raise ValueError("Missing timestamp or signature in Stripe header.")

        if abs(time.time() - int(timestamp)) > 300:
            raise ValueError("Stripe webhook timestamp too old.")

        payload_to_sign = f"{timestamp}.{raw_body.decode('utf-8')}"
        expected = hmac.new(
            STRIPE_WEBHOOK_SECRET.encode(),
            payload_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, sig_v1):
            raise ValueError("Stripe signature verification failed.")
