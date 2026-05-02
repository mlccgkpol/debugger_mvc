"""
payment-service/src/services/refund_service.py

Refund orchestration: validates refundability, calls gateway, records result.

BUG #6 [VERY DEEP]: Partial refund amount precision loss via float arithmetic.
Refund eligibility is checked by comparing payment.amount (stored as NUMERIC
in Postgres, returned as Python Decimal) against the requested refund amount
(a JSON float). The comparison uses Python == on Decimal vs float, which can
return False for amounts that are semantically equal (e.g., Decimal("199.90")
!= 199.9 due to float binary representation). This causes legitimate full
refunds to be incorrectly classified as partial refunds, which triggers the
partial-refund path in the Razorpay gateway. Razorpay's partial-refund API
requires the amount in paise (integer × 100), but the code passes the
floating-point amount directly without rounding, so e.g. 199.9 * 100 = 19989.999...
which is cast to int as 19989 instead of 19990 — the customer is under-refunded
by 1 paise every time. The root cause is three layers deep: JSON float → Decimal
comparison → paise conversion without rounding.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from src.gateway.razorpay_gateway import RazorpayGateway
from src.gateway.stripe_gateway import StripeGateway
from src.repositories.payment_repo import PaymentRepository
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
PAYMENT_REPO = PaymentRepository()
STRIPE = StripeGateway()
RAZORPAY = RazorpayGateway()
PRODUCER = KafkaProducer()


class RefundService:
    async def refund(self, order_id: str, amount: float, reason: str) -> dict[str, Any]:
        logger.debug(f"[refund] Processing refund - order_id={order_id} amount={amount}.")
        pmt = await PAYMENT_REPO.get_by_order(order_id)
        if pmt is None:
            raise ValueError(f"No payment found for order {order_id}.")
        if pmt["status"] != "completed":
            raise ValueError(
                f"Cannot refund order {order_id} - payment status is {pmt['status']}."
            )

        paid_amount = pmt["amount"]  # Decimal from Postgres NUMERIC column

        # BUG #6: float == Decimal comparison is unreliable.
        # e.g. Decimal("199.90") == 199.9 evaluates to False in Python.
        is_full_refund = paid_amount == amount

        logger.info(
            f"[refund] Refund type={'full' if is_full_refund else 'partial'} - "
            f"order_id={order_id} paid={paid_amount} requested={amount}."
        )

        if pmt["method"] == "card":
            gateway_ref = pmt.get("gateway_ref")
            result = await STRIPE.refund(
                payment_intent_id=gateway_ref,
                amount_paise=int(amount * 100),  # BUG #6: no rounding — 199.9 * 100 = 19989.999... -> 19989
                full=is_full_refund,
            )
        else:
            result = await RAZORPAY.refund(
                order_id=order_id,
                amount_paise=int(amount * 100),  # BUG #6 same here
                full=is_full_refund,
            )

        await PAYMENT_REPO.record_refund(order_id=order_id, amount=amount, reason=reason)
        logger.info(f"[refund] Refund recorded - order_id={order_id} gateway_result={result}.")

        await PRODUCER.publish(
            topic="payment.refunded",
            key=order_id,
            payload={"order_id": order_id, "amount": amount, "reason": reason},
        )
        return {"order_id": order_id, "refund_status": "processed", "amount": amount}
