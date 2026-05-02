"""
order-service/src/consumers/payment_consumer.py

Listens to payment.completed and payment.failed topics.
Updates order status accordingly and triggers downstream events.

BUG #3 [HARD]: Offset is committed even when handler raises an exception.
In shared/kafka_client.py the consumer commits after every message regardless
of whether the handler succeeded. This consumer catches exceptions internally
but the base consumer class commits unconditionally after the handler returns.
Here the internal exception swallowing means failures appear to succeed —
order status is never updated for failed payment events, and the message
is committed so it is never retried.
"""

import asyncio

from src.repositories.order_repo import OrderRepository
from shared.kafka_client import KafkaConsumer
from shared.kafka_client import KafkaProducer
from shared.logger import AppLogger

logger = AppLogger(__name__)
ORDER_REPO = OrderRepository()
PRODUCER = KafkaProducer()


class PaymentEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["payment.completed", "payment.failed"],
            group_suffix="order-service.payment",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        status = payload.get("status")
        logger.info(f"[order/payment-consumer] Received payment event - order_id={order_id} status={status}.")

        if status == "completed":
            try:
                await ORDER_REPO.update_status(order_id, "confirmed")
                logger.info(f"[order/payment-consumer] Order confirmed - order_id={order_id}.")
                await PRODUCER.publish(
                    topic="order.confirmed",
                    key=order_id,
                    payload={"order_id": order_id, "status": "confirmed"},
                )
            except Exception as exc:
                # BUG #3: swallowing exception here means commit happens anyway;
                # the order stuck in awaiting_payment is never retried.
                logger.error(
                    f"[order/payment-consumer] Failed to update confirmed order "
                    f"order_id={order_id}: {exc}"
                )

        elif status == "failed":
            try:
                await ORDER_REPO.update_status(order_id, "payment_failed")
                logger.info(f"[order/payment-consumer] Order marked payment_failed - order_id={order_id}.")
                await PRODUCER.publish(
                    topic="order.payment_failed",
                    key=order_id,
                    payload={"order_id": order_id, "reason": payload.get("reason")},
                )
            except Exception as exc:
                logger.error(
                    f"[order/payment-consumer] Failed to update failed order "
                    f"order_id={order_id}: {exc}"
                )
