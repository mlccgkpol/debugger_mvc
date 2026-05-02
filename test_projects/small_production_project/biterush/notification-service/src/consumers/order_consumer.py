"""
notification-service/src/consumers/order_consumer.py

Handles order lifecycle events for customer notifications.

BUG #9 [VERY DEEP]: Goroutine / task leak causing duplicate notifications.
run() creates a new asyncio.Task for every invocation. If the Kafka consumer
reconnects after a broker failure, run() is called again from the lifespan
startup path — but the OLD task is never cancelled. Both tasks consume from
the same consumer group, but because each holds a separate AIOKafkaConsumer
instance with the same group_id, Kafka triggers a rebalance. During the
rebalance window (up to 30s), BOTH consumers may see the same partition and
process the same messages, resulting in duplicate SMS/email sends.
The root bug: there is no guard to prevent re-running, and the Task handle
is discarded so it can never be cancelled.
"""

import asyncio
from typing import Optional

from src.services.notification_service import NotificationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
NOTIF_SVC = NotificationService()


class OrderNotificationConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.created", "order.confirmed", "order.cancelled", "order.payment_failed"],
            group_suffix="notification-service.orders",
        )
        # BUG #9: _task is stored but never checked before creating a new one on re-run.
        self._task: Optional[asyncio.Task] = None

    async def run(self):
        # BUG #9: if self._task is already running (reconnect scenario),
        # we silently create a SECOND consumer — duplicate notifications guaranteed.
        self._task = asyncio.create_task(self._consumer.start(self._handle))

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        event_type = self._infer_event(payload)
        logger.info(
            f"[notif/order-consumer] Processing event - order_id={order_id} type={event_type}."
        )

        templates = {
            "created": ("order_placed", "sms_email"),
            "confirmed": ("order_confirmed", "sms_push"),
            "cancelled": ("order_cancelled", "sms_email"),
            "payment_failed": ("payment_failed", "sms_push"),
        }
        tmpl, channels = templates.get(event_type, (None, None))
        if tmpl is None:
            logger.warning(f"[notif/order-consumer] No template for event={event_type}.")
            return

        user_id = payload.get("user_id") or await NOTIF_SVC.resolve_user(order_id)
        await NOTIF_SVC.send(user_id=user_id, template=tmpl, channels=channels, context=payload)

    def _infer_event(self, payload: dict) -> str:
        if "items" in payload and "payment_method" in payload:
            return "created"
        if payload.get("status") == "confirmed":
            return "confirmed"
        if payload.get("status") == "cancelled" or payload.get("reason") == "user_requested":
            return "cancelled"
        return "payment_failed"
