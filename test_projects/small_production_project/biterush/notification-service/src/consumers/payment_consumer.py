"""
notification-service/src/consumers/payment_consumer.py

Handles payment events for receipt and refund notifications.
"""

from src.services.notification_service import NotificationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
NOTIF_SVC = NotificationService()


class PaymentNotificationConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["payment.completed", "payment.failed", "payment.refunded"],
            group_suffix="notification-service.payments",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        status = payload.get("status") or ("refunded" if "amount" in payload and "reason" in payload else "unknown")
        logger.info(f"[notif/payment-consumer] Payment event - order_id={order_id} status={status}.")

        templates = {
            "completed": ("payment_receipt", "email_push"),
            "failed": ("payment_failed", "sms_push"),
            "refunded": ("refund_initiated", "sms_email"),
        }
        tmpl, channels = templates.get(status, (None, None))
        if tmpl is None:
            return

        user_id = await NOTIF_SVC.resolve_user(order_id)
        await NOTIF_SVC.send(user_id=user_id, template=tmpl, channels=channels, context=payload)
