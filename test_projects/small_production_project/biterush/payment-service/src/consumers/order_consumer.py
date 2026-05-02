"""
payment-service/src/consumers/order_consumer.py

Listens to order.cancelled to trigger refund if payment was already completed.
"""

from src.services.refund_service import RefundService
from src.repositories.payment_repo import PaymentRepository
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
PAYMENT_REPO = PaymentRepository()
REFUND_SVC = RefundService()


class OrderPaymentConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.cancelled"],
            group_suffix="payment-service.orders",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        logger.info(f"[payment/order-consumer] Order cancelled - order_id={order_id}.")
        pmt = await PAYMENT_REPO.get_by_order(order_id)
        if pmt and pmt["status"] == "completed":
            logger.info(f"[payment/order-consumer] Initiating auto-refund - order_id={order_id}.")
            try:
                await REFUND_SVC.refund(order_id=order_id, amount=float(pmt["amount"]), reason="order_cancelled")
            except Exception as exc:
                logger.error(f"[payment/order-consumer] Auto-refund failed - order_id={order_id}: {exc}")
