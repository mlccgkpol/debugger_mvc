"""
inventory-service/src/consumers/order_consumer.py

Listens to order.created to trigger stock reservation.
Listens to order.cancelled to release reserved stock.
"""

from src.services.reservation_service import ReservationService
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
RSV_SVC = ReservationService()


class OrderEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["order.created", "order.cancelled"],
            group_suffix="inventory-service.orders",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        topic_hint = payload.get("event_type") or ("created" if "items" in payload else "cancelled")
        order_id = payload.get("order_id")
        logger.info(f"[inventory/order-consumer] Event received - order_id={order_id} hint={topic_hint}.")

        if "items" in payload:
            try:
                await RSV_SVC.reserve(order_id=order_id, items=payload["items"])
            except ValueError as exc:
                logger.warning(f"[inventory/order-consumer] Reservation failed - order_id={order_id}: {exc}")
        else:
            await RSV_SVC.release(order_id=order_id, items=payload.get("items", []))
