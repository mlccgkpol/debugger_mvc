"""
order-service/src/consumers/inventory_consumer.py

Listens to inventory.reserved and inventory.reservation_failed topics.
"""

from src.repositories.order_repo import OrderRepository
from shared.kafka_client import KafkaConsumer
from shared.logger import AppLogger

logger = AppLogger(__name__)
ORDER_REPO = OrderRepository()


class InventoryEventConsumer:
    def __init__(self):
        self._consumer = KafkaConsumer(
            topics=["inventory.reserved", "inventory.reservation_failed"],
            group_suffix="order-service.inventory",
        )

    async def run(self):
        await self._consumer.start(self._handle)

    async def _handle(self, key: str, payload: dict) -> None:
        order_id = payload.get("order_id")
        event_type = payload.get("event_type")
        logger.info(
            f"[order/inventory-consumer] Received inventory event - "
            f"order_id={order_id} type={event_type}."
        )
        if event_type == "reservation_failed":
            await ORDER_REPO.update_status(order_id, "cancelled")
            logger.info(
                f"[order/inventory-consumer] Order cancelled due to stock failure - "
                f"order_id={order_id}."
            )
