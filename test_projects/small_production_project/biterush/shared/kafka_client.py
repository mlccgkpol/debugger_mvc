"""
shared/kafka_client.py

aiokafka producer / consumer wrappers for BiteRush services.
Each service instantiates its own producer; consumers run as background tasks.
"""

import asyncio
import json
import os
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from shared.logger import AppLogger

logger = AppLogger(__name__)

_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092")
_GROUP_PREFIX = os.getenv("KAFKA_GROUP_PREFIX", "biterush")


class KafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._producer = None
            cls._instance = inst
        return cls._instance

    async def start(self):
        if self._producer is not None:
            return
        logger.info(f"[kafka] Starting producer - brokers={_BROKERS}.")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            acks="all",
            enable_idempotence=True,
            max_batch_size=65536,
            linger_ms=5,
        )
        await self._producer.start()
        logger.info("[kafka] Producer ready.")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            self._producer = None
        logger.info("[kafka] Producer stopped.")

    async def publish(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer not started.")
        logger.debug(f"[kafka] Publishing to topic={topic} key={key}.")
        await self._producer.send_and_wait(
            topic,
            key=key.encode("utf-8"),
            value=payload,
        )
        logger.debug(f"[kafka] Published to topic={topic} key={key}.")


class KafkaConsumer:
    def __init__(self, topics: list[str], group_suffix: str):
        self._topics = topics
        self._group_id = f"{_GROUP_PREFIX}.{group_suffix}"
        self._consumer = None

    async def start(self, handler: Callable[[str, dict], Awaitable[None]]) -> None:
        logger.info(
            f"[kafka] Starting consumer group={self._group_id} topics={self._topics}."
        )
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=_BROKERS,
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=50,
        )
        await self._consumer.start()
        logger.info(f"[kafka] Consumer ready - group={self._group_id}.")

        try:
            async for msg in self._consumer:
                key = msg.key.decode("utf-8") if msg.key else "none"
                logger.debug(
                    f"[kafka] Received topic={msg.topic} partition={msg.partition} "
                    f"offset={msg.offset} key={key}."
                )
                try:
                    await handler(key, msg.value)
                    await self._consumer.commit()
                except Exception as exc:
                    logger.error(
                        f"[kafka] Handler failed for topic={msg.topic} key={key}: {exc}"
                    )
        except asyncio.CancelledError:
            logger.info(f"[kafka] Consumer cancelled - group={self._group_id}.")
        finally:
            await self._consumer.stop()
            logger.info(f"[kafka] Consumer stopped - group={self._group_id}.")
