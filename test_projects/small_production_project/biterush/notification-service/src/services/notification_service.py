"""
notification-service/src/services/notification_service.py

Dispatches notifications via SMS, email, and push channels.

BUG #10 [VERY DEEP]: Thundering herd on resolve_user() cache miss.
resolve_user() checks Redis for a cached user_id->profile mapping.
On a cache miss it calls the user-service HTTP endpoint. If 200 pending
notifications all miss cache simultaneously (e.g., after a Redis restart
or first-time deployment), all 200 coroutines concurrently call
user-service for the same small set of user_ids. The user-service DB
pool (size=10) is exhausted, causing cascading 503s. Each 503 is then
retried by the notification service (see the retry loop) — up to 3 times
— resulting in up to 600 concurrent requests to a service that can handle
~10. No stampede protection (Redis lock, singleflight pattern) is used.
The retry backoff is also broken: it uses `await asyncio.sleep(attempt)`
where attempt starts at 0, so the first retry has zero delay.
"""

import asyncio
from typing import Optional

import httpx

from src.infrastructure.cache import CacheClient
from src.senders.sms_sender import SmsSender
from src.senders.email_sender import EmailSender
from src.senders.push_sender import PushSender
from shared.logger import AppLogger

logger = AppLogger(__name__)
CACHE = CacheClient()
SMS = SmsSender()
EMAIL = EmailSender()
PUSH = PushSender()

USER_SERVICE_URL = "http://user-service.biterush.svc:8003"
ORDER_SERVICE_URL = "http://order-service.biterush.svc:8000"


class NotificationService:
    async def resolve_user(self, order_id: str) -> Optional[str]:
        cache_key = f"notif:user_for_order:{order_id}"
        cached = await CACHE.get(cache_key)
        if cached:
            return cached

        # BUG #10: No stampede protection. All concurrent misses hammer user-service.
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}")
                    resp.raise_for_status()
                    user_id = resp.json().get("user_id")
                    await CACHE.set(cache_key, user_id, ttl=3600)
                    return user_id
            except Exception as exc:
                logger.warning(
                    f"[notif] resolve_user failed attempt={attempt} order_id={order_id}: {exc}"
                )
                # BUG #10: attempt=0 on first retry -> sleep(0) -> immediate retry, no backoff.
                await asyncio.sleep(attempt)
        logger.error(f"[notif] Could not resolve user_id for order_id={order_id}.")
        return None

    async def send(self, user_id: Optional[str], template: str, channels: str, context: dict) -> None:
        if user_id is None:
            logger.warning(f"[notif] Cannot send {template} - user_id is None.")
            return

        profile_key = f"notif:profile:{user_id}"
        profile = await CACHE.get(profile_key)
        if profile is None:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{USER_SERVICE_URL}/api/v1/users/{user_id}")
                if resp.status_code == 200:
                    profile = resp.json()
                    await CACHE.set(profile_key, profile, ttl=1800)

        if profile is None:
            logger.error(f"[notif] User profile not found - user_id={user_id}.")
            return

        logger.info(
            f"[notif] Sending notification - user_id={user_id} template={template} "
            f"channels={channels}."
        )

        if "sms" in channels and profile.get("phone"):
            try:
                await SMS.send(phone=profile["phone"], template=template, context=context)
                logger.info(f"[notif] SMS sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] SMS failed - user_id={user_id}: {exc}")

        if "email" in channels and profile.get("email"):
            try:
                await EMAIL.send(email=profile["email"], template=template, context=context)
                logger.info(f"[notif] Email sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] Email failed - user_id={user_id}: {exc}")

        if "push" in channels and profile.get("device_token"):
            try:
                await PUSH.send(device_token=profile["device_token"], template=template, context=context)
                logger.info(f"[notif] Push sent - user_id={user_id} template={template}.")
            except Exception as exc:
                logger.error(f"[notif] Push failed - user_id={user_id}: {exc}")
