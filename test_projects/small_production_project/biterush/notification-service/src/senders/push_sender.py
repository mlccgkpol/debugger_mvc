"""
notification-service/src/senders/push_sender.py

Push notification delivery via Firebase Cloud Messaging (FCM).
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

FCM_SERVER_KEY = os.getenv("FCM_SERVER_KEY", "fcm_test_biterush")
FCM_URL = "https://fcm.googleapis.com/fcm/send"


class PushSender:
    async def send(self, device_token: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        svc = TemplateService()
        title = svc.render(template, "push_title", context)
        body = svc.render(template, "push_body", context)
        logger.debug(f"[push] Sending token={device_token[:8]}... template={template}.")
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                FCM_URL,
                headers={"Authorization": f"key={FCM_SERVER_KEY}"},
                json={
                    "to": device_token,
                    "notification": {"title": title, "body": body},
                    "data": {"order_id": context.get("order_id"), "template": template},
                },
            )
            resp.raise_for_status()
            logger.debug(f"[push] Delivered - token={device_token[:8]}...")
