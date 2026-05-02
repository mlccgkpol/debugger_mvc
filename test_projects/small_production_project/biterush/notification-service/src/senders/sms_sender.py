"""
notification-service/src/senders/sms_sender.py

SMS delivery via Exotel (India-primary) with Twilio fallback.
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

EXOTEL_SID = os.getenv("EXOTEL_SID", "biterush_sid")
EXOTEL_TOKEN = os.getenv("EXOTEL_TOKEN", "exotel_token_test")
EXOTEL_URL = f"https://api.exotel.com/v1/Accounts/{EXOTEL_SID}/Sms/send"


class SmsSender:
    async def send(self, phone: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        body = TemplateService().render(template, "sms", context)
        logger.debug(f"[sms] Sending to={phone} template={template}.")
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                EXOTEL_URL,
                auth=(EXOTEL_SID, EXOTEL_TOKEN),
                data={"From": "BiteRush", "To": phone, "Body": body},
            )
            resp.raise_for_status()
            logger.debug(f"[sms] Delivered - phone={phone} status={resp.status_code}.")
