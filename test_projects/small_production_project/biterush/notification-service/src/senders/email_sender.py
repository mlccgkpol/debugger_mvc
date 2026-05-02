"""
notification-service/src/senders/email_sender.py

Email delivery via SendGrid.
"""

import os
import httpx
from shared.logger import AppLogger

logger = AppLogger(__name__)

SENDGRID_KEY = os.getenv("SENDGRID_API_KEY", "SG.test_biterush")
SENDGRID_FROM = os.getenv("SENDGRID_FROM", "noreply@biterush.io")
SENDGRID_URL = "https://api.sendgrid.com/v3/mail/send"


class EmailSender:
    async def send(self, email: str, template: str, context: dict) -> None:
        from src.services.template_service import TemplateService
        svc = TemplateService()
        subject = svc.render(template, "email_subject", context)
        body = svc.render(template, "email_body", context)
        logger.debug(f"[email] Sending to={email} template={template}.")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                SENDGRID_URL,
                headers={"Authorization": f"Bearer {SENDGRID_KEY}"},
                json={
                    "personalizations": [{"to": [{"email": email}]}],
                    "from": {"email": SENDGRID_FROM},
                    "subject": subject,
                    "content": [{"type": "text/plain", "value": body}],
                },
            )
            resp.raise_for_status()
            logger.debug(f"[email] Delivered - email={email}.")
