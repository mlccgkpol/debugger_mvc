"""
notification-service/src/services/template_service.py

Notification template resolution and rendering.
"""

TEMPLATES = {
    "order_placed": {
        "sms": "Hi {name}! Your BiteRush order #{order_id} has been placed. Total: ₹{total}.",
        "email_subject": "Order Placed - #{order_id}",
        "email_body": "Dear {name},\n\nYour order #{order_id} has been placed successfully.\nTotal: ₹{total}\nEstimated delivery: 35 minutes.\n\nThank you,\nBiteRush",
        "push_title": "Order Placed!",
        "push_body": "Your order #{order_id} is confirmed. Delivery in ~35 mins.",
    },
    "order_confirmed": {
        "sms": "Great news, {name}! Order #{order_id} confirmed by the restaurant.",
        "push_title": "Order Confirmed!",
        "push_body": "Restaurant accepted #{order_id}. Getting it ready!",
    },
    "order_cancelled": {
        "sms": "Order #{order_id} has been cancelled. Refund will be processed in 5-7 days.",
        "email_subject": "Order Cancelled - #{order_id}",
        "email_body": "Hi {name},\n\nYour order #{order_id} has been cancelled.\nAny charges will be refunded in 5-7 business days.\n\nBiteRush Support",
    },
    "payment_receipt": {
        "email_subject": "Payment Receipt - ₹{amount}",
        "email_body": "Hi {name},\n\nPayment of ₹{amount} received for order #{order_id}.\n\nBiteRush",
        "push_title": "Payment Confirmed",
        "push_body": "₹{amount} paid for order #{order_id}.",
    },
    "refund_initiated": {
        "sms": "Refund of ₹{amount} for order #{order_id} is being processed.",
        "email_subject": "Refund Initiated - ₹{amount}",
        "email_body": "Hi {name},\n\nYour refund of ₹{amount} for order #{order_id} has been initiated. Expected in 5-7 days.\n\nBiteRush",
    },
}


class TemplateService:
    def render(self, template_name: str, channel: str, context: dict) -> str:
        tmpl = TEMPLATES.get(template_name, {})
        key = channel
        text = tmpl.get(key, "")
        try:
            return text.format(**context)
        except KeyError:
            return text
