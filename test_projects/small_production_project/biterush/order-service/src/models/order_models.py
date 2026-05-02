"""
order-service/src/models/order_models.py

Pydantic response models for orders.
"""

from typing import Any, Optional
from pydantic import BaseModel


class OrderItem(BaseModel):
    item_id: str
    name: str
    unit_price: float
    quantity: int
    variant_id: Optional[str] = None


class OrderResponse(BaseModel):
    order_id: str
    user_id: str
    status: str
    items: list[OrderItem]
    subtotal: float
    discount: float
    gst: float
    delivery_fee: float
    total: float
    payment_method: str
    estimated_delivery_minutes: Optional[int] = None
