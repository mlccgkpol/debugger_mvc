"""
user-service/src/api/addresses.py

User delivery address management.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.services.address_service import AddressService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = AddressService()


class AddressRequest(BaseModel):
    label: str
    line1: str
    line2: Optional[str] = None
    city: str
    state: str
    pincode: str
    lat: Optional[float] = None
    lng: Optional[float] = None


@router.post("")
async def add_address(
    body: AddressRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    result = await svc.add_address(token=token, **body.model_dump())
    return result


@router.get("/{address_id}")
async def get_address(
    address_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    addr = await svc.get_address(address_id=address_id, token=token)
    if addr is None:
        raise HTTPException(status_code=404, detail=f"Address {address_id} not found.")
    return addr


@router.get("")
async def list_addresses(authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    addresses = await svc.list_addresses(token=token)
    return {"addresses": addresses}
