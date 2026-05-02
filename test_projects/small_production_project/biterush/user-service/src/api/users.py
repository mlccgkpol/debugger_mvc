"""
user-service/src/api/users.py

User profile endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.services.user_service import UserService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = UserService()


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None


@router.get("/{user_id}")
async def get_user(user_id: str, authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    user = await svc.get_user(user_id=user_id, token=token)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.patch("/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateProfileRequest,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    try:
        return await svc.update_user(user_id=user_id, token=token, updates=body.model_dump(exclude_none=True))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
