"""
user-service/src/api/auth.py

Registration, login, token refresh, and /me endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, EmailStr

from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = AuthService()


class RegisterRequest(BaseModel):
    email: EmailStr
    phone: str
    name: str
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


@router.post("/register")
async def register(body: RegisterRequest) -> dict:
    logger.info(f"[auth] Register request - email={body.email}.")
    try:
        result = await svc.register(email=body.email, phone=body.phone, name=body.name, password=body.password)
        logger.info(f"[auth] User registered - user_id={result['user_id']}.")
        return result
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/login")
async def login(body: LoginRequest) -> dict:
    logger.info(f"[auth] Login request - email={body.email}.")
    try:
        result = await svc.login(email=body.email, password=body.password)
        logger.info(f"[auth] Login successful - user_id={result['user_id']}.")
        return result
    except PermissionError as exc:
        logger.warning(f"[auth] Login failed - email={body.email}: {exc}")
        raise HTTPException(status_code=401, detail=str(exc))


@router.get("/me")
async def me(authorization: Optional[str] = Header(default=None)) -> dict:
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing token.")
    try:
        user = await svc.me(token=token)
        return user
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
