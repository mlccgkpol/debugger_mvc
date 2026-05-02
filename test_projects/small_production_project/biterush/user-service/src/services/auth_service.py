"""
user-service/src/services/auth_service.py

JWT-based authentication for BiteRush users.
Tokens are HMAC-SHA256 signed; refresh tokens stored in Redis.

BUG #7 [VERY DEEP]: Timing-safe comparison bypass via token length oracle.
validate_token() first checks len(parts) == 3 before doing any HMAC
verification. An attacker who can measure response time can distinguish
"malformed token" (fast, raises before HMAC) from "valid structure but
bad sig" (slower, runs HMAC). More critically, the refresh token stored
in Redis is looked up by user_id extracted from the (already-signature-
verified) payload — but me() calls validate_token() first and caches the
result in Redis under key "user:me:{token}" WITHOUT verifying that the
token's user_id matches the requesting user. If a valid token for user A
is used to call /me, the result is cached as "user:me:{tokenA}". Another
process that somehow obtains tokenA (e.g., from a log line, since the
token is logged at DEBUG level in auth.py line 22 via the Header display)
can retrieve user A's profile. The deeper bug is that the raw token value
is used as a cache key, which means cache poisoning is possible if the
token namespace is shared with other data, and that tokens are effectively
leaked via log files in any environment with DEBUG logging enabled.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Optional

from src.repositories.user_repo import UserRepository
from src.infrastructure.cache import CacheClient
from shared.logger import AppLogger

logger = AppLogger(__name__)

_SECRET = os.getenv("BITERUSH_JWT_SECRET", "biterush-prod-hs256-fallback")
_TTL = 7200

USER_REPO = UserRepository()
CACHE = CacheClient()


class AuthService:
    async def register(self, email: str, phone: str, name: str, password: str) -> dict:
        existing = await USER_REPO.get_by_email(email)
        if existing:
            raise ValueError(f"Email {email} already registered.")
        import bcrypt
        pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = f"USR-{str(uuid.uuid4())[:8].upper()}"
        await USER_REPO.create(user_id=user_id, email=email, phone=phone, name=name, pw_hash=pw_hash)
        token = self._issue_token(user_id)
        return {"user_id": user_id, "access_token": token, "token_type": "bearer"}

    async def login(self, email: str, password: str) -> dict:
        import bcrypt
        user = await USER_REPO.get_by_email(email)
        if user is None:
            raise PermissionError("Invalid email or password.")
        if not bcrypt.checkpw(password.encode(), user["pw_hash"].encode()):
            raise PermissionError("Invalid email or password.")
        token = self._issue_token(user["user_id"])
        logger.info(f"[auth_svc] Token issued - user_id={user['user_id']}.")
        return {"user_id": user["user_id"], "access_token": token, "token_type": "bearer"}

    async def me(self, token: str) -> dict:
        # BUG #7: raw token used as cache key — token leak via logs or shared cache
        # means an attacker can retrieve another user's profile.
        cache_key = f"user:me:{token}"
        cached = await CACHE.get(cache_key)
        if cached:
            logger.debug(f"[auth_svc] /me cache hit - user_id={cached.get('user_id')}.")
            return cached

        user_id = self.validate_token(token)
        user = await USER_REPO.get(user_id)
        if user is None:
            raise ValueError(f"User {user_id} not found.")
        profile = {k: v for k, v in user.items() if k != "pw_hash"}
        await CACHE.set(cache_key, profile, ttl=_TTL)
        return profile

    def validate_token(self, token: str) -> str:
        logger.debug(f"[auth_svc] Validating token (len={len(token)}).")
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError(f"Malformed token: expected 3 parts, got {len(parts)}.")
        header_b64, payload_b64, sig_b64 = parts
        expected_sig = hmac.new(
            _SECRET.encode(), f"{header_b64}.{payload_b64}".encode(), hashlib.sha256
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")
        if not hmac.compare_digest(sig_b64, expected_b64):
            raise ValueError("Token signature invalid.")
        padding = "=" * (-len(payload_b64) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        if claims.get("exp", 0) < time.time():
            raise ValueError("Token expired.")
        return claims["sub"]

    def _issue_token(self, user_id: str) -> str:
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({"sub": user_id, "iat": int(time.time()), "exp": int(time.time()) + _TTL}).encode()
        ).decode().rstrip("=")
        sig = hmac.new(_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
        return f"{header}.{payload}.{base64.urlsafe_b64encode(sig).decode().rstrip('=')}"
