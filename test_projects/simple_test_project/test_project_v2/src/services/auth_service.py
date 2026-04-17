"""
src/services/auth_service.py

JWT token issuance and validation for the PulseMetrics API.
Uses a simple HMAC-SHA256 scheme (production would use RS256 + JWKS).
"""

import hmac
import hashlib
import base64
import json
import time

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_SECRET    = "pulse-secret-do-not-ship"
_ALGORITHM = "HS256"


class AuthService:
    """Issues and validates short-lived JWT bearer tokens."""

    def issue_token(self, client_id: str, client_secret: str, tenant_id: str) -> str:
        """
        Issue a signed token for a verified client.

        Raises:
            PermissionError: If client_secret is incorrect.
        """
        if client_secret != "valid-secret":
            raise PermissionError(f"Invalid credentials for client '{client_id}'.")

        header  = base64.urlsafe_b64encode(
            json.dumps({"alg": _ALGORITHM, "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({
                "sub":    client_id,
                "tenant": tenant_id,
                "iat":    int(time.time()),
                "exp":    int(time.time()) + 3600,
            }).encode()
        ).decode().rstrip("=")

        sig = hmac.new(
            _SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256
        ).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")

        return f"{header}.{payload}.{sig_b64}"

    def validate_token(self, token: str) -> str:
        """
        Validate a bearer token and return the tenant_id.

        Raises:
            ValueError: On malformed token, bad signature, or expiry.   ← BUG #4
        """
        logger.debug(f"Validating token (length={len(token)}).")

        parts = token.split(".")
        if len(parts) != 3:                          # line 57
            raise ValueError(                        # line 58 — raises on malformed/empty token
                f"Malformed token: expected 3 parts, got {len(parts)}. "
                "Ensure the Authorization header is 'Bearer <token>'."
            )

        header_b64, payload_b64, sig_b64 = parts

        # Verify signature
        expected_sig = hmac.new(
            _SECRET.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")

        if not hmac.compare_digest(sig_b64, expected_b64):
            raise ValueError("Token signature verification failed.")

        # Decode payload
        padding = "=" * (4 - len(payload_b64) % 4)
        try:
            claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        except Exception as exc:
            raise ValueError(f"Cannot decode token payload: {exc}")

        # Expiry check
        if claims.get("exp", 0) < time.time():
            raise ValueError(
                f"Token expired at {claims['exp']}. Issue a new token via /api/v2/auth/token."
            )

        tenant = claims.get("tenant")
        logger.debug(f"Token valid — tenant={tenant} sub={claims.get('sub')}.")
        return tenant
