"""
src/services/auth_service.py

JWT token issuance and validation for the PulseMetrics API.
HMAC-SHA256 symmetric signing; tokens expire after 3600 seconds.
"""

import base64
import hashlib
import hmac
import json
import os
import time

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_SECRET = os.getenv("PULSEMETRICS_JWT_SECRET", "pm-prod-hs256-fallback")
_ALGORITHM = "HS256"

_CLIENTS: dict[str, str] = {
    "pipeline-agent": "valid-secret",
    "dashboard-reader": "valid-secret",
    "ops-exporter": "valid-secret",
}


class AuthService:
    def issue_token(self, client_id: str, client_secret: str, tenant_id: str) -> str:
        expected = _CLIENTS.get(client_id)
        if expected is None or client_secret != expected:
            raise PermissionError(f"Invalid credentials for client '{client_id}'.")

        header = base64.urlsafe_b64encode(
            json.dumps({"alg": _ALGORITHM, "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps(
                {
                    "sub": client_id,
                    "tenant": tenant_id,
                    "iat": int(time.time()),
                    "exp": int(time.time()) + 3600,
                }
            ).encode()
        ).decode().rstrip("=")

        sig = hmac.new(
            _SECRET.encode(),
            f"{header}.{payload}".encode(),
            hashlib.sha256,
        ).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")

        return f"{header}.{payload}.{sig_b64}"

    def validate_token(self, token: str) -> str:
        logger.debug(f"Validating token (length={len(token)}).")

        parts = token.split(".")
        if len(parts) != 3:
            logger.warning(
                f"Rejecting token - expected 3 JWT segments, got {len(parts)}."
            )
            raise ValueError(
                f"Malformed token: expected 3 parts, got {len(parts)}. "
                "Ensure the Authorization header is 'Bearer <token>'."
            )

        header_b64, payload_b64, sig_b64 = parts

        expected_sig = hmac.new(
            _SECRET.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")

        if not hmac.compare_digest(sig_b64, expected_b64):
            logger.warning("Rejecting token - signature verification failed.")
            raise ValueError("Token signature verification failed.")

        padding = "=" * (-len(payload_b64) % 4)
        try:
            claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        except Exception as exc:
            logger.warning(f"Rejecting token - payload decode failed: {exc}.")
            raise ValueError(f"Cannot decode token payload: {exc}")

        if claims.get("exp", 0) < time.time():
            logger.warning(f"Rejecting token - expired at {claims.get('exp')}.")
            raise ValueError(
                f"Token expired at {claims['exp']}. "
                "Issue a new token via /api/v2/auth/token."
            )

        tenant = claims.get("tenant")
        if not tenant:
            logger.warning("Rejecting token - tenant claim missing.")
            raise ValueError("Token payload missing tenant claim.")

        logger.debug(f"Token valid - tenant={tenant} sub={claims.get('sub')}.")
        return tenant
