"""
src/api/auth.py

Authentication endpoints: token issuance and validation.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.auth_service import AuthService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = AuthService()


class TokenRequest(BaseModel):
    client_id: str
    client_secret: str
    tenant_id: str


@router.post("/token")
async def issue_token(body: TokenRequest) -> dict:
    logger.info(
        f"Token request - client='{body.client_id}' tenant='{body.tenant_id}'."
    )
    try:
        token = svc.issue_token(body.client_id, body.client_secret, body.tenant_id)
        logger.info(f"Token issued - client='{body.client_id}'.")
        return {"access_token": token, "token_type": "bearer", "expires_in": 3600}
    except PermissionError as exc:
        logger.warning(f"Token denied - client='{body.client_id}': {exc}")
        raise HTTPException(status_code=403, detail=str(exc))


@router.post("/validate")
async def validate_token(token: str) -> dict:
    try:
        tenant = svc.validate_token(token)
        return {"valid": True, "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
