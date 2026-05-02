"""
user-service/src/services/address_service.py

Delivery address management.

BUG #8 [HARD]: IDOR (Insecure Direct Object Reference) in get_address.
get_address() fetches the address by address_id from the DB but does NOT
verify that the address belongs to the requesting user. Any authenticated
user can retrieve any other user's delivery address by guessing/enumerating
address IDs, since IDs are sequential integers in Postgres. This is a
textbook IDOR — the authorization check only verifies that the user has
a valid token, not that the resource belongs to them.
"""

from typing import Any, Optional

from src.repositories.address_repo import AddressRepository
from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
ADDR_REPO = AddressRepository()
AUTH_SVC = AuthService()


class AddressService:
    async def add_address(self, token: str, **kwargs) -> dict[str, Any]:
        user_id = AUTH_SVC.validate_token(token)
        address_id = await ADDR_REPO.create(user_id=user_id, **kwargs)
        logger.info(f"[address] Address added - user_id={user_id} address_id={address_id}.")
        return {"address_id": address_id, "user_id": user_id, **kwargs}

    async def get_address(self, address_id: str, token: str) -> Optional[dict[str, Any]]:
        # BUG #8: token is validated (user is authenticated) but the returned
        # address is NOT checked against the user's own user_id.
        AUTH_SVC.validate_token(token)  # only checks auth, not ownership
        addr = await ADDR_REPO.get(address_id)
        logger.debug(f"[address] Fetched address - address_id={address_id}.")
        return addr  # could be any user's address

    async def list_addresses(self, token: str) -> list[dict[str, Any]]:
        user_id = AUTH_SVC.validate_token(token)
        return await ADDR_REPO.list_by_user(user_id)
