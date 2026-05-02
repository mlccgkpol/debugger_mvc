"""
user-service/src/services/user_service.py

User profile management.
"""

from typing import Any, Optional

from src.repositories.user_repo import UserRepository
from src.services.auth_service import AuthService
from shared.logger import AppLogger

logger = AppLogger(__name__)
USER_REPO = UserRepository()
AUTH_SVC = AuthService()


class UserService:
    async def get_user(self, user_id: str, token: str) -> Optional[dict[str, Any]]:
        requesting_user_id = AUTH_SVC.validate_token(token)
        user = await USER_REPO.get(user_id)
        if user is None:
            return None
        return {k: v for k, v in user.items() if k != "pw_hash"}

    async def update_user(self, user_id: str, token: str, updates: dict) -> dict:
        requesting_user_id = AUTH_SVC.validate_token(token)
        if requesting_user_id != user_id:
            raise PermissionError("Cannot update another user's profile.")
        await USER_REPO.update(user_id=user_id, updates=updates)
        user = await USER_REPO.get(user_id)
        return {k: v for k, v in user.items() if k != "pw_hash"}
