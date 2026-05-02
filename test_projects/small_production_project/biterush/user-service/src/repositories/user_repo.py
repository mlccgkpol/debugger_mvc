"""
user-service/src/repositories/user_repo.py

User record persistence.
"""

from typing import Any, Optional
from src.infrastructure.db import Database
from shared.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class UserRepository:
    async def create(self, user_id: str, email: str, phone: str, name: str, pw_hash: str) -> None:
        conn = await _db.acquire()
        try:
            await conn.execute(
                "INSERT INTO users (user_id, email, phone, name, pw_hash, created_at) VALUES ($1,$2,$3,$4,$5,NOW())",
                user_id, email, phone, name, pw_hash,
            )
        finally:
            await _db.release(conn)

    async def get(self, user_id: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def get_by_email(self, email: str) -> Optional[dict[str, Any]]:
        conn = await _db.acquire()
        try:
            row = await conn.fetchrow("SELECT * FROM users WHERE email=$1", email)
            return dict(row) if row else None
        finally:
            await _db.release(conn)

    async def update(self, user_id: str, updates: dict) -> None:
        if not updates:
            return
        set_clauses = ", ".join(f"{k}=${i+2}" for i, k in enumerate(updates.keys()))
        values = list(updates.values())
        conn = await _db.acquire()
        try:
            await conn.execute(
                f"UPDATE users SET {set_clauses}, updated_at=NOW() WHERE user_id=$1",
                user_id, *values,
            )
        finally:
            await _db.release(conn)
