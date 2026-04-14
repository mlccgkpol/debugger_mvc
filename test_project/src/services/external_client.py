"""
src/services/external_client.py

Simulated HTTP client for the upstream DataBridge provider.

In production this would use httpx or aiohttp; here the responses are
hard-coded to exercise specific failure scenarios without network I/O.
"""

from typing import Any


class ExternalClient:
    """Thin wrapper around the (simulated) upstream REST API."""

    BASE_URL: str = "https://provider.internal/api"

    # ── Feed endpoint ─────────────────────────────────────────────────────────

    def fetch_feed(self, feed_id: str) -> dict[str, Any]:
        """
        Retrieve a raw feed payload from the upstream provider.

        Args:
            feed_id: Unique identifier for the feed stream.

        Returns:
            Parsed JSON payload from the upstream provider.

        Raises:
            ConnectionError: When the upstream host is unreachable.  ← BUG #1
        """
        # Simulate a transient DNS / TCP failure for *every* feed_id.
        # The upstream provider is misconfigured and always returns 503.
        raise ConnectionError(                      # line 34
            f"Failed to connect to {self.BASE_URL}/feeds/{feed_id}: "
            "upstream host returned 503 Service Unavailable."
        )

    # ── Record endpoint ───────────────────────────────────────────────────────

    def fetch_record(self, record_id: int) -> dict[str, Any]:
        """
        Retrieve a single record from the upstream provider.

        Args:
            record_id: Numeric primary key for the record.

        Returns:
            Parsed JSON payload — **intentionally malformed** (score is a
            string instead of the expected int).  ← BUG #2 root cause
        """
        # The upstream API has a serialisation bug: 'score' is returned as a
        # quoted string rather than a bare integer.
        return {
            "id": record_id,
            "name": f"Record-{record_id}",
            "score": "not-a-number",               # should be int, e.g. 42
            "active": True,
        }
