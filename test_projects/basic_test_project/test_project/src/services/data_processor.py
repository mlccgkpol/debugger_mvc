"""
src/services/data_processor.py

Business-logic layer: validates upstream payloads and computes derived metrics.
"""

from typing import Any


class DataProcessor:
    """Validates and transforms raw feed records into domain objects."""

    # ── Schema validation ─────────────────────────────────────────────────────

    def normalise_record(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and normalise a raw record from the upstream provider.

        Args:
            raw: Unvalidated payload dict from ExternalClient.fetch_record().

        Returns:
            Normalised record with guaranteed type-safe fields.

        Raises:
            ValueError: If any field fails type validation.  ← BUG #2
        """
        # Strict int cast — raises ValueError when score is a non-numeric string.
        score = int(raw["score"])                   # line 28 — raises ValueError

        return {
            "id": raw["id"],
            "name": raw["name"],
            "score": score,
            "active": raw["active"],
        }

    # ── Score computation ──────────────────────────────────────────────────────

    def compute_score(self, record_id: int, divisor: int) -> float:
        """
        Compute a normalised score for a record.

        Args:
            record_id: ID used as the numerator in the score formula.
            divisor:   Scaling factor supplied by the caller.

        Returns:
            Floating-point normalised score.

        Raises:
            ZeroDivisionError: When divisor is 0.  ← BUG #3
        """
        # No guard for divisor == 0; caller is expected to validate — but
        # the API endpoint does not enforce divisor != 0 at the schema level.
        normalised = record_id / divisor            # line 52 — raises ZeroDivisionError
        return round(normalised * 100, 4)
