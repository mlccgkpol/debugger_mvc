"""
src/utils/logger.py

Structured application logger.

Format: YYYY-MM-DD_HH:MM:SS | LEVEL | filename             | LNN  | message

Fields are pipe-separated with fixed-width padding so columns align visually.
Request chains (lines sharing a timestamp) are separated by a dashed divider
written when the HTTP response line (main.py) is emitted.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Fixed-width format — filename padded to 20 chars, level to 5 chars, line to 4 digits
# Example: 2026-04-12_09:15:45 | ERROR | data_processor.py   | L52  | ZeroDivisionError: ...
_FMT     = "%(asctime)s | %(levelname)-5s | %(filename)-20s | L%(lineno)-4d | %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"

_CHAIN_DIVIDER = "--- " * 22  # visual separator between request chains


def _log_path() -> Path:
    """Return today's log file path."""
    return LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.text"


def _make_formatter() -> logging.Formatter:
    return logging.Formatter(_FMT, datefmt=_DATE_FMT)


class AppLogger:
    """
    Thin facade over stdlib logging.

    Writes to a daily rotating file in logs/ and to stderr.
    Appends a chain-divider line after every HTTP response entry so that
    request chains are visually separated in the log file.
    """

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        self._name   = name

        if not self._logger.handlers:
            self._logger.setLevel(logging.INFO)

            fh = logging.FileHandler(_log_path(), encoding="utf-8")
            fh.setFormatter(_make_formatter())
            self._logger.addHandler(fh)

            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(_make_formatter())
            self._logger.addHandler(sh)

    def info(self, message: str) -> None:
        self._logger.info(message)
        self._maybe_write_divider(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)
        self._maybe_write_divider(message)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _maybe_write_divider(self, message: str) -> None:
        """
        Write a chain-divider after the HTTP response line so each
        request chain is visually separated in the log file.
        """
        if "→ HTTP" in message:
            try:
                with open(_log_path(), "a", encoding="utf-8") as fh:
                    fh.write(_CHAIN_DIVIDER + "\n")
            except OSError:
                pass