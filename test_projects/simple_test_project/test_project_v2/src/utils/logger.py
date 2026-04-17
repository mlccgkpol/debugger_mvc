"""
src/utils/logger.py

Structured application logger.

Format : YYYY-MM-DD_HH:MM:SS | LEVEL | filename             | LNN  | message
Levels : DEBUG, INFO, WARNING, ERROR

A chain-divider line is appended after every HTTP response entry so that
request chains are visually separated in the log file.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT      = "%(asctime)s | %(levelname)-7s | %(filename)-22s | L%(lineno)-4d | %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"

_CHAIN_DIVIDER = "--- " * 22


def _log_path() -> Path:
    return LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.text"


class AppLogger:
    """Thin facade over stdlib logging with file + stderr sinks."""

    _LEVELS = {
        "DEBUG":   logging.DEBUG,
        "INFO":    logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR":   logging.ERROR,
    }

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.setLevel(logging.DEBUG)
            fmt = logging.Formatter(_FMT, datefmt=_DATE_FMT)
            fh  = logging.FileHandler(_log_path(), encoding="utf-8")
            fh.setFormatter(fmt)
            sh  = logging.StreamHandler(sys.stderr)
            sh.setFormatter(fmt)
            self._logger.addHandler(fh)
            self._logger.addHandler(sh)

    def debug(self, msg: str)   -> None: self._logger.debug(msg)
    def info(self, msg: str)    -> None: self._logger.info(msg);    self._divider(msg)
    def warning(self, msg: str) -> None: self._logger.warning(msg)
    def error(self, msg: str)   -> None: self._logger.error(msg);   self._divider(msg)

    def log(self, level: str, msg: str) -> None:
        lvl = self._LEVELS.get(level.upper(), logging.INFO)
        self._logger.log(lvl, msg)
        if "→ HTTP" in msg:
            self._write_divider()

    def _divider(self, msg: str) -> None:
        if "→ HTTP" in msg:
            self._write_divider()

    def _write_divider(self) -> None:
        try:
            with open(_log_path(), "a", encoding="utf-8") as fh:
                fh.write(_CHAIN_DIVIDER + "\n")
        except OSError:
            pass
