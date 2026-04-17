"""
test_project_generator.py

Generates a complex, realistic FastAPI test project with traceable failures
for testing an AI-driven MCP Debugger.
"""

from pathlib import Path

# ─────────────────────────────────────────────
# FILE CONTENTS
# ─────────────────────────────────────────────

MAIN_PY = '''\
"""
src/main.py

FastAPI application initialization, middleware configuration, and startup hooks.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time

from src.api.endpoints import router
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

app = FastAPI(
    title="DataBridge API",
    description="Internal service for aggregating and processing external data feeds.",
    version="1.4.2",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every incoming request and its response time."""
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        f"Request {request.method} {request.url.path} "
        f"completed in {duration_ms:.1f}ms → HTTP {response.status_code}"
    )
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all handler so unhandled exceptions return structured JSON."""
    logger.error(f"Unhandled exception on {request.url.path}: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


app.include_router(router, prefix="/api/v1")


@app.get("/health", tags=["ops"])
async def health_check() -> dict:
    """Liveness probe endpoint."""
    return {"status": "ok", "version": app.version}
'''

ENDPOINTS_PY = '''\
"""
src/api/endpoints.py

Route definitions for the DataBridge API.
Each endpoint delegates business logic to the service layer.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.services.external_client import ExternalClient
from src.services.data_processor import DataProcessor
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
client = ExternalClient()
processor = DataProcessor()


class ProcessRequest(BaseModel):
    feed_id: str
    record_id: int
    divisor: int = 10


# ── Route 1: Triggers ConnectionError (Bug #1) ──────────────────────────────

@router.get("/feeds/{feed_id}", tags=["feeds"])
async def get_feed(feed_id: str) -> dict:
    """
    Fetch a raw data feed from the upstream provider.

    Raises:
        HTTPException 503: When the upstream provider is unreachable.
    """
    logger.info(f"Fetching feed '{feed_id}' from upstream provider.")
    try:
        data = client.fetch_feed(feed_id)           # line 36 — raises ConnectionError
        return {"feed_id": feed_id, "data": data}
    except ConnectionError as exc:
        logger.error(f"Upstream provider unavailable for feed '{feed_id}': {exc}")
        raise HTTPException(status_code=503, detail=str(exc))


# ── Route 2: Triggers ValueError / Schema Mismatch (Bug #2) ─────────────────

@router.get("/records/{record_id}", tags=["records"])
async def get_record(record_id: int) -> dict:
    """
    Retrieve a processed record by its numeric ID.

    Raises:
        HTTPException 500: When the upstream payload fails schema validation.
    """
    logger.info(f"Retrieving record id={record_id}.")
    try:
        raw = client.fetch_record(record_id)        # returns malformed payload
        result = processor.normalise_record(raw)    # line 54 — raises ValueError
        return {"record_id": record_id, "result": result}
    except ValueError as exc:
        logger.error(f"Schema validation failed for record id={record_id}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


# ── Route 3: Triggers ZeroDivisionError on divisor=0 (Bug #3) ───────────────

@router.post("/process", tags=["process"])
async def process_record(body: ProcessRequest) -> dict:
    """
    Submit a processing job for a feed record.

    Raises:
        HTTPException 500: For unexpected computation errors.
    """
    logger.info(
        f"Processing feed='{body.feed_id}' record={body.record_id} "
        f"divisor={body.divisor}."
    )
    try:
        output = processor.compute_score(           # line 74 — raises ZeroDivisionError
            record_id=body.record_id,
            divisor=body.divisor,
        )
        return {"feed_id": body.feed_id, "score": output}
    except ZeroDivisionError as exc:
        logger.error(
            f"Computation error for feed='{body.feed_id}' "
            f"record={body.record_id}: {exc}"
        )
        raise HTTPException(status_code=500, detail="Division by zero in score computation.")


# ── Route 4: Healthy baseline ────────────────────────────────────────────────

@router.get("/summary", tags=["feeds"])
async def summary(limit: int = Query(default=10, ge=1, le=100)) -> dict:
    """Return a summary of the most recent feed records (no external calls)."""
    logger.info(f"Summary requested, limit={limit}.")
    records = [{"id": i, "status": "ok"} for i in range(1, limit + 1)]
    return {"count": len(records), "records": records}
'''

EXTERNAL_CLIENT_PY = '''\
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
'''

DATA_PROCESSOR_PY = '''\
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
'''

LOGGER_PY = '''\
"""
src/utils/logger.py

Structured application logger.

Format: YYYY-MM-DD_HH:MM:SS || filename || line_number || message
"""

import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT = "%(asctime)s || %(filename)s || %(lineno)d || %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"


def _build_handler(log_date: str) -> logging.FileHandler:
    handler = logging.FileHandler(LOG_DIR / f"{log_date}.text", encoding="utf-8")
    handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
    return handler


class AppLogger:
    """Thin facade over stdlib logging with file + stderr sinks."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.setLevel(logging.INFO)
            # stderr sink
            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
            self._logger.addHandler(sh)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)
'''

# ─────────────────────────────────────────────
# LOG FILE CONTENTS
# ─────────────────────────────────────────────

LOG_APR_11 = """\
2026-04-11_08:02:14 | INFO  | main.py             | L47  | Request GET /health completed in 1.2ms → HTTP 200
2026-04-11_08:15:30 | INFO  | endpoints.py        | L33  | Fetching feed 'market-data-eu' from upstream provider.
2026-04-11_08:15:30 | ERROR | external_client.py  | L34  | Failed to connect to https://provider.internal/api/feeds/market-data-eu: host returned 503.
2026-04-11_08:15:30 | ERROR | endpoints.py        | L39  | Upstream provider unavailable for feed 'market-data-eu': host returned 503.
2026-04-11_08:15:30 | INFO  | main.py             | L47  | Request GET /api/v1/feeds/market-data-eu completed in 18.4ms → HTTP 503
2026-04-11_09:00:05 | INFO  | endpoints.py        | L50  | Retrieving record id=1042.
2026-04-11_09:00:05 | ERROR | data_processor.py   | L28  | ValueError: invalid literal for int() with base 10: 'not-a-number'
2026-04-11_09:00:05 | ERROR | endpoints.py        | L57  | Schema validation failed for record id=1042: invalid literal for int().
2026-04-11_09:00:05 | INFO  | main.py             | L47  | Request GET /api/v1/records/1042 completed in 12.7ms → HTTP 500
2026-04-11_10:30:00 | INFO  | endpoints.py        | L67  | Processing feed='inventory' record=77 divisor=10.
2026-04-11_10:30:00 | INFO  | main.py             | L47  | Request POST /api/v1/process completed in 5.1ms → HTTP 200
2026-04-11_11:45:18 | INFO  | endpoints.py        | L33  | Fetching feed 'pricing-feed' from upstream provider.
2026-04-11_11:45:18 | ERROR | external_client.py  | L34  | Failed to connect to https://provider.internal/api/feeds/pricing-feed: 503.
2026-04-11_11:45:18 | ERROR | endpoints.py        | L39  | Upstream provider unavailable for feed 'pricing-feed': host returned 503.
2026-04-11_11:45:18 | INFO  | main.py             | L47  | Request GET /api/v1/feeds/pricing-feed completed in 14.9ms → HTTP 503
2026-04-11_14:22:44 | INFO  | endpoints.py        | L91  | Summary requested, limit=25.
2026-04-11_14:22:44 | INFO  | main.py             | L47  | Request GET /api/v1/summary completed in 2.3ms → HTTP 200
2026-04-11_16:05:01 | INFO  | endpoints.py        | L67  | Processing feed='orders' record=305 divisor=0.
2026-04-11_16:05:01 | ERROR | data_processor.py   | L52  | ZeroDivisionError: division by zero
2026-04-11_16:05:01 | ERROR | endpoints.py        | L81  | Computation error for feed='orders' record=305: division by zero
2026-04-11_16:05:01 | INFO  | main.py             | L47  | Request POST /api/v1/process completed in 9.8ms → HTTP 500
2026-04-11_17:58:33 | INFO  | endpoints.py        | L50  | Retrieving record id=2201.
2026-04-11_17:58:33 | ERROR | data_processor.py   | L28  | ValueError: invalid literal for int() with base 10: 'not-a-number'
2026-04-11_17:58:33 | ERROR | endpoints.py        | L57  | Schema validation failed for record id=2201: invalid literal for int().
2026-04-11_17:58:33 | INFO  | main.py             | L47  | Request GET /api/v1/records/2201 completed in 11.2ms → HTTP 500
"""

LOG_APR_12 = """\
2026-04-12_07:44:10 | INFO  | main.py             | L47  | Request GET /health completed in 0.9ms → HTTP 200
2026-04-12_08:30:22 | INFO  | endpoints.py        | L33  | Fetching feed 'realtime-quotes' from upstream provider.
2026-04-12_08:30:22 | ERROR | external_client.py  | L34  | Failed to connect to https://provider.internal/api/feeds/realtime-quotes: host returned 503.
2026-04-12_08:30:22 | ERROR | endpoints.py        | L39  | Upstream provider unavailable for feed 'realtime-quotes': host returned 503.
2026-04-12_08:30:22 | INFO  | main.py             | L47  | Request GET /api/v1/feeds/realtime-quotes completed in 20.1ms → HTTP 503
2026-04-12_09:15:45 | INFO  | endpoints.py        | L67  | Processing feed='settlements' record=99 divisor=0.
2026-04-12_09:15:45 | ERROR | data_processor.py   | L52  | ZeroDivisionError: division by zero
2026-04-12_09:15:45 | ERROR | endpoints.py        | L81  | Computation error for feed='settlements' record=99: division by zero
2026-04-12_09:15:45 | INFO  | main.py             | L47  | Request POST /api/v1/process completed in 8.5ms → HTTP 500
2026-04-12_10:00:00 | INFO  | endpoints.py        | L91  | Summary requested, limit=10.
2026-04-12_10:00:00 | INFO  | main.py             | L47  | Request GET /api/v1/summary completed in 1.8ms → HTTP 200
2026-04-12_11:27:03 | INFO  | endpoints.py        | L50  | Retrieving record id=8801.
2026-04-12_11:27:03 | ERROR | data_processor.py   | L28  | ValueError: invalid literal for int() with base 10: 'not-a-number'
2026-04-12_11:27:03 | ERROR | endpoints.py        | L57  | Schema validation failed for record id=8801: invalid literal for int().
2026-04-12_11:27:03 | INFO  | main.py             | L47  | Request GET /api/v1/records/8801 completed in 13.6ms → HTTP 500
2026-04-12_12:50:19 | INFO  | endpoints.py        | L33  | Fetching feed 'fx-rates' from upstream provider.
2026-04-12_12:50:19 | ERROR | external_client.py  | L34  | Failed to connect to https://provider.internal/api/feeds/fx-rates: host returned 503.
2026-04-12_12:50:19 | ERROR | endpoints.py        | L39  | Upstream provider unavailable for feed 'fx-rates': host returned 503.
2026-04-12_12:50:19 | INFO  | main.py             | L47  | Request GET /api/v1/feeds/fx-rates completed in 17.3ms → HTTP 503
2026-04-12_14:05:55 | INFO  | endpoints.py        | L67  | Processing feed='reconciliation' record=512 divisor=8.
2026-04-12_14:05:55 | INFO  | main.py             | L47  | Request POST /api/v1/process completed in 4.2ms → HTTP 200
2026-04-12_15:38:47 | INFO  | endpoints.py        | L67  | Processing feed='audit' record=1 divisor=0.
2026-04-12_15:38:47 | ERROR | data_processor.py   | L52  | ZeroDivisionError: division by zero
2026-04-12_15:38:47 | ERROR | endpoints.py        | L81  | Computation error for feed='audit' record=1: division by zero
2026-04-12_15:38:47 | INFO  | main.py             | L47  | Request POST /api/v1/process completed in 7.0ms → HTTP 500
2026-04-12_16:59:12 | INFO  | endpoints.py        | L50  | Retrieving record id=3374.
2026-04-12_16:59:12 | ERROR | data_processor.py   | L28  | ValueError: invalid literal for int() with base 10: 'not-a-number'
2026-04-12_16:59:12 | ERROR | endpoints.py        | L57  | Schema validation failed for record id=3374: invalid literal for int().
2026-04-12_16:59:12 | INFO  | main.py             | L47  | Request GET /api/v1/records/3374 completed in 10.9ms → HTTP 500
"""

# ─────────────────────────────────────────────
# GENERATOR
# ─────────────────────────────────────────────

def generate() -> Path:
    """Create the full test_project directory tree and return its root path."""

    root = Path("test_project")

    files: dict[Path, str] = {
        root / "src" / "main.py":                       MAIN_PY,
        root / "src" / "api" / "__init__.py":           "",
        root / "src" / "api" / "endpoints.py":          ENDPOINTS_PY,
        root / "src" / "services" / "__init__.py":      "",
        root / "src" / "services" / "external_client.py": EXTERNAL_CLIENT_PY,
        root / "src" / "services" / "data_processor.py":  DATA_PROCESSOR_PY,
        root / "src" / "utils" / "__init__.py":         "",
        root / "src" / "utils" / "logger.py":           LOGGER_PY,
        root / "src" / "__init__.py":                   "",
        root / "logs" / "2026-04-11.text":              LOG_APR_11,
        root / "logs" / "2026-04-12.text":              LOG_APR_12,
    }

    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    return root


def print_tree(root: Path) -> None:
    """Print the generated directory tree using the required prefix format."""
    print(f"\n{'─' * 56}")
    print("  Generated project structure")
    print(f"{'─' * 56}")

    def _walk(path: Path, prefix: str = "") -> None:
        if path.is_dir():
            print(f"{prefix}DIRECTORY: {path.name}/")
            for child in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name)):
                _walk(child, prefix + "  ")
        else:
            print(f"{prefix}FILE: {path.name}")

    _walk(root)
    print(f"{'─' * 56}\n")


def main() -> None:
    root = generate()

    print("\n✔  test_project generated successfully.\n")
    print("  Bugs embedded")
    print("  ┌─ Bug #1  ConnectionError  → external_client.py:34")
    print("  ├─ Bug #2  ValueError       → data_processor.py:28")
    print("  └─ Bug #3  ZeroDivisionError → data_processor.py:52\n")
    print("  Log files")
    print("  ┌─ logs/2026-04-11.text")
    print("  └─ logs/2026-04-12.text\n")

    print_tree(root)


if __name__ == "__main__":
    main()























"""
prompts.py

Prompt strategy module for the Ollama MCP Debugger.
Separated from client code so it can be imported independently,
tested in isolation, and swapped per model or use-case.

Usage:
    from prompts import DebugPromptBuilder
    builder = DebugPromptBuilder(available_tools=["read_file", "search_logs_content", ...])
    prompt = builder.initial(query, tools_json)
    prompt = builder.continuation(query, history_str, tools_json)
    prompt = builder.stall_redirect(query, history_str, tools_json, stall_count)
    prompt = builder.tool_correction(query, history_str, tools_json, bad_tool, available)
"""

from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# STATIC BLOCKS  (reusable across all prompt types)
# ─────────────────────────────────────────────────────────────────────────────

ROLE_DEFINITION = """\
ROLE: You are a LOG-FIRST Debugging Engine running inside a Zero-Trust environment.
You have NO internet access, NO internal knowledge about this codebase, and NO
built-in tools. Everything you know must come from an explicit tool call.
"""

HARD_RULES = """\
═══ ZERO-TRUST HARD RULES (violations abort the investigation) ═══
R1. CALL TOOLS ONLY  — Your only valid output is a tool call until the final
    report is ready. Do not explain your reasoning in text. Do not ask questions.
R2. NO INVENTED PATHS — If a file path did not appear in get_repository_structure,
    it does not exist. Never construct a path from memory.
R3. NO INVENTED TOOLS — You may ONLY call tools from the AVAILABLE TOOLS list
    printed below. google:search, get_file_list, list_files, web_search, and any
    tool not in that list DO NOT EXIST. Calling them wastes a turn.
R4. FULL TIMESTAMP CHAIN — Log lines that share the same timestamp are one
    request chain. Read ALL of them before forming any hypothesis.
R5. CITE EVERYTHING — Every bug claim requires:
    log '<file>:<line>' → code '<file>:<line>'
    If you cannot cite both, you have not confirmed the bug.
"""

MANDATORY_SEQUENCE = """\
═══ MANDATORY INVESTIGATION SEQUENCE ═══
STEP 1 — MAP
  Call get_repository_structure.
  Record every path. Do not open any file yet.

STEP 2 — FULL LOG SCAN
  Call search_logs_content on EVERY log file discovered in Step 1.
  Before reading any source file, build this table in memory:

  ERROR MANIFEST
  | timestamp | log_file | log_line# | reported_source_file | reported_line# | error_message |

  Group entries by shared timestamp (one group = one HTTP request chain).
  A chain is: INFO entry → exception line → handler line → HTTP response line.
  Do NOT read source code until all chains are fully listed.

STEP 3 — TRACE (once per manifest row)
  3a. Call read_file on reported_source_file.
  3b. Navigate to reported_line#. Identify the exception.
  3c. If the exception is raised inside a called function, follow the import
      and read that file too. Locate the actual raise site.
  3d. Record: CONFIRMED_FILE | CONFIRMED_LINE | EXCEPTION_TYPE | TRIGGER_CONDITION

STEP 4 — CROSS-REFERENCE
  Before writing the report verify all three:
  □ Every manifest row has a CONFIRMED source line.
  □ Every library/framework name in your report was read from an import statement.
  □ Zero unresolved 5XX events remain.
"""

FINAL_REPORT_FORMAT = """\
═══ FINAL REPORT FORMAT ═══
Begin the report with the exact header:  ## FINAL DIAGNOSIS REPORT

## Error Manifest
| timestamp | log_file | log_line# | source_file | source_line# | exception | trigger |
(one row per confirmed bug)

## Confirmed Entry Points
BUG #N
  - File      : <path>
  - Line      : <N>
  - Exception : <ExceptionType>
  - Trigger   : <exact condition>
  - Evidence  : log '<exact log text>' → code '<exact code line text>'
  - Fix       : <one concrete code change>

## Coverage Confirmation
  - Log files scanned : <comma-separated list>
  - 5XX events found  : <N>
  - Bugs confirmed    : <N>
  - Unresolved errors : <N>   ← MUST be 0 to conclude
"""


# ─────────────────────────────────────────────────────────────────────────────
# TOOL FENCE  (injected at the top of every message)
# ─────────────────────────────────────────────────────────────────────────────

def build_tool_fence(available_tools: list[str]) -> str:
    """
    Returns a hard-bordered block listing the ONLY valid tool names.
    Prepend this to every message sent to the model.

    Args:
        available_tools: Exact tool names registered across all MCP sessions.
    """
    lines = ["═══ AVAILABLE TOOLS — THE ONLY TOOLS THAT EXIST ═══"]
    for name in available_tools:
        lines.append(f"  • {name}")
    lines += [
        "",
        "ANY OTHER TOOL NAME IS AN INVENTION. Do not call it.",
        "If the tool you want is not in this list, pick the closest one that IS.",
        "═══════════════════════════════════════════════════════════════════",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

class DebugPromptBuilder:
    """
    Builds scenario-specific prompts for the MCP debugger.

    Each public method returns a complete, ready-to-send prompt string.
    The class is stateless — it reads `available_tools` at construction
    time and embeds the tool fence into every prompt automatically.

    Args:
        available_tools: List of tool names available across all MCP sessions.
    """

    def __init__(self, available_tools: list[str]) -> None:
        self._fence = build_tool_fence(available_tools)
        self._today = datetime.now().strftime("%Y-%m-%d")

    # ── Public scenario methods ───────────────────────────────────────────────

    def initial(self, query: str, tools_json: str) -> str:
        """
        First-turn prompt. Sent before the model has called any tool.
        Establishes the full role, rules, and sequence from scratch.

        Args:
            query:      Raw user query / log line supplied at the prompt.
            tools_json: JSON-serialised Ollama tool definitions.
        """
        return self._assemble(
            scenario_header=self._section("SCENARIO", "INITIAL INVESTIGATION"),
            context_block=self._context_block(query),
            tools_json=tools_json,
            mission=(
                "You have NO history yet.\n"
                "BEGIN with STEP 1 (get_repository_structure).\n"
                "Then STEP 2 (scan every log file).\n"
                "Do NOT open source files until your Error Manifest is complete.\n"
                "The user's query contains a specific timestamp — find ALL log lines "
                "sharing that timestamp before forming any hypothesis."
            ),
        )

    def continuation(self, query: str, history_str: str, tools_json: str) -> str:
        """
        Standard turn prompt. Sent after at least one tool call has completed.
        Includes the full investigation history and a checklist gate.

        Args:
            query:       Original user query (unchanged throughout session).
            history_str: Pre-formatted history string from _rebuild_history_str().
            tools_json:  JSON-serialised Ollama tool definitions.
        """
        checklist = (
            "BEFORE calling the next tool, verify:\n"
            "  □ Did you read ALL log lines sharing the same timestamp as each 5XX event?\n"
            "  □ Have you opened the source file at the exact line the log reported?\n"
            "  □ Have you followed the call chain to the actual raise site?\n"
            "  □ Are all library names sourced from import statements you read?\n\n"
            "If any box is unchecked — call the tool that checks it.\n"
            "If all boxes are checked and the manifest is complete — write the final report."
        )
        return self._assemble(
            scenario_header=self._section("SCENARIO", "INVESTIGATION IN PROGRESS"),
            context_block=self._context_block(query),
            history_block=self._history_block(history_str),
            tools_json=tools_json,
            mission=checklist,
        )

    def stall_redirect(
        self,
        query: str,
        history_str: str,
        tools_json: str,
        stall_count: int,
    ) -> str:
        """
        Injected when the model stops calling tools without producing a report.
        Escalates in urgency with each successive stall.

        Args:
            query:       Original user query.
            history_str: Current investigation history.
            tools_json:  JSON-serialised Ollama tool definitions.
            stall_count: How many times the model has stalled this session (1-based).
        """
        urgency = "⚠️" * min(stall_count, 3)
        message = (
            f"{urgency} STALL DETECTED (occurrence {stall_count}/3)\n\n"
            "You stopped calling tools without producing a ## FINAL DIAGNOSIS REPORT.\n"
            "This is not allowed. Text responses without a complete report are discarded.\n\n"
            "DIAGNOSE the next unchecked step:\n"
            "  → If you have not called get_repository_structure: call it NOW.\n"
            "  → If you have not scanned all log files: call search_logs_content NOW.\n"
            "  → If any manifest row lacks a confirmed source line: call read_file NOW.\n\n"
            "Respond with a tool call. Nothing else."
        )
        return self._assemble(
            scenario_header=self._section("SCENARIO", f"STALL RECOVERY — ATTEMPT {stall_count}"),
            context_block=self._context_block(query),
            history_block=self._history_block(history_str),
            tools_json=tools_json,
            mission=message,
        )

    def tool_correction(
        self,
        query: str,
        history_str: str,
        tools_json: str,
        bad_tool: str,
        available_tools: list[str],
    ) -> str:
        """
        Injected immediately when the model calls a non-existent tool.
        Identifies the bad call and re-anchors the model to valid tools.

        Args:
            query:           Original user query.
            history_str:     Current investigation history.
            tools_json:      JSON-serialised Ollama tool definitions.
            bad_tool:        The invented tool name the model tried to call.
            available_tools: Definitive list of valid tool names.
        """
        message = (
            f"TOOL ERROR: '{bad_tool}' does not exist.\n\n"
            f"Valid tools: {available_tools}\n\n"
            "Find the tool in that list that is closest to what you intended.\n"
            "Call it now. Do not call any tool not in that list."
        )
        return self._assemble(
            scenario_header=self._section("SCENARIO", "TOOL CORRECTION REQUIRED"),
            context_block=self._context_block(query),
            history_block=self._history_block(history_str),
            tools_json=tools_json,
            mission=message,
        )

    # ── Private assembly helpers ──────────────────────────────────────────────

    def _assemble(
        self,
        scenario_header: str,
        context_block: str,
        tools_json: str,
        mission: str,
        history_block: Optional[str] = None,
    ) -> str:
        """Assembles blocks into a final prompt in a consistent order."""
        parts = [
            self._fence,          # tool fence always first — highest attention
            "",
            ROLE_DEFINITION,
            HARD_RULES,
            MANDATORY_SEQUENCE,
            FINAL_REPORT_FORMAT,
            scenario_header,
            context_block,
        ]
        if history_block:
            parts.append(history_block)
        parts += [
            self._section("TOOLS", tools_json),
            self._section("CURRENT MISSION", mission),
            self._footer(),
        ]
        return "\n".join(parts)

    @staticmethod
    def _section(title: str, content: str) -> str:
        bar = "═" * 60
        return f"\n{bar}\n  {title}\n{bar}\n{content}\n"

    @staticmethod
    def _context_block(query: str) -> str:
        return DebugPromptBuilder._section("USER PROBLEM", query)

    @staticmethod
    def _history_block(history_str: str) -> str:
        return DebugPromptBuilder._section("INVESTIGATION HISTORY (most recent last)", history_str)

    def _footer(self) -> str:
        return (
            f"\n[ Session date: {self._today} | "
            "Zero-Trust Mode: ON | "
            "Only tool calls are accepted as valid responses until the final report ]\n"
        )