"""
log_server.py (Optimized for Small LLMs)
"""

from pathlib import Path
from fastmcp import FastMCP

mcp = FastMCP("Log_Investigator")

LOG_DIR = Path("./logs").resolve()
LOG_DIR.mkdir(exist_ok=True)
_MAX_RESULTS = 200

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — PRIMARY SCAN
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_logs_content(query: str, date_pattern: str = "*") -> str:
    """
    Search log files for text. 
    
    USAGE:
    1. Initial Scan: query='500' to find HTTP errors.
    2. Chain Discovery (MANDATORY): Once you find a 500 error, call this again 
       with the EXACT TIMESTAMP (e.g., '17:58:33') to see the lines before it.
    
    Args:
        query: Search string (e.g., '500', 'ValueError', or '17:58:33').
        date_pattern: Date filter (e.g., '2026-04-11'). Use '*' for all.
    """
    log_files = sorted(LOG_DIR.glob(f"{date_pattern}.text"))
    if not log_files:
        return f"ERROR: No log files found for '{date_pattern}'"

    results: list[str] = []
    needle = query.lower()

    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8") as fh:
                for line_num, line in enumerate(fh, 1):
                    if needle in line.lower():
                        results.append(f"[{log_file.name}] L{line_num}: {line.rstrip()}")
                    if len(results) >= _MAX_RESULTS:
                        break
        except OSError as exc:
            results.append(f"[{log_file.name}] ERROR: {exc}")
        if len(results) >= _MAX_RESULTS:
            results.append("WARNING: Result cap reached. Narrow your query.")
            break

    return "\n".join(results) if results else f"No matches for '{query}'"


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2 — SOURCE-FILE TRACE
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def grep_logs_by_file(target_filename: str, date: str) -> str:
    """
    Get all log activity for one source file on a specific date.
    Use this to trace logic flow in a specific file like 'endpoints.py'.
    
    Args:
        target_filename: Source file name (e.g., 'data_processor.py').
        date: Date in 'YYYY-MM-DD' format.
    """
    file_path = LOG_DIR / f"{date}.text"
    if not file_path.exists():
        return f"ERROR: No log for {date}"

    matches: list[str] = []
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if f"|| {target_filename} ||" in line:
                    matches.append(line.rstrip())
    except OSError as exc:
        return f"ERROR: {exc}"

    return "\n".join(matches) if matches else f"No logs for {target_filename} on {date}"


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 3 — TIME RANGE ANCHOR
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_log_timerange(date: str) -> str:
    """
    Check log coverage: Returns first/last timestamps and 5XX error counts.
    Call this BEFORE searching to confirm the log covers the time you need.
    
    Args:
        date: Date in 'YYYY-MM-DD' format.
    """
    file_path = LOG_DIR / f"{date}.text"
    if not file_path.exists():
        return f"ERROR: No log for {date}"

    first_ts, last_ts = None, None
    total_lines, error_count = 0, 0

    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line: continue
                total_lines += 1
                ts = line.split("||")[0].strip()
                if not first_ts: first_ts = ts
                last_ts = ts
                if "HTTP 5" in line: error_count += 1
    except Exception as exc:
        return f"ERROR: {exc}"

    return (f"Log: {date}.text\nStart: {first_ts}\nEnd: {last_ts}\n"
            f"Total Lines: {total_lines}\n5XX Errors: {error_count}")


if __name__ == "__main__":
    mcp.run(transport="sse", port=8001)