import os
import re
from pathlib import Path
from datetime import datetime
from fastmcp import FastMCP

mcp = FastMCP("Log_Investigator")

LOG_DIR = Path("./logs").resolve()

# Ensure directory exists for safety
LOG_DIR.mkdir(exist_ok=True)

# --- TOOLS ---

@mcp.tool()
def list_log_dates() -> str:
    """
    ACTION: Lists all available log dates.
    INTENT: Use this to see which days have recorded logs before searching.
    """
    files = sorted(LOG_DIR.glob("*.text"), reverse=True)
    if not files:
        return "INFO: No log files found in ./logs/"
    
    dates = [f.stem for f in files]
    return "Available log dates (Newest first):\n" + "\n".join(dates)

@mcp.tool()
def tail_logs(date: str, lines: int = 50) -> str:
    """
    ACTION: Gets the most recent entries from a specific log file.
    INTENT: Use this to see the latest errors for a specific day (YYYY-MM-DD).
    """
    file_path = LOG_DIR / f"{date}.text"
    if not file_path.exists():
        return f"ERROR: No log file found for date: {date}"

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Efficiently get last N lines
            all_lines = f.readlines()
            tail = all_lines[-lines:]
            
        return f"--- Last {len(tail)} logs for {date} ---\n" + "".join(tail)
    except Exception as e:
        return f"ERROR: Failed to tail logs: {str(e)}"

@mcp.tool()
def search_logs_content(query: str, date_pattern: str = "*") -> str:
    """
    ACTION: Searches for a string across log files.
    INTENT: Use this to find a specific 'message' or 'file_name' in the logs.
    
    ARGS:
        query: The string to look for (e.g., 'DatabaseConnectionError').
        date_pattern: Glob pattern (e.g., '2026-04-*' for the whole month).
    """
    results = []
    log_files = sorted(LOG_DIR.glob(f"{date_pattern}.text"))
    
    if not log_files:
        return f"INFO: No log files found matching pattern: {date_pattern}"

    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    if query.lower() in line.lower():
                        # Structure: date_time || file_name || line || message
                        results.append(f"[{log_file.name}] L{line_num}: {line.strip()}")
                        if len(results) >= 100: # Safety cap
                            break
        except Exception:
            continue
        if len(results) >= 100: break

    if not results:
        return f"INFO: No matches found for '{query}' in {date_pattern}.text"

    header = f"Found {len(results)} matches for '{query}':\n"
    return header + "\n".join(results)

@mcp.tool()
def grep_logs_by_file(target_filename: str, date: str) -> str:
    """
    ACTION: Filters logs to show only entries related to a specific source code file.
    INTENT: Use this when you suspect 'auth.py' is buggy and want to see all its log entries.
    """
    file_path = LOG_DIR / f"{date}.text"
    if not file_path.exists():
        return f"ERROR: No logs for {date}"

    matches = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            # Split by your structure: date_time || file_name || line || message
            parts = line.split("||")
            if len(parts) >= 2 and target_filename.lower() in parts[1].strip().lower():
                matches.append(line.strip())
    
    if not matches:
        return f"INFO: No logs found mentioning source file '{target_filename}' on {date}."

    return f"Logs for {target_filename} on {date}:\n" + "\n".join(matches)

if __name__ == "__main__":
    mcp.run(transport="sse", port=8001) # Run on a different port than the Repo server