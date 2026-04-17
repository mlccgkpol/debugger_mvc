"""
log_server.py (Optimized for Small LLMs)
"""

from pathlib import Path
from fastmcp import FastMCP

mcp = FastMCP("Log_Investigator")

LOG_DIR = Path("./logs").resolve()
LOG_DIR.mkdir(exist_ok=True)
_MAX_LIFECYCLE_RESULTS = 100 

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — PRIMARY SCAN
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_log(query: str, date_pattern: str = "*") -> str:
    """
    Search logs. 
    LITERAL EXAMPLES: query='k7l8m9n0' (ID search), query='10:00:00' (Time).
    
    CRITICAL FOR SMALL MODELS:
    1. Search the Request ID (e.g. 'k7l8m9n0') FIRST to trace the full chain.
    2. Do NOT use '5XX'; search exact strings like 'HTTP 500' or 'ERROR'.
    3. Once a log shows a file and line (e.g. 'main.py | L83'), STOP and use read_file.
    """
    log_files = sorted(LOG_DIR.glob(f"{date_pattern}.text"))
    if not log_files:
        return f"ERROR: No log files found for '{date_pattern}'"

    results: list[str] = []
    # Small models are more reliable with case-insensitive search
    needle_bytes = query.lower().encode("utf-8")

    for log_file in log_files:
        try:
            first_offset = None
            last_offset = None
            first_line_num = 0
            
            # --- PASS 1: Binary scan for byte offsets ---
            with open(log_file, "rb") as fh:
                current_pos = 0
                for line_num, line in enumerate(fh, 1):
                    # 'line' is bytes, so we compare it against 'needle_bytes'
                    if needle_bytes in line.lower():
                        if first_offset is None:
                            first_offset = current_pos
                            first_line_num = line_num
                        last_offset = fh.tell()
                    current_pos = fh.tell()

            # --- PASS 2: Surgical Binary Extraction ---
            if first_offset is not None and last_offset is not None:
                # We stay in "rb" mode here to ensure 'read(n)' matches our byte math
                with open(log_file, "rb") as fh:
                    fh.seek(first_offset)
                    content_bytes = fh.read(last_offset - first_offset)
                    # Now decode the chunk into a string for the LLM
                    content = content_bytes.decode("utf-8", errors="ignore")
                    
                    for i, line in enumerate(content.splitlines()):
                        results.append(f"[{log_file.name}] L{first_line_num + i}: {line.rstrip()}")
                        
                        if len(results) >= _MAX_LIFECYCLE_RESULTS:
                            results.append("... [TRUNCATED] ...")
                            break
                            
        except OSError as exc:
            results.append(f"[{log_file.name}] ERROR: {exc}")

        if len(results) >= _MAX_LIFECYCLE_RESULTS:
            break

    return "\n".join(results) if results else f"No logs found matching: '{query}'"


if __name__ == "__main__":
    mcp.run(transport="sse", port=8001)