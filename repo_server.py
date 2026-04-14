"""
repo_server.py (Optimized for Small LLMs)
"""

import os
from pathlib import Path
from fastmcp import FastMCP

mcp = FastMCP("Repository_Expert")

# ── Security ──────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(os.getcwd()).resolve()

_IGNORE = frozenset({
    ".git", "__pycache__", ".venv", "venv", ".env",
    "node_modules", ".pytest_cache", ".mypy_cache", "dist", "build",
})

_MAX_FILE_BYTES   = 1_000_000   # 1 MB — protects context window on read_file
_MAX_SEARCH_BYTES =   500_000   # 0.5 MB — per-file limit during repo-wide search
_MAX_SEARCH_HITS  =        50   # result cap for search_text_in_repository


def _is_safe(path: Path) -> bool:
    """Return True only if the resolved path stays inside the project root."""
    try:
        path.resolve().relative_to(_PROJECT_ROOT)
        return True
    except ValueError:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — REPOSITORY MAP
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_repository_structure(max_depth: int = 5) -> str:
    """
    Get the full file tree.
    
    USAGE:
    MANDATORY STEP 1. Call this once at the start of every session. 
    Use the returned paths VERBATIM in other tools. Never guess paths.
    
    Args:
        max_depth: How deep to scan. Default 5.
    """
    tree_lines = [f"📁 {_PROJECT_ROOT.name}/"]

    def _walk(current: Path, prefix: str, depth: int) -> None:
        if depth > max_depth:
            return
        try:
            entries = sorted(
                [e for e in current.iterdir() if e.name not in _IGNORE],
                key=lambda e: (e.is_file(), e.name.lower()),  # dirs first
            )
        except PermissionError:
            tree_lines.append(f"{prefix}└── [Permission Denied]")
            return

        for idx, entry in enumerate(entries):
            is_last  = idx == len(entries) - 1
            connector = "└── " if is_last else "├── "
            icon      = "📁 " if entry.is_dir() else "📄 "
            tree_lines.append(f"{prefix}{connector}{icon}{entry.name}")
            if entry.is_dir():
                _walk(entry, prefix + ("    " if is_last else "│   "), depth + 1)

    _walk(_PROJECT_ROOT, "", 1)
    return "\n".join(tree_lines)


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2 — FILE READER
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def read_file(path: str) -> str:
    """
    Read full text and line numbers of a file.
    
    USAGE:
    Call this after logs identify a suspicious file. 
    ONLY use paths found in get_repository_structure.
    
    Args:
        path: Relative path (e.g., 'src/api/endpoints.py').
    """
    target = Path(path)
    resolved = (_PROJECT_ROOT / target).resolve() if not target.is_absolute() else target.resolve()

    if not _is_safe(resolved):
        return "ERROR: Access denied — path is outside the project root."

    if not resolved.exists():
        return (
            f"ERROR: '{path}' does not exist.\n"
            "Check the spelling against get_repository_structure output."
        )

    if not resolved.is_file():
        return (
            f"ERROR: '{path}' is a directory, not a file.\n"
            "Use get_repository_structure to browse directories."
        )

    if resolved.stat().st_size > _MAX_FILE_BYTES:
        size_mb = resolved.stat().st_size / 1_000_000
        return (
            f"ERROR: '{path}' is {size_mb:.1f} MB — too large to read safely.\n"
            "Use search_text_in_repository to locate the specific lines you need."
        )

    try:
        raw = resolved.read_text(encoding="utf-8", errors="replace")
    except UnicodeDecodeError:
        return f"ERROR: '{path}' is a binary file and cannot be read as text."
    except OSError as exc:
        return f"ERROR: Could not read '{path}' — {exc}"

    # Numbered lines make it trivial to cite code evidence in the final report
    numbered = "\n".join(
        f"{lineno:>4} | {line}"
        for lineno, line in enumerate(raw.splitlines(), 1)
    )
    return (
        f"--- START OF FILE: {path} ({raw.count(chr(10)) + 1} lines) ---\n"
        f"{numbered}\n"
        f"--- END OF FILE: {path} ---"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 3 — CODE SEARCH
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_text_in_repository(query: str) -> str:
    """
    Search codebase for a specific string.
    
    USAGE:
    1. Find where a function is defined (e.g., 'def process_data').
    2. Follow imports when an error happens in a library file.
    3. Find all places an exception is raised (e.g., 'ValueError').
    
    Args:
        query: Case-sensitive string to find.
    """
    results: list[str] = []

    for file_path in sorted(_PROJECT_ROOT.rglob("*")):
        if not file_path.is_file():
            continue
        if any(part in _IGNORE for part in file_path.parts):
            continue
        if file_path.name.startswith("."):
            continue
        if file_path.stat().st_size > _MAX_SEARCH_BYTES:
            continue

        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except (OSError, PermissionError):
            continue

        if query not in content:
            continue

        rel = file_path.relative_to(_PROJECT_ROOT)
        for lineno, line in enumerate(content.splitlines(), 1):
            if query in line:
                snippet = line.strip()[:120]
                results.append(f"FILE: {rel} | LINE {lineno}: {snippet}")
            if len(results) >= _MAX_SEARCH_HITS:
                break

        if len(results) >= _MAX_SEARCH_HITS:
            break

    if not results:
        return (
            f"INFO: '{query}' not found in any readable file under {_PROJECT_ROOT.name}/.\n"
            "Verify the exact string — this search is case-sensitive."
        )

    cap_note = (
        f"\nNOTE: Results capped at {_MAX_SEARCH_HITS}. "
        "Use a more specific query to narrow results."
        if len(results) == _MAX_SEARCH_HITS
        else ""
    )
    return (
        f"Found {len(results)} match(es) for '{query}':\n"
        + "\n".join(results)
        + cap_note
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="sse", port=8000)