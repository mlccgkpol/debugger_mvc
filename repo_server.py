from fastmcp import FastMCP
from pathlib import Path
import os

mcp = FastMCP("Repository_Expert")

@mcp.tool()
def list_directory(path: str = ".") -> str:
    """
    ACTION: Scans a directory and lists all sub-folders and files.
    
    INTENT: Use this tool to map out the repository structure or locate a specific file 
    before attempting to read it. It provides the 'map' for your exploration.
    
    INPUT: 
        path (str): The relative path to the directory (e.g., 'src/'). Use '.' for root.
        
    OUTPUT: 
        A list of items prefixed with 'DIRECTORY:' or 'FILE:'. 
        If an error occurs, the response will start with 'ERROR:'.
    """
    try:
        target_path = Path(path).resolve()
        
        # Security: Keep the LLM within the project bounds
        if not str(target_path).startswith(os.getcwd()):
            return "ERROR: Permission Denied. You cannot access directories outside the project root."

        if not target_path.exists():
            return f"ERROR: Path '{path}' does not exist. Use '.' to see the root directory."
        
        if not target_path.is_dir():
            return f"ERROR: '{path}' is a file. Use 'read_file' to view its contents."

        entries = []
        for entry in target_path.iterdir():
            label = "DIRECTORY:" if entry.is_dir() else "FILE:     "
            entries.append(f"{label} {entry.name}")
        
        if not entries:
            return f"INFO: The directory '{path}' is currently empty."

        # Adding a summary count helps the LLM understand the scale of the folder
        summary = f"Found {len(entries)} items in '{path}':\n"
        return summary + "\n".join(sorted(entries))

    except Exception as e:
        return f"ERROR: System failed to list directory: {str(e)}"

@mcp.tool()
def read_file(path: str) -> str:
    """
    ACTION: Retrieves the text-based content of a specific file.
    
    INTENT: Use this tool to examine the source code, logic, or data within a file 
    once you have identified its path using 'list_directory'.
    
    INPUT: 
        path (str): The exact path to the file (e.g., 'src/main.py').
        
    OUTPUT: 
        The raw text of the file. If the file is binary or too large, an error is returned.
    """
    try:
        target_path = Path(path).resolve()

        if not str(target_path).startswith(os.getcwd()):
            return "ERROR: Permission Denied. Access restricted to project files."

        if not target_path.exists():
            return f"ERROR: File '{path}' not found. Did you check the spelling with 'list_directory'?"
        
        if not target_path.is_file():
            return f"ERROR: '{path}' is a directory. Use 'list_directory' instead."

        # 1MB limit check to protect the LLM's context window
        if target_path.stat().st_size > 1_000_000:
            return "ERROR: File is too large (>1MB). Reading this would exceed your memory limits."

        content = target_path.read_text(encoding='utf-8', errors='replace')
        
        # Using clear delimiters helps the LLM distinguish file content from its own reasoning
        return f"--- START OF FILE: {path} ---\n{content}\n--- END OF FILE: {path} ---"

    except UnicodeDecodeError:
        return f"ERROR: '{path}' appears to be a binary file (like an image or executable) and cannot be read as text."
    except Exception as e:
        return f"ERROR: System could not read file: {str(e)}"


@mcp.tool()
def find_files_by_name(pattern: str) -> str:
    """
    ACTION: Searches for files that match a specific name or pattern.
    
    INTENT: Use this when you know the name of a file (or part of it) but don't know 
    which directory it's in. It performs a recursive search.
    
    INPUT: 
        pattern (str): The filename or glob pattern to search for (e.g., 'config.json' or '*.py').
        
    OUTPUT: 
        A list of relative paths to matching files.
    """
    try:
        root = Path(os.getcwd())
        # Use rglob for recursive searching
        matches = list(root.rglob(pattern))
        
        # Filter to ensure we only return files, not directories
        file_matches = [m.relative_to(root) for m in matches if m.is_file()]

        if not file_matches:
            return f"INFO: No files matching '{pattern}' were found in the repository."

        result = f"Found {len(file_matches)} match(es) for '{pattern}':\n"
        return result + "\n".join([f"PATH: {path}" for path in file_matches])

    except Exception as e:
        return f"ERROR: Search failed due to system error: {str(e)}"

@mcp.tool()
def search_text_in_repository(query: str) -> str:
    """
    ACTION: Searches for a specific string/text inside all files in the repository.
    
    INTENT: Use this to find where a specific function is defined, where a variable is used, 
    or to find specific keywords across the entire codebase.
    
    INPUT: 
        query (str): The text string to search for.
        
    OUTPUT: 
        A list of files containing the text, including the line number where it was found.
    """
    try:
        root = Path(os.getcwd())
        results = []
        
        # Walk through all files
        for file_path in root.rglob('*'):
            # Security & File Type Checks
            if not file_path.is_file() or file_path.name.startswith('.'):
                continue
                
            try:
                # Check file size before reading to stay efficient
                if file_path.stat().st_size > 500_000: # 0.5MB limit for search
                    continue

                content = file_path.read_text(encoding='utf-8', errors='ignore')
                
                if query in content:
                    # Find line numbers
                    lines = content.splitlines()
                    for i, line in enumerate(lines):
                        if query in line:
                            rel_path = file_path.relative_to(root)
                            # Truncate line if it's too long
                            clean_line = line.strip()[:100]
                            results.append(f"FILE: {rel_path} | LINE {i+1}: {clean_line}")
                            
            except (UnicodeDecodeError, PermissionError):
                continue # Skip binary or locked files

        if not results:
            return f"INFO: The string '{query}' was not found in any readable text files."

        # Limit results to 50 to prevent context window overflow
        final_list = results[:50]
        header = f"Found matches for '{query}' in the following locations:\n"
        footer = "\n(Truncated to first 50 matches)" if len(results) > 50 else ""
        
        return header + "\n".join(final_list) + footer

    except Exception as e:
        return f"ERROR: Global text search failed: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="sse", port=8000)